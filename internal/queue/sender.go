package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

// ErrEmptyArray is returned when Send gets an empty JSON array (nothing to send).
var ErrEmptyArray = errors.New("the array is empty")

// SendResult is what Publisher.Send did.
type SendResult struct {
	Queue    string `json:"queue"`
	Messages int    `json:"messages"`
	Items    int    `json:"items"`
	Bytes    int    `json:"bytes"`
}

// Publisher pushes messages into queues. It keeps one sender per queue open,
// because opening a link costs several network round trips.
type Publisher struct {
	client  *azservicebus.Client
	mu      sync.Mutex
	senders map[string]*azservicebus.Sender
}

func NewPublisher(client *azservicebus.Client) *Publisher {
	return &Publisher{client: client, senders: map[string]*azservicebus.Sender{}}
}

// Send pushes a JSON body (an object or an array) into the queue:
//
//	object => one message
//	array  => one message, or several if it is larger than the queue's max message size
//	          (split in halves until every part fits); a single item that is too large => ErrMessageTooLarge
//
// Every message gets contentType application/json and the property source = "feed-api".
func (p *Publisher) Send(ctx context.Context, queueName string, body []byte) (*SendResult, error) {
	sender, err := p.sender(queueName)
	if err != nil {
		return nil, err
	}

	trimmed := bytes.TrimSpace(body)
	parts, items := [][]byte{trimmed}, 1
	if len(trimmed) > 0 && trimmed[0] == '[' {
		var raws []json.RawMessage
		if err := json.Unmarshal(trimmed, &raws); err != nil {
			return nil, err
		}
		if len(raws) == 0 {
			return nil, ErrEmptyArray
		}
		items = len(raws)
		if parts, err = splitToFit(ctx, sender, raws); err != nil {
			return nil, p.fail(queueName, err)
		}
	}

	result := &SendResult{Queue: queueName, Messages: len(parts), Items: items}
	batch, err := sender.NewMessageBatch(ctx, nil)
	if err != nil {
		return nil, p.fail(queueName, err)
	}
	for _, part := range parts {
		result.Bytes += len(part)
		err := batch.AddMessage(newJSONMessage(part), nil)
		if errors.Is(err, azservicebus.ErrMessageTooLarge) && batch.NumMessages() > 0 {
			// The batch is full: send it and start a new one.
			if err := sender.SendMessageBatch(ctx, batch, nil); err != nil {
				return nil, p.fail(queueName, err)
			}
			if batch, err = sender.NewMessageBatch(ctx, nil); err != nil {
				return nil, p.fail(queueName, err)
			}
			err = batch.AddMessage(newJSONMessage(part), nil)
		}
		if err != nil {
			return nil, p.fail(queueName, err)
		}
	}
	if err := sender.SendMessageBatch(ctx, batch, nil); err != nil {
		return nil, p.fail(queueName, err)
	}
	return result, nil
}

// splitToFit returns the items as one JSON array, or as several arrays when one is too large for
// a single message (halving until every part fits).
func splitToFit(ctx context.Context, sender *azservicebus.Sender, raws []json.RawMessage) ([][]byte, error) {
	body, err := json.Marshal(raws)
	if err != nil {
		return nil, err
	}
	batch, err := sender.NewMessageBatch(ctx, nil)
	if err != nil {
		return nil, err
	}
	err = batch.AddMessage(newJSONMessage(body), nil)
	switch {
	case err == nil:
		return [][]byte{body}, nil
	case !errors.Is(err, azservicebus.ErrMessageTooLarge) || len(raws) == 1:
		return nil, err
	}

	half := len(raws) / 2
	left, err := splitToFit(ctx, sender, raws[:half])
	if err != nil {
		return nil, err
	}
	right, err := splitToFit(ctx, sender, raws[half:])
	if err != nil {
		return nil, err
	}
	return append(left, right...), nil
}

func newJSONMessage(body []byte) *azservicebus.Message {
	contentType := "application/json"
	return &azservicebus.Message{Body: body, ContentType: &contentType, ApplicationProperties: map[string]any{"source": "feed-api"}}
}

func (p *Publisher) sender(queueName string) (*azservicebus.Sender, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if sender, ok := p.senders[queueName]; ok {
		return sender, nil
	}
	sender, err := p.client.NewSender(queueName, nil)
	if err != nil {
		return nil, err
	}
	p.senders[queueName] = sender
	return sender, nil
}

// fail drops the cached sender after an Azure error (it is reopened on the next call).
// ErrMessageTooLarge is a data problem, not a link problem, so the sender is kept.
func (p *Publisher) fail(queueName string, err error) error {
	if errors.Is(err, azservicebus.ErrMessageTooLarge) {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if sender, ok := p.senders[queueName]; ok {
		sender.Close(context.Background())
		delete(p.senders, queueName)
	}
	return err
}
