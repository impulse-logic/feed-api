package queue

import (
	"context"
	"encoding/json"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

// PeekedMessage is one message as it sits in the queue. Peeking never locks or removes it.
type PeekedMessage struct {
	SequenceNumber        int64          `json:"sequenceNumber"`
	MessageID             string         `json:"messageId"`
	EnqueuedTime          *time.Time     `json:"enqueuedTime"`
	DeliveryCount         uint32         `json:"deliveryCount"`
	ContentType           *string        `json:"contentType"`
	ApplicationProperties map[string]any `json:"applicationProperties"`
	BodyFormat            string         `json:"bodyFormat"` // "json", "text" or "binary"
	Body                  any            `json:"body"`

	// Only set for dead-letter messages: why the message was dead-lettered.
	DeadLetterReason           *string `json:"deadLetterReason,omitempty"`
	DeadLetterErrorDescription *string `json:"deadLetterErrorDescription,omitempty"`
	DeadLetterSource           *string `json:"deadLetterSource,omitempty"`
}

// PeekResult is one page of peeked messages. Pass NextSequence as fromSequence to get the next page.
type PeekResult struct {
	Queue        string          `json:"queue"`
	SubQueue     string          `json:"subQueue,omitempty"` // "deadletter" for the dead-letter queue
	Count        int             `json:"count"`
	NextSequence int64           `json:"nextSequence"`
	Messages     []PeekedMessage `json:"messages"`
}

// Peeker reads queues without changing them. It keeps one receiver per queue open,
// because opening a link costs several network round trips.
type Peeker struct {
	client    *azservicebus.Client
	mu        sync.Mutex
	receivers map[string]*azservicebus.Receiver
}

func NewPeeker(client *azservicebus.Client) *Peeker {
	return &Peeker{client: client, receivers: map[string]*azservicebus.Receiver{}}
}

// Peek reads up to count messages starting at fromSequence (0 = the oldest message).
// deadLetter = true reads the queue's dead-letter queue instead.
// Read-only: no lock, no delete, the delivery count does not change.
func (p *Peeker) Peek(ctx context.Context, queueName string, deadLetter bool, count int, fromSequence int64) (*PeekResult, error) {
	receiver, err := p.receiver(queueName, deadLetter)
	if err != nil {
		return nil, err
	}

	msgs, err := receiver.PeekMessages(ctx, count, &azservicebus.PeekMessagesOptions{FromSequenceNumber: &fromSequence})
	if err != nil {
		p.drop(queueName, deadLetter) // reopened on the next call
		return nil, err
	}

	result := &PeekResult{Queue: queueName, NextSequence: fromSequence, Messages: []PeekedMessage{}}
	if deadLetter {
		result.SubQueue = "deadletter"
	}
	for _, m := range msgs {
		format, body := decodeBody(m.Body)
		peeked := PeekedMessage{
			MessageID:             m.MessageID,
			EnqueuedTime:          m.EnqueuedTime,
			DeliveryCount:         m.DeliveryCount,
			ContentType:           m.ContentType,
			ApplicationProperties: m.ApplicationProperties,
			BodyFormat:            format,
			Body:                  body,

			DeadLetterReason:           m.DeadLetterReason,
			DeadLetterErrorDescription: m.DeadLetterErrorDescription,
			DeadLetterSource:           m.DeadLetterSource,
		}
		if m.SequenceNumber != nil {
			peeked.SequenceNumber = *m.SequenceNumber
			result.NextSequence = *m.SequenceNumber + 1
		}
		result.Messages = append(result.Messages, peeked)
	}
	result.Count = len(result.Messages)
	return result, nil
}

// decodeBody shows a JSON body as JSON, UTF-8 text as a string, anything else as base64 bytes.
func decodeBody(body []byte) (string, any) {
	var parsed any
	if json.Unmarshal(body, &parsed) == nil {
		return "json", parsed
	}
	if utf8.Valid(body) {
		return "text", string(body)
	}
	return "binary", body
}

func (p *Peeker) receiver(queueName string, deadLetter bool) (*azservicebus.Receiver, error) {
	key := receiverKey(queueName, deadLetter)
	p.mu.Lock()
	defer p.mu.Unlock()
	if receiver, ok := p.receivers[key]; ok {
		return receiver, nil
	}
	var options *azservicebus.ReceiverOptions
	if deadLetter {
		options = &azservicebus.ReceiverOptions{SubQueue: azservicebus.SubQueueDeadLetter}
	}
	receiver, err := p.client.NewReceiverForQueue(queueName, options)
	if err != nil {
		return nil, err
	}
	p.receivers[key] = receiver
	return receiver, nil
}

func (p *Peeker) drop(queueName string, deadLetter bool) {
	key := receiverKey(queueName, deadLetter)
	p.mu.Lock()
	defer p.mu.Unlock()
	if receiver, ok := p.receivers[key]; ok {
		receiver.Close(context.Background())
		delete(p.receivers, key)
	}
}

func receiverKey(queueName string, deadLetter bool) string {
	if deadLetter {
		return queueName + "/$deadletterqueue"
	}
	return queueName
}
