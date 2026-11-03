package queue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

const (
	// How long one receive waits for messages. Nothing arrives in this time => the queue is empty.
	receiveWait = 3 * time.Second
	// The first receive on a new link also opens it (3-15s from a far network), so it waits
	// longer, and a timeout there only stops that worker (not the whole run).
	firstReceiveWait = 30 * time.Second
	// After the first message arrives, wait this long for the batch to fill up.
	batchFillWait = 200 * time.Millisecond
	// Max parallel complete / dead-letter / abandon calls (one AMQP round trip each).
	settleParallel = 200
	// Settling must finish even if the HTTP request was cancelled.
	settleTimeout = 30 * time.Second
	// At most this many error messages are returned in RunResult.Errors.
	maxReportedErrors = 100
	// While a batch is being saved, its message locks are renewed this often
	// (a big batch can take longer than the queue's lock duration).
	lockRenewEvery = 20 * time.Second
)

// ErrAlreadyRunning is returned when Process is called while another run is still active.
var ErrAlreadyRunning = errors.New("a run is already active for this queue")

// Action is what happens to one message after its batch was handled.
type Action int

const (
	Complete   Action = iota // saved => delete it from the queue
	DeadLetter               // bad data => move it to the dead-letter queue (kept for inspection)
	Abandon                  // temporary failure => put it back in the queue (retried later)
)

// Outcome is the decision for ONE message. For DeadLetter, Reason / Description become the
// message's DeadLetterReason / DeadLetterErrorDescription and Properties are added to it.
//
// Resend (only with DeadLetter) splits a partly invalid message: the Resend message (only the
// invalid items) is sent to the queue and the original is completed; the Resend message is then
// dead-lettered when it is received. If the send fails, the original is dead-lettered whole.
type Outcome struct {
	Action      Action
	Reason      string
	Description string
	Properties  map[string]any
	Resend      *Resend
}

// Resend is a new message that holds only a message's invalid items.
type Resend struct {
	Body       []byte
	Properties map[string]any
}

// Properties of an invalid-items-only message (see Resend). It is dead-lettered on receive.
const (
	PropInvalidItemsOnly      = "invalidItemsOnly"
	PropDeadLetterReason      = "deadLetterReason"
	PropDeadLetterDescription = "deadLetterDescription"
)

// BatchOutcome is a processor's answer for one batch: one Outcome per message (same order),
// counters to add to the run result (e.g. "inserted": 3) and item-level problems.
type BatchOutcome struct {
	Outcomes []Outcome
	Counts   map[string]int64
	Errors   []string
}

// Processor turns one batch of messages into DB writes. One processor per queue (vendor, ...).
// It must return exactly len(msgs) outcomes, and only Complete a message after its data is committed.
type Processor interface {
	Handle(ctx context.Context, msgs []*azservicebus.ReceivedMessage) BatchOutcome
}

// RunResult is what one Process call did.
type RunResult struct {
	Queue        string           `json:"queue"`
	Received     int64            `json:"received"`
	Completed    int64            `json:"completed"`
	DeadLettered int64            `json:"deadLettered"`
	Abandoned    int64            `json:"abandoned"`
	Counts       map[string]int64 `json:"counts"`
	DurationMs   int64            `json:"durationMs"`
	PerSecond    float64          `json:"perSecond"`
	Errors       []string         `json:"errors"`
	StoppedEarly string           `json:"stoppedEarly,omitempty"`
}

// Consumer reads ONE queue in PeekLock mode and hands batches to its Processor.
type Consumer struct {
	client    *azservicebus.Client
	queue     string
	processor Processor
	workers   int
	batchSize int
	running   atomic.Bool
	slots     []*slot // one per worker, kept open between runs

	senderMu sync.Mutex
	sender   *azservicebus.Sender // for Resend messages, opened once and shared by all workers
}

// slot is one worker's receiver. Opening a link costs several network round trips,
// so it is opened once and reused by every run (like a DB connection pool).
type slot struct {
	receiver *azservicebus.Receiver
	warm     bool // the link is open (it has received messages before)
}

func NewConsumer(client *azservicebus.Client, queueName string, processor Processor, workers, batchSize int) *Consumer {
	slots := make([]*slot, workers)
	for i := range slots {
		slots[i] = &slot{}
	}
	return &Consumer{client: client, queue: queueName, processor: processor, workers: workers, batchSize: batchSize, slots: slots}
}

// run collects the result of one Process call from all workers.
type run struct {
	mu     sync.Mutex
	result RunResult
	fatal  error
	budget atomic.Int64 // messages still allowed; < 0 = no limit
}

// Process receives up to limit messages (0 = until the queue is empty), saves them through the
// processor and settles every message. Only one Process runs at a time per queue.
//
//  1. lock the run (no overlapping runs)
//  2. start N workers, each with its own PeekLock receiver (kept open between runs)
//  3. each worker: receive a batch => processor.Handle => complete / dead-letter / abandon
//  4. stop when the queue is empty, max is reached, or ctx is cancelled
func (c *Consumer) Process(ctx context.Context, limit int) (*RunResult, error) {
	if !c.running.CompareAndSwap(false, true) {
		return nil, ErrAlreadyRunning
	}
	defer c.running.Store(false)

	start := time.Now()
	r := &run{result: RunResult{Queue: c.queue, Counts: map[string]int64{}, Errors: []string{}}}
	r.budget.Store(-1)
	if limit > 0 {
		r.budget.Store(int64(limit))
	}

	// stopCtx only cancels RECEIVING: when a worker that already received messages finds the
	// queue empty, the others stop waiting. A batch that is already being saved still finishes with ctx.
	stopCtx, stop := context.WithCancel(ctx)
	defer stop()

	var wg sync.WaitGroup
	for _, s := range c.slots {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.worker(ctx, stopCtx, stop, s, r)
		}()
	}
	wg.Wait()

	elapsed := time.Since(start)
	r.result.DurationMs = elapsed.Milliseconds()
	if s := elapsed.Seconds(); s > 0 {
		r.result.PerSecond = float64(int(float64(r.result.Received)/s*10)) / 10
	}
	return &r.result, r.fatal
}

// worker uses its slot only while the run guard is held, so slots need no lock.
func (c *Consumer) worker(ctx, stopCtx context.Context, stop context.CancelFunc, s *slot, r *run) {
	if s.receiver == nil {
		receiver, err := c.client.NewReceiverForQueue(c.queue, nil) // PeekLock is the default mode
		if err != nil {
			r.setFatal(err)
			stop()
			return
		}
		s.receiver = receiver
	}
	receiver := s.receiver

	wait := receiveWait
	if !s.warm {
		wait = firstReceiveWait
	}
	for stopCtx.Err() == nil {
		n := r.take(c.batchSize)
		if n == 0 {
			return // max reached
		}

		receiveCtx, cancel := context.WithTimeout(stopCtx, wait)
		wait = receiveWait
		msgs, err := receiver.ReceiveMessages(receiveCtx, n, &azservicebus.ReceiveMessagesOptions{TimeAfterFirstMessage: batchFillWait})
		timedOut := receiveCtx.Err() != nil
		cancel()
		r.giveBack(n - len(msgs))

		if len(msgs) == 0 {
			switch {
			case err != nil && !timedOut:
				r.setFatal(err)
				receiver.Close(context.Background()) // reopened by the next run
				s.receiver, s.warm = nil, false
				stop() // Azure error => every worker stops
			case s.warm:
				stop() // a working link got nothing => the queue is empty, every worker stops
			}
			return // first receive timed out: only this worker stops (its link may just be slow)
		}
		s.warm = true

		// Invalid-items-only messages (made by Resend) go straight to the dead-letter queue.
		msgs = c.deadLetterInvalidOnly(ctx, receiver, msgs, r)
		if len(msgs) == 0 {
			continue
		}

		stopRenew := keepLocks(ctx, receiver, msgs)
		outcome := c.processor.Handle(ctx, msgs)
		stopRenew()
		c.settle(ctx, receiver, msgs, outcome, r)

		// An abandoned message comes back at once. Retrying it in this run would use up its
		// delivery attempts (Azure dead-letters after the max), so the whole run stops instead.
		if slices.ContainsFunc(outcome.Outcomes, func(o Outcome) bool { return o.Action == Abandon }) {
			r.stopEarly("a temporary error happened (see errors); the abandoned messages will be retried in the next run")
			stop()
			return
		}
	}
}

// settle applies each message's Outcome in parallel. A failed complete is not fatal:
// the lock expires, the message comes back and is saved again as "unchanged".
func (c *Consumer) settle(ctx context.Context, receiver *azservicebus.Receiver, msgs []*azservicebus.ReceivedMessage, outcome BatchOutcome, r *run) {
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
	defer cancel()

	var completed, deadLettered, abandoned atomic.Int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, settleParallel)

	for i, msg := range msgs {
		o := Outcome{Action: Abandon, Reason: "processor returned no outcome"}
		if i < len(outcome.Outcomes) {
			o = outcome.Outcomes[i]
		}
		if o.Action != Complete {
			r.addError(fmt.Sprintf("seq %d: %s", sequence(msg), o.Reason))
		}

		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer func() { <-sem; wg.Done() }()
			var err error
			switch o.Action {
			case Complete:
				if err = receiver.CompleteMessage(settleCtx, msg, nil); err == nil {
					completed.Add(1)
				}
			case DeadLetter:
				if o.Resend != nil {
					// Split: send the invalid items as a new message, then delete the original.
					sendErr := c.send(settleCtx, o.Resend)
					if sendErr == nil {
						if err = receiver.CompleteMessage(settleCtx, msg, nil); err == nil {
							completed.Add(1)
						}
						break
					}
					log.Printf("Service Bus send error (queue %s, seq %d), dead-lettering the whole message: %v", c.queue, sequence(msg), sendErr)
				}
				description := o.Description
				if description == "" {
					description = o.Reason
				}
				options := &azservicebus.DeadLetterOptions{Reason: &o.Reason, ErrorDescription: &description, PropertiesToModify: o.Properties}
				if err = receiver.DeadLetterMessage(settleCtx, msg, options); err == nil {
					deadLettered.Add(1)
				}
			default:
				if err = receiver.AbandonMessage(settleCtx, msg, nil); err == nil {
					abandoned.Add(1)
				}
			}
			if err != nil {
				log.Printf("Service Bus settle error (queue %s, seq %d): %v", c.queue, sequence(msg), err)
				r.addError(fmt.Sprintf("seq %d: could not settle the message, it will be delivered again", sequence(msg)))
			}
		}()
	}
	wg.Wait()

	for _, e := range outcome.Errors {
		r.addError(e)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.result.Received += int64(len(msgs))
	r.result.Completed += completed.Load()
	r.result.DeadLettered += deadLettered.Load()
	r.result.Abandoned += abandoned.Load()
	for k, v := range outcome.Counts {
		r.result.Counts[k] += v
	}
}

// deadLetterInvalidOnly dead-letters the invalid-items-only messages (made by Resend) and
// returns the other messages.
func (c *Consumer) deadLetterInvalidOnly(ctx context.Context, receiver *azservicebus.Receiver, msgs []*azservicebus.ReceivedMessage, r *run) []*azservicebus.ReceivedMessage {
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
	defer cancel()

	rest := msgs[:0]
	var dead int64
	for _, msg := range msgs {
		if only, _ := msg.ApplicationProperties[PropInvalidItemsOnly].(bool); !only {
			rest = append(rest, msg)
			continue
		}
		reason, _ := msg.ApplicationProperties[PropDeadLetterReason].(string)
		description, _ := msg.ApplicationProperties[PropDeadLetterDescription].(string)
		if reason == "" {
			reason = "invalid items"
		}
		if err := receiver.DeadLetterMessage(settleCtx, msg, &azservicebus.DeadLetterOptions{Reason: &reason, ErrorDescription: &description}); err != nil {
			log.Printf("Service Bus settle error (queue %s, seq %d): %v", c.queue, sequence(msg), err)
			r.addError(fmt.Sprintf("seq %d: could not settle the message, it will be delivered again", sequence(msg)))
			continue
		}
		dead++
		r.addError(fmt.Sprintf("seq %d: moved to the dead-letter queue: %s", sequence(msg), reason))
	}

	r.mu.Lock()
	r.result.Received += int64(len(msgs) - len(rest))
	r.result.DeadLettered += dead
	r.mu.Unlock()
	return rest
}

// send sends one Resend message to this consumer's queue (the sender is opened once).
func (c *Consumer) send(ctx context.Context, m *Resend) error {
	c.senderMu.Lock()
	if c.sender == nil {
		sender, err := c.client.NewSender(c.queue, nil)
		if err != nil {
			c.senderMu.Unlock()
			return err
		}
		c.sender = sender
	}
	sender := c.sender
	c.senderMu.Unlock()

	contentType := "application/json"
	return sender.SendMessage(ctx, &azservicebus.Message{Body: m.Body, ContentType: &contentType, ApplicationProperties: m.Properties}, nil)
}

// keepLocks renews the locks of msgs every lockRenewEvery until the returned stop is called.
func keepLocks(ctx context.Context, receiver *azservicebus.Receiver, msgs []*azservicebus.ReceivedMessage) (stop func()) {
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		ticker := time.NewTicker(lockRenewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				var wg sync.WaitGroup
				for _, msg := range msgs {
					wg.Add(1)
					go func() {
						defer wg.Done()
						if err := receiver.RenewMessageLock(ctx, msg, nil); err != nil {
							log.Printf("Service Bus lock renew error (seq %d): %v", sequence(msg), err)
						}
					}()
				}
				wg.Wait()
			}
		}
	}()
	return func() {
		close(done)
		<-finished
	}
}

// take reserves up to n messages from the max budget (returns 0 when max is reached).
func (r *run) take(n int) int {
	for {
		left := r.budget.Load()
		if left < 0 {
			return n
		}
		if left == 0 {
			return 0
		}
		k := min(int64(n), left)
		if r.budget.CompareAndSwap(left, left-k) {
			return int(k)
		}
	}
}

// giveBack returns reserved but not received messages to the budget.
func (r *run) giveBack(n int) {
	if n > 0 && r.budget.Load() >= 0 {
		r.budget.Add(int64(n))
	}
}

func (r *run) setFatal(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fatal == nil {
		r.fatal = err
	}
}

func (r *run) stopEarly(reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.result.StoppedEarly = reason
}

func (r *run) addError(msg string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.result.Errors) < maxReportedErrors {
		r.result.Errors = append(r.result.Errors, msg)
	}
}

func sequence(msg *azservicebus.ReceivedMessage) int64 {
	if msg.SequenceNumber == nil {
		return -1
	}
	return *msg.SequenceNumber
}
