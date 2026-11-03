package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"slices"
	"strconv"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/gin-gonic/gin"

	"feed-api/internal/queue"
)

const (
	peekDefaultCount = 10
	peekMaxCount     = 100
)

type QueueHandler struct {
	client    *azservicebus.Client       // nil when Service Bus is not configured
	peeker    *queue.Peeker              // read-only view, nil when not configured
	publisher *queue.Publisher           // pushes messages into queues, nil when not configured
	queues    []string                   // allowed queue names (SERVICE_BUS_ENTITY)
	consumers map[string]*queue.Consumer // queues that can be processed (pick => insert => delete)
}

func NewQueueHandler(client *azservicebus.Client, queues []string, consumers map[string]*queue.Consumer) *QueueHandler {
	h := &QueueHandler{client: client, queues: queues, consumers: consumers}
	if client != nil {
		h.peeker = queue.NewPeeker(client)
		h.publisher = queue.NewPublisher(client)
	}
	return h
}

// Peek shows messages of one queue WITHOUT removing them.
// GET /queue/:name/peek?count=10&fromSequence=0
func (h *QueueHandler) Peek(c *gin.Context) {
	h.peek(c, c.Param("name"), false)
}

// DeadLetter shows the dead-letter messages of one queue WITHOUT removing them,
// with the reason each one was dead-lettered.
// GET /queue/deadletter?queue=vendor&count=10&fromSequence=0
func (h *QueueHandler) DeadLetter(c *gin.Context) {
	name := c.Query("queue")
	if name == "" {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "The queue query parameter is required, e.g. /queue/deadletter?queue=vendor"})
		return
	}
	h.peek(c, name, true)
}

// peek is shared by Peek and DeadLetter.
func (h *QueueHandler) peek(c *gin.Context, name string, deadLetter bool) {
	// 1. Service Bus must be configured
	if h.client == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Service Bus is not configured."})
		return
	}

	// 2. Only queues listed in SERVICE_BUS_ENTITY
	if !slices.Contains(h.queues, name) {
		c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("Unknown queue %q. Allowed: %v", name, h.queues)})
		return
	}

	// 3. count 1..100 (default 10), fromSequence >= 0 (default 0 = oldest)
	count, err := strconv.Atoi(c.DefaultQuery("count", strconv.Itoa(peekDefaultCount)))
	if err != nil || count < 1 || count > peekMaxCount {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": fmt.Sprintf("count must be a number from 1 to %d.", peekMaxCount)})
		return
	}
	fromSequence, err := strconv.ParseInt(c.DefaultQuery("fromSequence", "0"), 10, 64)
	if err != nil || fromSequence < 0 {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "fromSequence must be 0 or a positive whole number."})
		return
	}

	// 4. Peek (read-only)
	result, err := h.peeker.Peek(c.Request.Context(), name, deadLetter, count, fromSequence)
	if err != nil {
		log.Printf("Service Bus peek error (queue %s, dead-letter %v): %v", name, deadLetter, err)
		c.JSON(http.StatusBadGateway, gin.H{"error": "Could not read the queue."})
		return
	}

	kind := "messages"
	if deadLetter {
		kind = "dead-letter messages"
	}
	c.JSON(http.StatusOK, gin.H{
		"message": fmt.Sprintf("Peeked %d %s (not removed)", result.Count, kind),
		"data":    result,
	})
}

// Send pushes the JSON body (an object or an array) into the queue as a message.
// A big array is split into several messages automatically. The content is checked later by process.
// POST /queue/:name/send
func (h *QueueHandler) Send(c *gin.Context) {
	// 1. Service Bus must be configured
	if h.client == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Service Bus is not configured."})
		return
	}

	// 2. Only queues listed in SERVICE_BUS_ENTITY
	name := c.Param("name")
	if !slices.Contains(h.queues, name) {
		c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("Unknown queue %q. Allowed: %v", name, h.queues)})
		return
	}

	// 3. Body must be a JSON object or array
	body, err := c.GetRawData()
	trimmed := bytes.TrimSpace(body)
	if err != nil || len(trimmed) == 0 || !json.Valid(trimmed) || (trimmed[0] != '{' && trimmed[0] != '[') {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "The body must be a JSON object or a JSON array."})
		return
	}

	// 4. Send
	result, err := h.publisher.Send(c.Request.Context(), name, trimmed)
	switch {
	case errors.Is(err, queue.ErrEmptyArray):
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "The array is empty: nothing to send."})
		return
	case errors.Is(err, azservicebus.ErrMessageTooLarge):
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "One item is larger than the queue's maximum message size."})
		return
	case err != nil:
		log.Printf("Service Bus send error (queue %s): %v", name, err)
		c.JSON(http.StatusBadGateway, gin.H{"error": "Could not send to the queue."})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": fmt.Sprintf("Sent %d items in %d messages to queue %s", result.Items, result.Messages, name),
		"data":    result,
	})
}

// Process picks messages (PeekLock), saves them to the DB, then deletes them from the queue.
// POST /queue/:name/process?max=1000   (max=0 or missing = until the queue is empty)
func (h *QueueHandler) Process(c *gin.Context) {
	// 1. Service Bus must be configured
	if h.client == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Service Bus is not configured."})
		return
	}

	// 2. Only queues that have a processor (e.g. vendor)
	name := c.Param("name")
	consumer, ok := h.consumers[name]
	if !ok || !slices.Contains(h.queues, name) {
		c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("Queue %q cannot be processed (no processor or not in SERVICE_BUS_ENTITY).", name)})
		return
	}

	// 3. max >= 0
	limit, err := strconv.Atoi(c.DefaultQuery("max", "0"))
	if err != nil || limit < 0 {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "max must be 0 or a positive whole number (0 = until the queue is empty)."})
		return
	}

	// 4. Run: receive => save => complete / dead-letter / abandon
	result, err := consumer.Process(c.Request.Context(), limit)
	if errors.Is(err, queue.ErrAlreadyRunning) {
		c.JSON(http.StatusConflict, gin.H{"error": "This queue is already being processed. Try again when the current run ends."})
		return
	}
	if err != nil {
		log.Printf("Service Bus process error (queue %s): %v", name, err)
		c.JSON(http.StatusBadGateway, gin.H{"error": "Could not read the queue.", "data": result})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": fmt.Sprintf("Processed %d messages", result.Received),
		"data":    result,
	})
}
