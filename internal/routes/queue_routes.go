package routes

import (
	"github.com/gin-gonic/gin"

	"feed-api/internal/handler"
)

// RegisterQueueRoutes registers all /queue endpoints.
func RegisterQueueRoutes(rg *gin.RouterGroup, h *handler.QueueHandler) {
	rg.GET("/deadletter", h.DeadLetter)
	rg.GET("/:name/peek", h.Peek)
	rg.POST("/:name/process", h.Process)
}
