package handler

import (
	"crypto/subtle"
	"net/http"

	"github.com/gin-gonic/gin"
)

// RequireAdminToken protects admin routes: the X-Admin-Token header must equal QUEUE_ADMIN_TOKEN.
//
//	token not configured => 503 (the routes are off)
//	header missing/wrong => 401
func RequireAdminToken(token string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if token == "" {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "QUEUE_ADMIN_TOKEN is not configured."})
			return
		}
		sent := c.GetHeader("X-Admin-Token")
		if subtle.ConstantTimeCompare([]byte(sent), []byte(token)) != 1 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Missing or invalid X-Admin-Token header."})
			return
		}
		c.Next()
	}
}
