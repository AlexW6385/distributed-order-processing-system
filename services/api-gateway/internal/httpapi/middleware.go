package httpapi

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/logging"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/requestid"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

// RequestID assigns every request a correlation ID (or keeps the client's
// X-Request-ID) and puts it in the context so gRPC calls carry it downstream.
func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader("X-Request-ID")
		if id == "" {
			id = uuid.NewString()
		}
		c.Header("X-Request-ID", id)
		c.Request = c.Request.WithContext(requestid.With(c.Request.Context(), id))
		c.Next()
	}
}

// AccessLog logs one JSON line per request.
func AccessLog(log *logging.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		log.Info("http request", map[string]any{
			"method":      c.Request.Method,
			"path":        c.FullPath(),
			"status":      c.Writer.Status(),
			"duration_ms": time.Since(start).Milliseconds(),
			"request_id":  requestid.From(c.Request.Context()),
		})
	}
}

// fixedWindow atomically counts a request in the current window and returns
// the count. INCR and PEXPIRE run as one script so a crash between them cannot
// leave a counter without a TTL.
var fixedWindow = redis.NewScript(`
local n = redis.call('INCR', KEYS[1])
if n == 1 then redis.call('PEXPIRE', KEYS[1], ARGV[1]) end
return n
`)

// RateLimit allows limit requests per client IP per window. The counter lives
// in Redis, so every gateway replica enforces one shared limit. If Redis is
// unreachable the limiter fails open: availability over strictness.
func RateLimit(rdb *redis.Client, limit int, window time.Duration, log *logging.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		if limit <= 0 {
			c.Next()
			return
		}
		bucket := time.Now().UnixMilli() / window.Milliseconds()
		key := fmt.Sprintf("ratelimit:%s:%d", c.ClientIP(), bucket)
		n, err := fixedWindow.Run(c.Request.Context(), rdb, []string{key}, window.Milliseconds()).Int()
		if err != nil {
			log.Error("rate limiter unavailable, allowing request", err, nil)
			c.Next()
			return
		}
		remaining := limit - n
		if remaining < 0 {
			remaining = 0
		}
		c.Header("X-RateLimit-Limit", strconv.Itoa(limit))
		c.Header("X-RateLimit-Remaining", strconv.Itoa(remaining))
		if n > limit {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "rate limit exceeded"})
			return
		}
		c.Next()
	}
}
