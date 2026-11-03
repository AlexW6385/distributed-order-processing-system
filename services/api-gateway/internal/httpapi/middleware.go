package httpapi

import (
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel/trace"
)

var (
	httpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request latency, by route, method and status.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"route", "method", "status"})
	rateLimited = promauto.NewCounter(prometheus.CounterOpts{
		Name: "http_rate_limited_total",
		Help: "Requests rejected by the rate limiter.",
	})
)

// Observe records RED metrics per route and returns the trace ID in the
// X-Trace-Id header so a client can look the request up in Jaeger.
func Observe() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		if sc := trace.SpanContextFromContext(c.Request.Context()); sc.HasTraceID() {
			c.Header("X-Trace-Id", sc.TraceID().String())
		}
		c.Next()
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		httpDuration.WithLabelValues(route, c.Request.Method, strconv.Itoa(c.Writer.Status())).Observe(time.Since(start).Seconds())
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
func RateLimit(rdb *redis.Client, limit int, window time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		if limit <= 0 {
			c.Next()
			return
		}
		bucket := time.Now().UnixMilli() / window.Milliseconds()
		key := fmt.Sprintf("ratelimit:%s:%d", c.ClientIP(), bucket)
		n, err := fixedWindow.Run(c.Request.Context(), rdb, []string{key}, window.Milliseconds()).Int()
		if err != nil {
			slog.WarnContext(c.Request.Context(), "rate limiter unavailable, allowing request", "error", err)
			c.Next()
			return
		}
		c.Header("X-RateLimit-Limit", strconv.Itoa(limit))
		c.Header("X-RateLimit-Remaining", strconv.Itoa(max(limit-n, 0)))
		if n > limit {
			rateLimited.Inc()
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "rate limit exceeded"})
			return
		}
		c.Next()
	}
}
