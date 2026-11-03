// Package grpcx holds the gRPC plumbing every service shares: deadlines,
// retries, circuit breaking, tracing, metrics and health checks.
package grpcx

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

var (
	serverDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "grpc_server_handling_seconds",
		Help:    "Time to handle a gRPC request, by method and status code.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"method", "code"})
	clientDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "grpc_client_handling_seconds",
		Help:    "Time for an outgoing gRPC call including retries, by method and status code.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"method", "code"})
)

// ClientOptions configures a connection to one downstream service.
type ClientOptions struct {
	// Timeout is the per-hop deadline. It always applies, capped by the
	// caller's own deadline, so one slow hop cannot spend the whole budget
	// of the request above it.
	Timeout time.Duration
	// Breaker, if set, fails calls fast while the downstream is unhealthy.
	Breaker *Breaker
	// RetryMethods lists fully qualified methods ("/pkg.Service/Method")
	// that are idempotent and may be retried on UNAVAILABLE.
	RetryMethods []string
}

// Dial creates a client connection. With a dns:/// target the client
// resolves every replica behind the name and balances calls round-robin.
func Dial(target string, o ClientOptions) (*grpc.ClientConn, error) {
	interceptors := []grpc.UnaryClientInterceptor{clientMetrics, deadline(o.Timeout)}
	if o.Breaker != nil {
		// The breaker sits outside the retries: it sees the final outcome of
		// a call, after gRPC has already retried transient failures.
		interceptors = append([]grpc.UnaryClientInterceptor{o.Breaker.interceptor()}, interceptors...)
	}
	return grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithChainUnaryInterceptor(interceptors...),
		grpc.WithDefaultServiceConfig(serviceConfig(o.RetryMethods)),
	)
}

// serviceConfig enables round-robin balancing and transparent retries with
// exponential backoff and jitter (built into grpc-go) for idempotent methods.
// retryThrottling caps retries at roughly 10% of traffic once failures pile
// up, so retries cannot amplify an outage.
func serviceConfig(retryMethods []string) string {
	cfg := map[string]any{"loadBalancingConfig": []any{map[string]any{"round_robin": map[string]any{}}}}
	if len(retryMethods) > 0 {
		var names []map[string]string
		for _, m := range retryMethods {
			// "/pkg.Service/Method" -> {"service": "pkg.Service", "method": "Method"}
			for i := len(m) - 1; i > 0; i-- {
				if m[i] == '/' {
					names = append(names, map[string]string{"service": m[1:i], "method": m[i+1:]})
					break
				}
			}
		}
		cfg["methodConfig"] = []any{map[string]any{
			"name": names,
			"retryPolicy": map[string]any{
				"maxAttempts":          3,
				"initialBackoff":       "0.05s",
				"maxBackoff":           "0.5s",
				"backoffMultiplier":    2,
				"retryableStatusCodes": []string{"UNAVAILABLE"},
			},
		}}
		cfg["retryThrottling"] = map[string]any{"maxTokens": 10, "tokenRatio": 0.1}
	}
	raw, _ := json.Marshal(cfg)
	return string(raw)
}

func deadline(timeout time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout) // keeps the earlier deadline
			defer cancel()
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func clientMetrics(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	start := time.Now()
	err := invoker(ctx, method, req, reply, cc, opts...)
	clientDuration.WithLabelValues(method, status.Code(err).String()).Observe(time.Since(start).Seconds())
	return err
}

// NewServer returns a gRPC server with tracing, metrics, error logging and
// panic recovery, plus a registered health service. Services flip health to
// SERVING once ready and back to NOT_SERVING when shutting down.
func NewServer() (*grpc.Server, *health.Server) {
	server := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(serverInterceptor),
	)
	h := health.NewServer()
	h.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	healthpb.RegisterHealthServer(server, h)
	return server, h
}

func serverInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	start := time.Now()
	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx, "grpc handler panic", "method", info.FullMethod, "panic", r)
			err = status.Error(codes.Internal, "internal error")
		}
		code := status.Code(err)
		serverDuration.WithLabelValues(info.FullMethod, code.String()).Observe(time.Since(start).Seconds())
		if code == codes.Internal || code == codes.Unknown {
			slog.ErrorContext(ctx, "grpc call failed", "method", info.FullMethod, "error", err)
		}
	}()
	return handler(ctx, req)
}

// IsUncertain reports whether err leaves the outcome of a remote call unknown:
// the request may or may not have been applied. Callers must not treat these
// as a definite failure.
func IsUncertain(err error) bool {
	if err == nil {
		return false
	}
	switch status.Code(err) {
	case codes.InvalidArgument, codes.NotFound, codes.AlreadyExists,
		codes.FailedPrecondition, codes.PermissionDenied, codes.Unauthenticated,
		codes.OutOfRange, codes.Unimplemented:
		return false
	}
	return true
}
