// Package grpcx holds the gRPC plumbing every service shares: request-ID
// propagation, default deadlines, access logging and panic recovery.
package grpcx

import (
	"context"
	"time"

	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/logging"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/requestid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const requestIDKey = "x-request-id"

// Dial creates a client connection whose calls carry the caller's request ID
// and never wait longer than defaultTimeout.
// Every remote call needs a deadline; without one a slow dependency holds the
// caller's goroutines and connections forever.
//
// With a dns:/// target the client resolves every replica behind the name and
// spreads calls across them (client-side round-robin load balancing).
func Dial(addr string, defaultTimeout time.Duration) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}]}`),
		grpc.WithUnaryInterceptor(clientInterceptor(defaultTimeout)),
	)
}

func clientInterceptor(defaultTimeout time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// Apply the per-hop timeout even when the caller already has a
		// deadline: WithTimeout keeps whichever is earlier. Otherwise a 5s
		// gateway deadline would flow through unchanged and a slow payment
		// call would use up the whole budget, leaving no time to answer
		// "outcome unknown" before the client gives up.
		if defaultTimeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, defaultTimeout)
			defer cancel()
		}
		if id := requestid.From(ctx); id != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, requestIDKey, id)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// NewServer returns a gRPC server that logs every call and turns panics into
// Internal errors instead of crashing the process.
func NewServer(log *logging.Logger) *grpc.Server {
	return grpc.NewServer(grpc.UnaryInterceptor(serverInterceptor(log)))
}

func serverInterceptor(log *logging.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if values := md.Get(requestIDKey); len(values) > 0 {
				ctx = requestid.With(ctx, values[0])
			}
		}
		start := time.Now()
		defer func() {
			if r := recover(); r != nil {
				log.Error("grpc handler panic", nil, map[string]any{"method": info.FullMethod, "panic": r})
				err = status.Error(codes.Internal, "internal error")
			}
			fields := map[string]any{
				"method":      info.FullMethod,
				"code":        status.Code(err).String(),
				"duration_ms": time.Since(start).Milliseconds(),
				"request_id":  requestid.From(ctx),
			}
			if err != nil && status.Code(err) == codes.Internal {
				log.Error("grpc call", err, fields)
			} else {
				log.Info("grpc call", fields)
			}
		}()
		return handler(ctx, req)
	}
}

// IsUncertain reports whether err leaves the outcome of a remote call unknown:
// the request may or may not have been applied by the server. Callers must not
// treat these as a definite failure.
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
