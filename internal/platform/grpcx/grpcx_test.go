package grpcx

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func deadlineSeen(t *testing.T, parent context.Context, hop time.Duration) time.Duration {
	t.Helper()
	var left time.Duration
	invoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		d, ok := ctx.Deadline()
		if !ok {
			t.Fatal("call has no deadline")
		}
		left = time.Until(d)
		return nil
	}
	if err := deadline(hop)(parent, "/m", nil, nil, nil, invoker); err != nil {
		t.Fatal(err)
	}
	return left
}

func TestHopTimeoutTightensInheritedDeadline(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if left := deadlineSeen(t, parent, 100*time.Millisecond); left > 100*time.Millisecond {
		t.Fatalf("call deadline %s away, want <= 100ms (hop timeout must win over the 5s parent)", left)
	}
}

func TestEarlierParentDeadlineWins(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if left := deadlineSeen(t, parent, time.Second); left > 50*time.Millisecond {
		t.Fatalf("call deadline %s away, want <= 50ms", left)
	}
}

func TestIsUncertain(t *testing.T) {
	for code, want := range map[codes.Code]bool{
		codes.DeadlineExceeded: true, codes.Unavailable: true, codes.Internal: true,
		codes.FailedPrecondition: false, codes.InvalidArgument: false, codes.NotFound: false,
	} {
		if got := IsUncertain(status.Error(code, "x")); got != want {
			t.Errorf("IsUncertain(%s) = %v, want %v", code, got, want)
		}
	}
	if !IsUncertain(errors.New("connection reset")) {
		t.Error("a non-status error must be treated as uncertain")
	}
}

func TestBreakerOpensAndRecovers(t *testing.T) {
	b := NewBreaker("test", 3, 50*time.Millisecond)
	call := b.interceptor()
	fail := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		return status.Error(codes.Unavailable, "down")
	}
	ok := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error { return nil }
	definite := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		return status.Error(codes.FailedPrecondition, "no stock")
	}

	// Definite errors prove the dependency is up and never trip the breaker.
	for i := 0; i < 10; i++ {
		_ = call(context.Background(), "/m", nil, nil, nil, definite)
	}
	for i := 0; i < 3; i++ {
		_ = call(context.Background(), "/m", nil, nil, nil, fail)
	}
	if err := call(context.Background(), "/m", nil, nil, nil, ok); err != ErrBreakerOpen {
		t.Fatalf("after 3 failures: err = %v, want breaker open", err)
	}

	time.Sleep(60 * time.Millisecond)
	// Half-open: one probe; it fails, so the breaker opens again.
	if err := call(context.Background(), "/m", nil, nil, nil, fail); status.Code(err) != codes.Unavailable || err == ErrBreakerOpen {
		t.Fatalf("probe should reach the server, got %v", err)
	}
	if err := call(context.Background(), "/m", nil, nil, nil, ok); err != ErrBreakerOpen {
		t.Fatalf("failed probe should reopen the breaker, got %v", err)
	}

	time.Sleep(60 * time.Millisecond)
	if err := call(context.Background(), "/m", nil, nil, nil, ok); err != nil {
		t.Fatalf("successful probe: %v", err)
	}
	if err := call(context.Background(), "/m", nil, nil, nil, ok); err != nil {
		t.Fatalf("breaker should be closed after a good probe: %v", err)
	}
}

func TestServiceConfigRetriesOnlyListedMethods(t *testing.T) {
	cfg := serviceConfig([]string{"/payment.v1.PaymentService/Charge"})
	for _, want := range []string{`"service":"payment.v1.PaymentService"`, `"method":"Charge"`, `"UNAVAILABLE"`, `"retryThrottling"`, `"round_robin"`} {
		if !strings.Contains(cfg, want) {
			t.Errorf("service config %s lacks %s", cfg, want)
		}
	}
	if strings.Contains(serviceConfig(nil), "retryPolicy") {
		t.Error("no retry policy expected without retry methods")
	}
}
