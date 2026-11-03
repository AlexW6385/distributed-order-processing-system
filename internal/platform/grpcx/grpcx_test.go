package grpcx

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/requestid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
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
	if err := clientInterceptor(hop)(parent, "/m", nil, nil, nil, invoker); err != nil {
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

func TestRequestIDIsPropagated(t *testing.T) {
	var got []string
	invoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		md, _ := metadata.FromOutgoingContext(ctx)
		got = md.Get(requestIDKey)
		return nil
	}
	ctx := requestid.With(context.Background(), "req-1")
	_ = clientInterceptor(time.Second)(ctx, "/m", nil, nil, nil, invoker)
	if len(got) != 1 || got[0] != "req-1" {
		t.Fatalf("metadata request id = %v", got)
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
