package grpcx

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Breaker is a circuit breaker for one downstream service.
//
//	closed ──N consecutive uncertain failures──▶ open ──cool-down──▶ half-open
//	  ▲                                                                 │
//	  └──────────────── probe succeeds ◀────────────────────────────────┘
//	                    probe fails ──▶ open again
//
// While open, calls fail immediately with Unavailable instead of waiting for
// their timeout. The caller treats that like any other uncertain failure (the
// saga's recovery worker retries later), but without tying up goroutines
// and connections on a dependency that is down.
type Breaker struct {
	name      string
	threshold int
	coolDown  time.Duration

	mu       sync.Mutex
	state    breakerState
	failures int
	openedAt time.Time
	probing  bool
}

type breakerState int

const (
	closed breakerState = iota
	open
	halfOpen
)

var breakerStateGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "circuit_breaker_state",
	Help: "Circuit breaker state per downstream: 0 closed, 1 open, 2 half-open.",
}, []string{"target"})

// ErrBreakerOpen is returned while the breaker rejects calls.
var ErrBreakerOpen = status.Error(codes.Unavailable, breakerOpenMessage)

const breakerOpenMessage = "circuit breaker open"

// IsBreakerOpen reports whether err came from an open breaker. Unlike other
// Unavailable errors, its outcome is certain: the request was never sent.
func IsBreakerOpen(err error) bool {
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Unavailable && st.Message() == breakerOpenMessage
}

func NewBreaker(name string, threshold int, coolDown time.Duration) *Breaker {
	b := &Breaker{name: name, threshold: threshold, coolDown: coolDown}
	breakerStateGauge.WithLabelValues(name).Set(0)
	return b
}

// allow reports whether a call may proceed.
func (b *Breaker) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case open:
		if time.Since(b.openedAt) < b.coolDown {
			return false
		}
		b.setState(halfOpen)
		fallthrough
	case halfOpen:
		if b.probing {
			return false // one probe at a time
		}
		b.probing = true
	}
	return true
}

// record updates the breaker with the outcome of an allowed call. Only
// uncertain errors count as failures: a definite answer such as
// FailedPrecondition proves the dependency is up.
func (b *Breaker) record(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	failed := IsUncertain(err) && status.Code(err) != codes.Canceled
	switch {
	case b.state == halfOpen && failed:
		b.probing = false
		b.trip()
	case b.state == halfOpen:
		b.probing = false
		b.failures = 0
		b.setState(closed)
	case failed:
		b.failures++
		if b.failures >= b.threshold {
			b.trip()
		}
	default:
		b.failures = 0
	}
}

func (b *Breaker) trip() {
	b.openedAt = time.Now()
	b.setState(open)
}

func (b *Breaker) setState(s breakerState) {
	b.state = s
	breakerStateGauge.WithLabelValues(b.name).Set(float64(s))
}

func (b *Breaker) interceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if !b.allow() {
			return ErrBreakerOpen
		}
		err := invoker(ctx, method, req, reply, cc, opts...)
		b.record(err)
		return err
	}
}
