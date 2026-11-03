// Package obs wires up the three observability signals every service emits:
// structured logs (log/slog, JSON, with trace IDs), traces (OpenTelemetry,
// exported over OTLP) and metrics (Prometheus, served on /metrics).
package obs

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Setup installs the global logger, tracer provider and propagator for a
// service. Traces are exported only when OTEL_EXPORTER_OTLP_ENDPOINT is set.
// The returned function flushes pending spans and must run before exit.
func Setup(ctx context.Context, service string) (shutdown func(context.Context) error) {
	slog.SetDefault(slog.New(traceHandler{slog.NewJSONHandler(os.Stdout, nil)}).With("service", service))
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return func(context.Context) error { return nil }
	}
	exporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		slog.Error("trace exporter disabled", "error", err)
		return func(context.Context) error { return nil }
	}
	res, _ := resource.Merge(resource.Default(), resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName(service)))
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(sampleRatio()))),
	)
	otel.SetTracerProvider(provider)
	return provider.Shutdown
}

func sampleRatio() float64 {
	if f, err := strconv.ParseFloat(os.Getenv("OTEL_TRACES_SAMPLER_ARG"), 64); err == nil {
		return f
	}
	return 1
}

// Tracer returns the tracer for hand-made spans (Kafka, background work).
func Tracer() trace.Tracer { return otel.Tracer("order-system") }

// ServeMetrics serves /metrics (Prometheus) and /healthz on addr until ctx is
// cancelled. gRPC services use this port only for metrics; their probes use
// the gRPC health protocol.
func ServeMetrics(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	server := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server stopped", "error", err)
		}
	}()
}

// traceHandler adds trace_id and span_id to every record logged with a
// context that carries a span, so logs and traces can be joined.
type traceHandler struct{ slog.Handler }

func (h traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
	}
	return h.Handler.Handle(ctx, r)
}

func (h traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return traceHandler{h.Handler.WithAttrs(attrs)}
}

func (h traceHandler) WithGroup(name string) slog.Handler {
	return traceHandler{h.Handler.WithGroup(name)}
}

// TraceParent returns the W3C traceparent of the span in ctx, or "". It is
// stored with outbox rows so the publisher can continue the trace in Kafka.
func TraceParent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier.Get("traceparent")
}
