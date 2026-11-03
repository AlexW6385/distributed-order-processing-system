// Package app holds the startup boilerplate shared by every service binary.
package app

import (
	"context"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/config"
	platformdb "github.com/AlexW6385/distributed-order-processing-system/internal/platform/db"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/migrate"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/obs"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Start sets up logging, tracing and the metrics endpoint, and returns a
// context cancelled on SIGINT/SIGTERM plus a cleanup function for main to defer.
func Start(service string) (context.Context, func()) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	shutdownTracing := obs.Setup(ctx, service)
	obs.ServeMetrics(ctx, config.String("METRICS_ADDR", ":9090"))
	return ctx, func() {
		stop()
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(flushCtx)
	}
}

// Must logs err and exits if it is not nil.
func Must(what string, err error) {
	if err != nil {
		slog.Error(what, "error", err)
		os.Exit(1)
	}
}

// OpenDB connects to Postgres and applies the service's migrations.
func OpenDB(ctx context.Context, url string, migrations fs.FS) *pgxpool.Pool {
	db, err := platformdb.Open(ctx, url)
	Must("connect database", err)
	Must("run migrations", migrate.Run(ctx, db, migrations))
	return db
}

// ServeGRPC marks the service SERVING and serves until ctx is cancelled.
// Shutdown follows the Kubernetes pod lifecycle: report NOT_SERVING so the
// readiness probe fails and the pod leaves the Service's endpoints, wait for
// that to propagate, then drain in-flight calls.
func ServeGRPC(ctx context.Context, server *grpc.Server, h *health.Server, port string) {
	listener, err := net.Listen("tcp", ":"+port)
	Must("listen", err)
	go func() {
		<-ctx.Done()
		h.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
		time.Sleep(config.Duration("SHUTDOWN_DELAY", 0))
		server.GracefulStop()
	}()
	h.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	slog.Info("grpc server started", "addr", listener.Addr().String())
	if err := server.Serve(listener); err != nil {
		slog.Error("grpc server stopped", "error", err)
	}
}
