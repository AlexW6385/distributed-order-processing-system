// Package app holds the startup boilerplate shared by every service binary.
package app

import (
	"context"
	"io/fs"
	"net"
	"os"
	"os/signal"
	"syscall"

	platformdb "github.com/AlexW6385/distributed-order-processing-system/internal/platform/db"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/logging"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/migrate"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
)

// SignalContext is cancelled on SIGINT or SIGTERM.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// Must logs err and exits if it is not nil.
func Must(log *logging.Logger, what string, err error) {
	if err != nil {
		log.Error(what, err, nil)
		os.Exit(1)
	}
}

// OpenDB connects to Postgres and applies the service's migrations.
func OpenDB(ctx context.Context, log *logging.Logger, url string, migrations fs.FS) *pgxpool.Pool {
	db, err := platformdb.Open(ctx, url)
	Must(log, "connect database", err)
	Must(log, "run migrations", migrate.Run(ctx, db, migrations))
	return db
}

// ServeGRPC serves until ctx is cancelled, then drains in-flight calls.
func ServeGRPC(ctx context.Context, log *logging.Logger, server *grpc.Server, port string) {
	listener, err := net.Listen("tcp", ":"+port)
	Must(log, "listen", err)
	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()
	log.Info("grpc server started", map[string]any{"addr": listener.Addr().String()})
	if err := server.Serve(listener); err != nil {
		log.Error("grpc server stopped", err, nil)
	}
}
