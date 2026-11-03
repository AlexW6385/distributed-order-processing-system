package db

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Open connects to Postgres, retrying while the database is still starting.
func Open(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = int32(maxConns())
	cfg.MinConns = 1
	cfg.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	for attempt := 0; ; attempt++ {
		err = pool.Ping(ctx)
		if err == nil || attempt == 30 {
			break
		}
		select {
		case <-ctx.Done():
			pool.Close()
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	if err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// maxConns reads DB_MAX_CONNS (default 20): the per-replica connection pool.
func maxConns() int {
	if n, err := strconv.Atoi(os.Getenv("DB_MAX_CONNS")); err == nil && n > 0 {
		return n
	}
	return 20
}
