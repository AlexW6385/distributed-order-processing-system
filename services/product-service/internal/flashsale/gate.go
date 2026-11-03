// Package flashsale puts hot products behind a Redis admission gate.
//
// A flash sale sends thousands of buyers at one product. In Postgres they all
// queue on the same row lock, even the ones that are going to be told "sold
// out". The gate answers those requests from Redis in one atomic Lua call and
// lets only admitted orders reach Postgres.
//
// Redis is admission control, not the source of truth: Postgres still checks
// stock under the row lock. If the Redis counter drifts (Redis restarts, a
// process dies between admission and commit), the sale can undersell, never
// oversell.
package flashsale

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-redis/redis/v8"
)

var (
	ErrSoldOut = errors.New("flash sale sold out")
	ErrLimit   = errors.New("flash sale limit is one unit per customer")
)

// Keys share a {hash tag} so a sale's keys live on one Redis Cluster slot,
// which Lua scripts require.
func keys(productID string) []string {
	return []string{
		fmt.Sprintf("flash:{%s}:tokens", productID),
		fmt.Sprintf("flash:{%s}:buyers", productID),
		fmt.Sprintf("flash:{%s}:orders", productID),
	}
}

// admit returns 1 admitted (or already admitted: retries are idempotent),
// 0 not a flash product, -1 sold out, -2 per-customer limit reached.
var admit = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
if redis.call('SISMEMBER', KEYS[3], ARGV[1]) == 1 then return 1 end
if tonumber(ARGV[3]) > 1 or redis.call('SISMEMBER', KEYS[2], ARGV[2]) == 1 then return -2 end
if tonumber(redis.call('GET', KEYS[1])) < tonumber(ARGV[3]) then return -1 end
redis.call('DECRBY', KEYS[1], ARGV[3])
redis.call('SADD', KEYS[2], ARGV[2])
redis.call('SADD', KEYS[3], ARGV[1])
return 1
`)

// undo returns an admitted order's units. It acts only if the order is still
// admitted, so repeated releases return the units once.
var undo = redis.NewScript(`
if redis.call('SREM', KEYS[3], ARGV[1]) == 1 then
  redis.call('INCRBY', KEYS[1], ARGV[3])
  redis.call('SREM', KEYS[2], ARGV[2])
  return 1
end
return 0
`)

type Gate struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *Gate { return &Gate{rdb: rdb} }

// Start opens a sale with stock units and clears earlier buyers.
func (g *Gate) Start(ctx context.Context, productID string, stock int) error {
	k := keys(productID)
	_, err := g.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, k[0], stock, 0)
		p.Del(ctx, k[1], k[2])
		return nil
	})
	return err
}

// Admit asks the gate for qty units. ok is false when the product is not on
// flash sale, in which case the caller uses the normal path.
func (g *Gate) Admit(ctx context.Context, productID, orderID, customer string, qty int32) (ok bool, err error) {
	code, err := admit.Run(ctx, g.rdb, keys(productID), orderID, customer, qty).Int()
	if err != nil {
		return false, err
	}
	switch code {
	case 1:
		return true, nil
	case -1:
		return false, ErrSoldOut
	case -2:
		return false, ErrLimit
	}
	return false, nil
}

// Undo gives back units admitted for an order that will not complete.
func (g *Gate) Undo(ctx context.Context, productID, orderID, customer string, qty int32) error {
	return undo.Run(ctx, g.rdb, keys(productID), orderID, customer, qty).Err()
}
