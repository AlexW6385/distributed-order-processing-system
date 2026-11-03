// Package cache holds the product catalog in Redis (cache-aside).
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/domain"
	"github.com/go-redis/redis/v8"
)

const productsKey = "product:list:v1"

type Redis struct {
	client *redis.Client
	ttl    time.Duration
}

func NewRedis(client *redis.Client, ttl time.Duration) *Redis {
	return &Redis{client: client, ttl: ttl}
}

// GetProducts returns the cached catalog; ok is false on a miss.
func (r *Redis) GetProducts(ctx context.Context) (products []domain.Product, ok bool, err error) {
	raw, err := r.client.Get(ctx, productsKey).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if err := json.Unmarshal(raw, &products); err != nil {
		return nil, false, err
	}
	return products, true, nil
}

func (r *Redis) SetProducts(ctx context.Context, products []domain.Product) error {
	raw, err := json.Marshal(products)
	if err != nil {
		return err
	}
	return r.client.Set(ctx, productsKey, raw, r.ttl).Err()
}

func (r *Redis) Invalidate(ctx context.Context) error {
	return r.client.Del(ctx, productsKey).Err()
}
