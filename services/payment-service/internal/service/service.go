package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/AlexW6385/distributed-order-processing-system/services/payment-service/internal/domain"
	"github.com/go-redis/redis/v8"
)

type Repository interface {
	FindByIdempotencyKey(ctx context.Context, key string) (*domain.Payment, error)
	CreateSucceeded(ctx context.Context, orderID string, amountCents int64, key string) (*domain.Payment, error)
}

type Service struct {
	repo  Repository
	redis *redis.Client
}

func New(repo Repository, redisClient *redis.Client) *Service {
	return &Service{repo: repo, redis: redisClient}
}

func (s *Service) PayOrder(ctx context.Context, orderID string, amountCents int64, key string) (*domain.Payment, error) {
	if orderID == "" {
		return nil, errors.New("order id is required")
	}
	if amountCents <= 0 {
		return nil, errors.New("amount must be positive")
	}
	if key == "" {
		return nil, errors.New("idempotency key is required")
	}

	existing, err := s.repo.FindByIdempotencyKey(ctx, key)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if existing.OrderID != orderID || existing.AmountCents != amountCents {
			return nil, errors.New("idempotency key was used for a different payment")
		}
		return existing, nil
	}

	lockKey := fmt.Sprintf("payment:idempotency:%s", key)
	locked, err := s.redis.SetNX(ctx, lockKey, "processing", 24*time.Hour).Result()
	if err != nil {
		return nil, err
	}
	if !locked {
		existing, err := s.repo.FindByIdempotencyKey(ctx, key)
		if err != nil {
			return nil, err
		}
		if existing != nil {
			return existing, nil
		}
		return nil, errors.New("payment with this idempotency key is already processing")
	}

	payment, err := s.repo.CreateSucceeded(ctx, orderID, amountCents, key)
	if err != nil {
		_ = s.redis.Del(ctx, lockKey).Err()
		return nil, err
	}
	_ = s.redis.Set(ctx, lockKey, payment.ID, 24*time.Hour).Err()
	return payment, nil
}
