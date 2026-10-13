package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type database struct {
	pool *pgxpool.Pool
}

func connect(ctx context.Context, url string) (*database, error) {
	var lastErr error
	for i := 0; i < 30; i++ {
		pool, err := pgxpool.New(ctx, url)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				return &database{pool: pool}, nil
			}
			pool.Close()
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return nil, fmt.Errorf("database not reachable after retries: %w", lastErr)
}

func (d *database) Close() { d.pool.Close() }

var columns = []string{
	"tenant_id", "payment_id", "order_id", "merchant_id", "amount", "currency",
	"fee_amount", "method", "status", "description", "auth_code",
	"error_description", "created_at", "finalized_at", "updated_at",
}

// upsertPayment applies one full-state record. The updated_at guard makes the
// upsert idempotent and safe against reordered/replayed records: stale state
// never overwrites newer state. Returns whether the row was actually written.
func (d *database) upsertPayment(ctx context.Context, row map[string]any) (bool, error) {
	args := make([]any, len(columns))
	for i, c := range columns {
		args[i] = row[c]
	}
	tag, err := d.pool.Exec(ctx, `
		INSERT INTO payments
			(tenant_id, payment_id, order_id, merchant_id, amount, currency,
			 fee_amount, method, status, description, auth_code,
			 error_description, created_at, finalized_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
		ON CONFLICT (payment_id) DO UPDATE SET
			tenant_id         = EXCLUDED.tenant_id,
			order_id          = EXCLUDED.order_id,
			merchant_id       = EXCLUDED.merchant_id,
			amount            = EXCLUDED.amount,
			currency          = EXCLUDED.currency,
			fee_amount        = EXCLUDED.fee_amount,
			method            = EXCLUDED.method,
			status            = EXCLUDED.status,
			description       = EXCLUDED.description,
			auth_code         = EXCLUDED.auth_code,
			error_description = EXCLUDED.error_description,
			created_at        = EXCLUDED.created_at,
			finalized_at      = EXCLUDED.finalized_at,
			updated_at        = EXCLUDED.updated_at
		WHERE payments.updated_at < EXCLUDED.updated_at`,
		args...)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}
