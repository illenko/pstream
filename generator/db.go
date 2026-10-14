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

// connect opens a pool, retrying while the database container starts up.
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

func (d *database) insertPayment(ctx context.Context, p payment) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO payments
			(tenant_id, payment_id, order_id, merchant_id, amount, currency,
			 fee_amount, method, card_token, status, description, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		p.TenantID, p.PaymentID, p.OrderID, p.MerchantID, p.Amount, p.Currency,
		p.FeeAmount, p.Method, p.CardToken, p.Status, p.Description, p.CreatedAt)
	return err
}

// Updates target a single partition via the composite PK (payment_id, created_at).

func (d *database) markProcessing(ctx context.Context, p payment) error {
	_, err := d.pool.Exec(ctx, `
		UPDATE payments SET status = 'processing'
		WHERE payment_id = $1 AND created_at = $2`,
		p.PaymentID, p.CreatedAt)
	return err
}

func (d *database) markSuccess(ctx context.Context, p payment, authCode string) error {
	_, err := d.pool.Exec(ctx, `
		UPDATE payments SET status = 'success', auth_code = $3, finalized_at = now()
		WHERE payment_id = $1 AND created_at = $2`,
		p.PaymentID, p.CreatedAt, authCode)
	return err
}

func (d *database) markFailed(ctx context.Context, p payment, reason string) error {
	_, err := d.pool.Exec(ctx, `
		UPDATE payments SET status = 'failed', error_description = $3, finalized_at = now()
		WHERE payment_id = $1 AND created_at = $2`,
		p.PaymentID, p.CreatedAt, reason)
	return err
}

func (d *database) markRefunded(ctx context.Context, p payment) error {
	_, err := d.pool.Exec(ctx, `
		UPDATE payments SET status = 'refunded', finalized_at = now()
		WHERE payment_id = $1 AND created_at = $2`,
		p.PaymentID, p.CreatedAt)
	return err
}
