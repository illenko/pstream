package main

import (
	"context"
	"log"
	"time"
)

// runLifecycle drives one payment: created -> processing -> (success|failed|refunded).
// Each transition is a separate UPDATE; final payments are never touched again.
func runLifecycle(ctx context.Context, db *database, gen *generator, p payment) {
	if !sleep(ctx, gen.delay()) {
		return
	}
	if err := db.markProcessing(ctx, p); err != nil {
		if ctx.Err() == nil {
			log.Printf("ERROR update tenant=%s payment_id=%s created->processing: %v", p.TenantID, p.PaymentID, err)
		}
		return
	}
	log.Printf("UPDATE tenant=%s payment_id=%s created->processing", p.TenantID, p.PaymentID)

	if !sleep(ctx, gen.delay()) {
		return
	}
	final := gen.finalStatus()
	var err error
	switch final {
	case "success":
		err = db.markSuccess(ctx, p, gen.authCode())
	case "failed":
		err = db.markFailed(ctx, p, "issuer declined the transaction")
	default:
		err = db.markRefunded(ctx, p)
	}
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("ERROR update tenant=%s payment_id=%s processing->%s: %v", p.TenantID, p.PaymentID, final, err)
		}
		return
	}
	log.Printf("UPDATE tenant=%s payment_id=%s processing->%s", p.TenantID, p.PaymentID, final)
}

// sleep waits d or until ctx is cancelled; returns false when cancelled.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
