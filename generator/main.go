// Command generator writes multi-tenant payments into the source database and
// drives each payment through its lifecycle with randomized delays.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type config struct {
	databaseURL    string
	createInterval time.Duration
	maxInflight    int
	seed           uint64
	tenants        []string
}

func loadConfig() config {
	cfg := config{
		databaseURL:    mustEnv("DATABASE_URL"),
		createInterval: 500 * time.Millisecond,
		maxInflight:    100,
		seed:           1,
		tenants:        []string{"tenant_a", "tenant_b", "tenant_c"},
	}
	if v := os.Getenv("GEN_CREATE_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			log.Fatalf("invalid GEN_CREATE_INTERVAL %q: %v", v, err)
		}
		cfg.createInterval = d
	}
	if v := os.Getenv("GEN_MAX_INFLIGHT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			log.Fatalf("invalid GEN_MAX_INFLIGHT %q", v)
		}
		cfg.maxInflight = n
	}
	if v := os.Getenv("GEN_SEED"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			log.Fatalf("invalid GEN_SEED %q", v)
		}
		cfg.seed = n
	}
	if v := os.Getenv("GEN_TENANTS"); v != "" {
		cfg.tenants = strings.Split(v, ",")
	}
	return cfg
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("%s is required", key)
	}
	return v
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := connect(ctx, cfg.databaseURL)
	if err != nil {
		log.Fatalf("connect to database: %v", err)
	}
	defer db.Close()

	gen := newGenerator(cfg.seed, cfg.tenants)
	inflight := make(chan struct{}, cfg.maxInflight)

	log.Printf("generator started: interval=%s max_inflight=%d tenants=%s seed=%d",
		cfg.createInterval, cfg.maxInflight, strings.Join(cfg.tenants, ","), cfg.seed)

	ticker := time.NewTicker(cfg.createInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("shutting down")
			return
		case <-ticker.C:
			select {
			case inflight <- struct{}{}:
				p := gen.newPayment()
				if err := db.insertPayment(ctx, p); err != nil {
					log.Printf("ERROR insert tenant=%s payment_id=%s: %v", p.TenantID, p.PaymentID, err)
					<-inflight
					continue
				}
				log.Printf("INSERT tenant=%s payment_id=%s status=created", p.TenantID, p.PaymentID)
				go func() {
					defer func() { <-inflight }()
					runLifecycle(ctx, db, gen, p)
				}()
			default:
				// max in-flight lifecycles reached; skip this tick
			}
		}
	}
}
