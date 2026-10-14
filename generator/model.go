package main

import (
	cryptorand "crypto/rand"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

type payment struct {
	TenantID    string
	PaymentID   string
	OrderID     string
	MerchantID  string
	Amount      string
	Currency    string
	FeeAmount   string
	Method      string
	Status      string
	Description string
	CreatedAt   time.Time
}

var (
	currencies   = []string{"EUR", "GBP", "USD", "PLN", "ARS"}
	methods      = []string{"card", "apple_pay", "google_pay", "bank_transfer"}
	descriptions = []string{"subscription renewal", "online order", "top-up", "digital goods", "service fee"}
)

// generator produces random payments; the shared rng is mutex-guarded because
// lifecycle goroutines also draw delays and final statuses from it.
type generator struct {
	mu        sync.Mutex
	rng       *rand.Rand
	tenants   []string
	merchants map[string][]string
}

func newGenerator(seed uint64, tenants []string) *generator {
	merchants := make(map[string][]string, len(tenants))
	for _, t := range tenants {
		ids := make([]string, 3)
		for i := range ids {
			ids[i] = fmt.Sprintf("%s_merchant_%d", t, i+1)
		}
		merchants[t] = ids
	}
	return &generator{
		rng:       rand.New(rand.NewPCG(seed, seed)),
		tenants:   tenants,
		merchants: merchants,
	}
}

func (g *generator) newPayment() payment {
	g.mu.Lock()
	defer g.mu.Unlock()
	tenant := g.tenants[g.rng.IntN(len(g.tenants))]
	amount := float64(g.rng.IntN(49900)+100) / 100 // 1.00 .. 500.00
	return payment{
		TenantID:    tenant,
		PaymentID:   g.uuidV4(),
		OrderID:     fmt.Sprintf("order-%08x", g.rng.Uint32()),
		MerchantID:  g.merchants[tenant][g.rng.IntN(len(g.merchants[tenant]))],
		Amount:      fmt.Sprintf("%.2f", amount),
		Currency:    currencies[g.rng.IntN(len(currencies))],
		FeeAmount:   fmt.Sprintf("%.2f", amount*0.02),
		Method:      methods[g.rng.IntN(len(methods))],
		Status:      "created",
		Description: descriptions[g.rng.IntN(len(descriptions))],
		CreatedAt:   time.Now(),
	}
}

// delay returns a random lifecycle transition delay between 1 and 10 seconds.
func (g *generator) delay() time.Duration {
	g.mu.Lock()
	defer g.mu.Unlock()
	return time.Duration(g.rng.IntN(9000)+1000) * time.Millisecond
}

// finalStatus picks the terminal status: mostly success, some failed/refunded.
func (g *generator) finalStatus() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	switch n := g.rng.IntN(10); {
	case n < 7:
		return "success"
	case n < 9:
		return "failed"
	default:
		return "refunded"
	}
}

// uuidV4 builds a random UUID from crypto/rand, NOT from the seeded rng:
// a deterministic seed would replay the same id sequence after a generator
// restart, producing new rows that reuse old payment_ids (the composite source
// PK allows it) and silently merge in the sink. GEN_SEED keeps the business
// fields reproducible; identity must stay globally unique.
func (g *generator) uuidV4() string {
	var b [16]byte
	if _, err := cryptorand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("crypto/rand unavailable: %v", err))
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func (g *generator) authCode() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return fmt.Sprintf("%06d", g.rng.IntN(1000000))
}
