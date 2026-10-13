// Command sink consumes payments.state and upserts each record into the sink
// database. Offsets are committed only after a successful upsert.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"

	"sink/internal/avrodec"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	brokers := strings.Split(env("KAFKA_BROKERS", "kafka:9092"), ",")
	registryURL := env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
	topic := env("TOPIC", "payments.state")
	group := env("GROUP", "sink")
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := connect(ctx, databaseURL)
	if err != nil {
		log.Fatalf("connect to database: %v", err)
	}
	defer db.Close()

	decoder, err := avrodec.New(registryURL)
	if err != nil {
		log.Fatal(err)
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Fatalf("create kafka client: %v", err)
	}
	defer client.Close()

	log.Printf("sink started: topic=%s group=%s", topic, group)

	for {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			log.Printf("shutting down")
			return
		}
		fetches.EachError(func(t string, p int32, err error) {
			log.Fatalf("fetch error topic=%s partition=%d: %v", t, p, err)
		})

		for iter := fetches.RecordIter(); !iter.Done(); {
			rec := iter.Next()
			if rec.Value == nil {
				// Tombstones are dropped by the connector, but skip defensively.
				commit(ctx, client, rec)
				continue
			}
			row, err := decoder.Decode(ctx, rec.Value)
			if err != nil {
				log.Fatalf("decode record partition=%d offset=%d: %v", rec.Partition, rec.Offset, err)
			}
			applied, err := db.upsertPayment(ctx, row)
			if err != nil {
				log.Fatalf("upsert payment_id=%v: %v", row["payment_id"], err)
			}
			log.Printf("UPSERT payment_id=%v status=%v applied=%t", row["payment_id"], row["status"], applied)
			commit(ctx, client, rec)
		}
	}
}

func commit(ctx context.Context, client *kgo.Client, rec *kgo.Record) {
	if err := client.CommitRecords(ctx, rec); err != nil && ctx.Err() == nil {
		log.Fatalf("commit offset partition=%d offset=%d: %v", rec.Partition, rec.Offset, err)
	}
}
