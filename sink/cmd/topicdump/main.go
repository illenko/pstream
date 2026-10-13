// Command topicdump consumes the whole topic from the beginning, prints every
// record as one JSON line (partition/offset + decoded value) and exits once it
// reaches the end offsets captured at startup. Used by `make verify`.
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
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
	log.SetFlags(0)
	log.SetOutput(os.Stderr)

	brokers := strings.Split(env("KAFKA_BROKERS", "kafka:9092"), ",")
	registryURL := env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
	topic := env("TOPIC", "payments.state")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		log.Fatalf("create kafka client: %v", err)
	}
	defer client.Close()

	ends, err := kadm.NewClient(client).ListEndOffsets(ctx, topic)
	if err != nil {
		log.Fatalf("list end offsets: %v", err)
	}
	remaining := make(map[int32]int64)
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			remaining[o.Partition] = o.Offset
		}
	})

	decoder, err := avrodec.New(registryURL)
	if err != nil {
		log.Fatal(err)
	}
	out := json.NewEncoder(os.Stdout)

	for len(remaining) > 0 {
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			log.Fatalf("timed out before reaching end offsets (remaining: %v)", remaining)
		}
		fetches.EachError(func(t string, p int32, err error) {
			log.Fatalf("fetch error topic=%s partition=%d: %v", t, p, err)
		})
		for iter := fetches.RecordIter(); !iter.Done(); {
			rec := iter.Next()
			end, tracked := remaining[rec.Partition]
			if !tracked || rec.Offset >= end {
				continue
			}
			line := map[string]any{"partition": rec.Partition, "offset": rec.Offset}
			if rec.Value == nil {
				line["tombstone"] = true
			} else {
				value, err := decoder.Decode(ctx, rec.Value)
				if err != nil {
					log.Fatalf("decode partition=%d offset=%d: %v", rec.Partition, rec.Offset, err)
				}
				line["value"] = value
			}
			if err := out.Encode(line); err != nil {
				log.Fatal(err)
			}
			if rec.Offset == end-1 {
				delete(remaining, rec.Partition)
			}
		}
	}
}
