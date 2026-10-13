// Package avrodec decodes Confluent wire-format Avro messages using schemas
// fetched (and cached) from a Schema Registry.
package avrodec

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/hamba/avro/v2"
	"github.com/twmb/franz-go/pkg/sr"
)

type Decoder struct {
	client *sr.Client

	mu      sync.Mutex
	schemas map[int]avro.Schema
}

func New(registryURL string) (*Decoder, error) {
	client, err := sr.NewClient(sr.URLs(registryURL))
	if err != nil {
		return nil, fmt.Errorf("create schema registry client: %w", err)
	}
	return &Decoder{client: client, schemas: make(map[int]avro.Schema)}, nil
}

// Decode parses one wire-format message (magic byte + schema id + Avro binary)
// into a generic map.
func (d *Decoder) Decode(ctx context.Context, payload []byte) (map[string]any, error) {
	if len(payload) < 5 || payload[0] != 0 {
		return nil, fmt.Errorf("not a Confluent wire-format message")
	}
	id := int(binary.BigEndian.Uint32(payload[1:5]))

	schema, err := d.schema(ctx, id)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := avro.Unmarshal(schema, payload[5:], &m); err != nil {
		return nil, fmt.Errorf("avro unmarshal (schema id %d): %w", id, err)
	}
	return m, nil
}

func (d *Decoder) schema(ctx context.Context, id int) (avro.Schema, error) {
	d.mu.Lock()
	cached, ok := d.schemas[id]
	d.mu.Unlock()
	if ok {
		return cached, nil
	}

	srSchema, err := d.client.SchemaByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("fetch schema id %d: %w", id, err)
	}
	parsed, err := avro.Parse(srSchema.Schema)
	if err != nil {
		return nil, fmt.Errorf("parse schema id %d: %w", id, err)
	}

	d.mu.Lock()
	d.schemas[id] = parsed
	d.mu.Unlock()
	return parsed, nil
}
