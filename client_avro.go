package schema_registry

import (
	"context"
	"fmt"
	"github.com/amient/avro"
)

func (c *Client) RegisterAvroType(ctx context.Context, subject string, schema avro.Schema) (uint32, error) {
	return 0, fmt.Errorf("implement me: RegisterAvroType")
}

func (c *Client) deserializeAvro(ctx context.Context, schema *AvroSchema, data []byte) (*avro.GenericRecord, error) {
	return nil, fmt.Errorf("implement me: deserializeAvro")
}

func (c *Client) parseAvroSchema(ctx context.Context, definition string, refs references, name *string) (*AvroSchema, error) {
	return nil, fmt.Errorf("implement me: parseAvroSchema")
}



