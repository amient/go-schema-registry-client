package schema_registry

import (
	"context"
	"fmt"
	"github.com/amient/avro"
)

func (c *Client) RegisterAvroType(ctx context.Context, subject string, schema avro.Schema) (uint32, error) {
	fp, err := schema.Fingerprint()
	if err != nil {
		return 0, err
	}
	if id, ok := c.cacheAvro[*fp]; ok {
		return id, nil
	}

	refs, err := c.registerReferencedAvroSchemas(ctx, schema)
	if err != nil {
		return 0, fmt.Errorf("RegisterAvroType.registerReferencedAvroSchemas: %v", err)
	}

	id, err := c.registerSchemaUnderSubject(ctx, subject, schemaTypeAvro, schema.String(), refs)
	if err != nil {
		return 0, err
	}
	c.cacheAvro[*fp] = id
	return id, nil
}

func (c *Client) registerReferencedAvroSchemas(_ context.Context, _ avro.Schema) (references, error) {
	result := make(references, 0)
	//TODO #4
	return result, nil
}

func (c *Client) deserializeAvro(_ context.Context, schema *AvroSchema, data []byte) (*avro.GenericRecord, error) {
	decodedRecord := avro.NewGenericRecord(schema.avro)
	reader := avro.NewDatumReader(schema.avro)
	if err := reader.Read(decodedRecord, avro.NewBinaryDecoder(data[5:])); err != nil {
		return nil, err
	}
	return decodedRecord, nil
}

func (c *Client) deserializeAvroInto(_ context.Context, schema *AvroSchema, payload []byte, value avro.AvroRecord) error {
	reader, err := avro.NewDatumProjector(value.Schema(), schema.avro)
	if err != nil {
		return err
	}
	if err := reader.Read(value, avro.NewBinaryDecoder(payload)); err != nil {
		return err
	}
	return nil
}

func (c *Client) parseAvroSchema(_ context.Context, definition string, _ references, _ *string) (*AvroSchema, error) {
	schema, err := avro.ParseSchema(definition)
	if err != nil {
		return nil, err
	}
	return &AvroSchema{avro: schema},nil
}



