package registry

import (
	"github.com/amient/avro"
)

type AvroSchema struct {
	avro avro.Schema
}

func NewAvroSchema(jsonSchema string) (*AvroSchema, error) {
	s, err := avro.ParseSchema(jsonSchema)
	if err != nil {
		return nil, err
	}
	return &AvroSchema{avro: s},nil
}

func (s *AvroSchema) Type() string {
	return schemaTypeAvro
}

func (s *AvroSchema) Render() (string, error) {
	return s.avro.String(), nil
}

//func (s *AvroSchema) Fingerprint() (*Fingerprint, error) {
//	f, err := s.schema.Fingerprint()
//	if err != nil {
//		return nil, err
//	}
//	fp := Fingerprint(*f)
//	return &fp, nil
//}



