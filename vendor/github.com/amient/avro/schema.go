package avro

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// ***********************
// NOTICE this file was changed beginning in November 2016 by the team maintaining
// https://github.com/go-avro/avro. This notice is required to be here due to the
// terms of the Apache license, see LICENSE for details.
// ***********************

type Fingerprint [32]byte

func (f *Fingerprint) Equal(other *Fingerprint) bool {
	for i, b := range f {
		if other[i] != b {
			return false
		}
	}
	return true
}

const (
	// Record schema type constant
	Record int = iota

	// Enum schema type constant
	Enum

	// Array schema type constant
	Array

	// Map schema type constant
	Map

	// Union schema type constant
	Union

	// Fixed schema type constant
	Fixed

	// String schema type constant
	String

	// Bytes schema type constant
	Bytes

	// Int schema type constant
	Int

	// Long schema type constant
	Long

	// Float schema type constant
	Float

	// Double schema type constant
	Double

	// Boolean schema type constant
	Boolean

	// Null schema type constant
	Null

	// Recursive schema type constant. Recursive is an artificial type that means a Record schema without its definition
	// that should be looked up in some registry.
	Recursive
)

const (
	typeRecord  = "record"
	typeUnion   = "union"
	typeEnum    = "enum"
	typeArray   = "array"
	typeMap     = "map"
	typeFixed   = "fixed"
	typeString  = "string"
	typeBytes   = "bytes"
	typeInt     = "int"
	typeLong    = "long"
	typeFloat   = "float"
	typeDouble  = "double"
	typeBoolean = "boolean"
	typeNull    = "null"
)

const (
	schemaAliasesField   = "aliases"
	schemaDefaultField   = "default"
	schemaDocField       = "doc"
	schemaFieldsField    = "fields"
	schemaItemsField     = "items"
	schemaNameField      = "name"
	schemaNamespaceField = "namespace"
	schemaSizeField      = "size"
	schemaSymbolsField   = "symbols"
	schemaTypeField      = "type"
	schemaValuesField    = "values"
)

// Schema is an interface representing a single Avro schema (both primitive and complex).
type Schema interface {
	// Returns an integer constant representing this schema type.
	Type() int

	// If this is a record, enum or fixed, returns its name, otherwise the name of the primitive type.
	GetName() string

	// Gets a custom non-reserved property from this schema and a bool representing if it exists.
	Prop(key string) (interface{}, bool)

	// Converts this schema to its JSON representation.
	String() string

	// Converts go runtime datum into a value acceptable by this schema
	Generic(datum interface{}) (interface{}, error)

	// Checks whether the given value is writeable to this schema.
	Validate(v reflect.Value) bool

	// Canonical Schema
	Canonical() (*CanonicalSchema, error)

	// Returns a pre-computed or cached fingerprint
	Fingerprint() (*Fingerprint, error)

	// Returns representation considering whether the same type was already declared
	withRegistry(registry map[string]Schema) Schema
}

// StringSchema implements Schema and represents Avro string type.
type StringSchema struct{}

// Returns representation considering whether the same type was already declared
func (s *StringSchema) withRegistry(registry map[string]Schema) Schema {
	return s
}

// Returns a pre-computed or cached fingerprint
func (*StringSchema) Fingerprint() (*Fingerprint, error) {
	return &Fingerprint{
		233, 229, 193, 201, 228, 246, 39, 115, 57, 209, 188, 222, 7, 51, 165, 155,
		212, 47, 135, 49, 244, 73, 218, 109, 193, 48, 16, 169, 22, 147, 13, 72,
	}, nil
}

// Returns a JSON representation of StringSchema.
func (*StringSchema) String() string {
	return `{"type": "string"}`
}

// Converts go runtime datum into a value acceptable by this schema
func (*StringSchema) Generic(datum interface{}) (interface{}, error) {
	if value, ok := datum.(string); ok {
		return value, nil
	} else if value, ok := datum.(fmt.Stringer); ok {
		return value.String(), nil
	} else {
		return nil, fmt.Errorf("don't know how to convert datum to a string value: %v", datum)
	}
}

// Type returns a type constant for this StringSchema.
func (*StringSchema) Type() int {
	return String
}

// GetName returns a type name for this StringSchema.
func (*StringSchema) GetName() string {
	return typeString
}

// Prop doesn't return anything valuable for StringSchema.
func (*StringSchema) Prop(key string) (interface{}, bool) {
	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (*StringSchema) Validate(v reflect.Value) bool {
	_, ok := dereference(v).Interface().(string)
	return ok
}

// Canonical Schema
func (s *StringSchema) Canonical() (*CanonicalSchema, error) {
	return &CanonicalSchema{Type: "string"}, nil
}

// Standard JSON representation
func (*StringSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"string"`), nil
}

// BytesSchema implements Schema and represents Avro bytes type.
type BytesSchema struct{}

// Returns a pre-computed or cached fingerprint
func (*BytesSchema) Fingerprint() (*Fingerprint, error) {
	return &Fingerprint{
		154, 229, 7, 169, 221, 57, 238, 91, 124, 126, 40, 93, 162, 192, 132, 101,
		33, 200, 174, 141, 128, 254, 234, 229, 80, 78, 12, 152, 29, 83, 245, 250,
	}, nil
}

// Returns representation considering whether the same type was already declared
func (s *BytesSchema) withRegistry(registry map[string]Schema) Schema {
	return s
}

// String returns a JSON representation of BytesSchema.
func (*BytesSchema) String() string {
	return `{"type": "bytes"}`
}

// Converts go runtime datum into a value acceptable by this schema
func (s *BytesSchema) Generic(datum interface{}) (interface{}, error) {
	if value, ok := datum.([]byte); ok {
		return value, nil
	} else if value, ok := datum.(string); ok {
		return []byte(value), nil
	} else if value, ok := datum.(fmt.Stringer); ok {
		return s.Generic(value.String())
	} else {
		return nil, fmt.Errorf("don't know how to convert datum to a bytes value: %v", datum)
	}
}

// Type returns a type constant for this BytesSchema.
func (*BytesSchema) Type() int {
	return Bytes
}

// GetName returns a type name for this BytesSchema.
func (*BytesSchema) GetName() string {
	return typeBytes
}

// Prop doesn't return anything valuable for BytesSchema.
func (*BytesSchema) Prop(key string) (interface{}, bool) {
	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (*BytesSchema) Validate(v reflect.Value) bool {
	v = dereference(v)

	return v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8
}

// Canonical Schema
func (s *BytesSchema) Canonical() (*CanonicalSchema, error) {
	return &CanonicalSchema{Type: "bytes"}, nil
}

// Standard JSON representation
func (*BytesSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"bytes"`), nil
}

// IntSchema implements Schema and represents Avro int type.
type IntSchema struct{}

// Returns representation considering whether the same type was already declared
func (s *IntSchema) withRegistry(registry map[string]Schema) Schema {
	return s
}

// Returns a pre-computed or cached fingerprint
func (*IntSchema) Fingerprint() (*Fingerprint, error) {
	return &Fingerprint{
		63, 43, 135, 169, 254, 124, 201, 177, 56, 53, 89, 140, 57, 129, 205, 69,
		227, 227, 85, 48, 158, 80, 144, 170, 9, 51, 215, 190, 203, 111, 186, 69,
	}, nil
}

// String returns a JSON representation of IntSchema.
func (*IntSchema) String() string {
	return `{"type": "int"}`
}

// Converts go runtime datum into a value acceptable by this schema
func (s *IntSchema) Generic(datum interface{}) (interface{}, error) {
	if value, ok := datum.(int32); ok {
		return int32(value), nil
	} else if value, ok := datum.(int); ok {
		return int32(value), nil
	} else if value, ok := datum.(int16); ok {
		return int32(value), nil
	} else if value, ok := datum.(int8); ok {
		return int32(value), nil
	} else if value, ok := datum.(uint32); ok {
		return int32(value), nil
	} else if value, ok := datum.(uint16); ok {
		return int32(value), nil
	} else if value, ok := datum.(uint8); ok {
		return int32(value), nil
	} else if value, ok := datum.(string); ok {
		if i, err := strconv.Atoi(value); err != nil {
			return nil, err
		} else {
			return int32(i), nil
		}
	} else if value, ok := datum.(fmt.Stringer); ok {
		return s.Generic(value.String())
	} else {
		return nil, fmt.Errorf("don't know how to convert datum to an int value: %v", datum)
	}
}

// Type returns a type constant for this IntSchema.
func (*IntSchema) Type() int {
	return Int
}

// GetName returns a type name for this IntSchema.
func (*IntSchema) GetName() string {
	return typeInt
}

// Prop doesn't return anything valuable for IntSchema.
func (*IntSchema) Prop(key string) (interface{}, bool) {
	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (*IntSchema) Validate(v reflect.Value) bool {
	return reflect.TypeOf(dereference(v).Interface()).Kind() == reflect.Int32
}

// Canonical Schema
func (s *IntSchema) Canonical() (*CanonicalSchema, error) {
	return &CanonicalSchema{Type: "int"}, nil
}

// Standard JSON representation
func (*IntSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"int"`), nil
}

// LongSchema implements Schema and represents Avro long type.
type LongSchema struct{}

// Returns representation considering whether the same type was already declared
func (s *LongSchema) withRegistry(registry map[string]Schema) Schema {
	return s
}

// Returns a pre-computed or cached fingerprint
func (*LongSchema) Fingerprint() (*Fingerprint, error) {
	return &Fingerprint{
		195, 44, 73, 125, 246, 115, 12, 151, 250, 7, 54, 42, 165, 2, 63, 55,
		212, 154, 2, 126, 196, 82, 54, 7, 120, 17, 76, 244, 39, 150, 90, 221,
	}, nil
}

// Returns a JSON representation of LongSchema.
func (*LongSchema) String() string {
	return `{"type": "long"}`
}

// Converts go runtime datum into a value acceptable by this schema
func (s *LongSchema) Generic(datum interface{}) (interface{}, error) {
	if value, ok := datum.(int64); ok {
		return int64(value), nil
	} else if value, ok := datum.(float64); ok {
		return int64(value), nil
	} else if value, ok := datum.(float32); ok {
		return int64(value), nil
	} else if value, ok := datum.(int); ok {
		return int64(value), nil
	} else if value, ok := datum.(int32); ok {
		return int64(value), nil
	} else if value, ok := datum.(int16); ok {
		return int64(value), nil
	} else if value, ok := datum.(int8); ok {
		return int64(value), nil
	} else if value, ok := datum.(uint64); ok {
		return int64(value), nil
	} else if value, ok := datum.(uint32); ok {
		return int64(value), nil
	} else if value, ok := datum.(uint16); ok {
		return int64(value), nil
	} else if value, ok := datum.(uint8); ok {
		return int64(value), nil
	} else if value, ok := datum.(string); ok {
		if i, err := strconv.Atoi(value); err != nil {
			return nil, err
		} else {
			return int64(i), nil
		}
	} else if value, ok := datum.(fmt.Stringer); ok {
		return s.Generic(value.String())
	} else {
		return nil, fmt.Errorf("don't know how to convert datum to a long value: %v from type %v", datum, reflect.TypeOf(datum))
	}
}

// Type returns a type constant for this LongSchema.
func (*LongSchema) Type() int {
	return Long
}

// GetName returns a type name for this LongSchema.
func (*LongSchema) GetName() string {
	return typeLong
}

// Prop doesn't return anything valuable for LongSchema.
func (*LongSchema) Prop(key string) (interface{}, bool) {
	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (*LongSchema) Validate(v reflect.Value) bool {
	return reflect.TypeOf(dereference(v).Interface()).Kind() == reflect.Int64
}

// Canonical Schema
func (s *LongSchema) Canonical() (*CanonicalSchema, error) {
	return &CanonicalSchema{Type: "long"}, nil
}

// Standard JSON representation
func (*LongSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"long"`), nil
}

// FloatSchema implements Schema and represents Avro float type.
type FloatSchema struct{}

// Returns representation considering whether the same type was already declared
func (s *FloatSchema) withRegistry(registry map[string]Schema) Schema {
	return s
}

// Returns a pre-computed or cached fingerprint
func (*FloatSchema) Fingerprint() (*Fingerprint, error) {
	return &Fingerprint{
		30, 113, 249, 236, 5, 29, 102, 63, 86, 176, 216, 225, 252, 132, 215, 26,
		165, 108, 207, 233, 250, 147, 170, 32, 209, 5, 71, 167, 171, 235, 92, 192,
	}, nil
}

// String returns a JSON representation of FloatSchema.
func (*FloatSchema) String() string {
	return `{"type": "float"}`
}

// Converts go runtime datum into a value acceptable by this schema
func (s *FloatSchema) Generic(datum interface{}) (interface{}, error) {
	if value, ok := datum.(float32); ok {
		return float32(value), nil
	} else if value, ok := datum.(int); ok {
		return float32(value), nil
	} else if value, ok := datum.(int32); ok {
		return float32(value), nil
	} else if value, ok := datum.(int16); ok {
		return float32(value), nil
	} else if value, ok := datum.(int8); ok {
		return float32(value), nil
	} else if value, ok := datum.(uint32); ok {
		return float32(value), nil
	} else if value, ok := datum.(uint16); ok {
		return float32(value), nil
	} else if value, ok := datum.(uint8); ok {
		return float32(value), nil
	} else if value, ok := datum.(string); ok {
		return strconv.ParseFloat(value, 32)
	} else if value, ok := datum.(fmt.Stringer); ok {
		return s.Generic(value.String())
	} else {
		return nil, fmt.Errorf("don't know how to convert datum to a float value: %v", datum)
	}
}

// Type returns a type constant for this FloatSchema.
func (*FloatSchema) Type() int {
	return Float
}

// GetName returns a type name for this FloatSchema.
func (*FloatSchema) GetName() string {
	return typeFloat
}

// Prop doesn't return anything valuable for FloatSchema.
func (*FloatSchema) Prop(key string) (interface{}, bool) {
	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (*FloatSchema) Validate(v reflect.Value) bool {
	return reflect.TypeOf(dereference(v).Interface()).Kind() == reflect.Float32
}

// Canonical Schema
func (s *FloatSchema) Canonical() (*CanonicalSchema, error) {
	return &CanonicalSchema{Type: "float"}, nil
}

// Standard JSON representation
func (*FloatSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"float"`), nil
}

// DoubleSchema implements Schema and represents Avro double type.
type DoubleSchema struct{}

// Returns representation considering whether the same type was already declared
func (s *DoubleSchema) withRegistry(registry map[string]Schema) Schema {
	return s
}

// Returns a pre-computed or cached fingerprint
func (*DoubleSchema) Fingerprint() (*Fingerprint, error) {
	return &Fingerprint{
		115, 10, 154, 140, 97, 22, 129, 215, 238, 244, 66, 224, 60, 22, 199, 13,
		19, 188, 163, 235, 139, 151, 123, 180, 3, 234, 255, 82, 23, 106, 242, 84,
	}, nil
}

// Returns a JSON representation of DoubleSchema.
func (*DoubleSchema) String() string {
	return `{"type": "double"}`
}

// Converts go runtime datum into a value acceptable by this schema
func (s *DoubleSchema) Generic(datum interface{}) (interface{}, error) {
	if value, ok := datum.(float64); ok {
		return float64(value), nil
	} else if value, ok := datum.(float32); ok {
		return float64(value), nil
	} else if value, ok := datum.(int64); ok {
		return float64(value), nil
	} else if value, ok := datum.(int); ok {
		return float64(value), nil
	} else if value, ok := datum.(int32); ok {
		return float64(value), nil
	} else if value, ok := datum.(int16); ok {
		return float64(value), nil
	} else if value, ok := datum.(int8); ok {
		return float64(value), nil
	} else if value, ok := datum.(uint64); ok {
		return float64(value), nil
	} else if value, ok := datum.(uint32); ok {
		return float64(value), nil
	} else if value, ok := datum.(uint16); ok {
		return float64(value), nil
	} else if value, ok := datum.(uint8); ok {
		return float64(value), nil
	} else if value, ok := datum.(string); ok {
		return strconv.ParseFloat(value, 64)
	} else if value, ok := datum.(fmt.Stringer); ok {
		return s.Generic(value.String())
	} else {
		return nil, fmt.Errorf("don't know how to convert datum to a double value: %v", datum)
	}
}

// Type returns a type constant for this DoubleSchema.
func (*DoubleSchema) Type() int {
	return Double
}

// GetName returns a type name for this DoubleSchema.
func (*DoubleSchema) GetName() string {
	return typeDouble
}

// Prop doesn't return anything valuable for DoubleSchema.
func (*DoubleSchema) Prop(key string) (interface{}, bool) {
	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (*DoubleSchema) Validate(v reflect.Value) bool {
	return reflect.TypeOf(dereference(v).Interface()).Kind() == reflect.Float64
}

// Canonical Schema
func (s *DoubleSchema) Canonical() (*CanonicalSchema, error) {
	return &CanonicalSchema{Type: "double"}, nil
}

// Standard JSON representation
func (*DoubleSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"double"`), nil
}

// BooleanSchema implements Schema and represents Avro boolean type.
type BooleanSchema struct{}

// Returns representation considering whether the same type was already declared
func (s *BooleanSchema) withRegistry(registry map[string]Schema) Schema {
	return s
}

// Returns a pre-computed or cached fingerprint
func (*BooleanSchema) Fingerprint() (*Fingerprint, error) {
	return &Fingerprint{
		165, 176, 49, 171, 98, 188, 65, 109, 114, 12, 4, 16, 216, 2, 234, 70,
		185, 16, 196, 251, 232, 92, 80, 169, 70, 204, 198, 88, 183, 78, 103, 126,
	}, nil
}

// String returns a JSON representation of BooleanSchema.
func (*BooleanSchema) String() string {
	return `{"type": "boolean"}`
}

// Converts go runtime datum into a value acceptable by this schema
func (s *BooleanSchema) Generic(datum interface{}) (interface{}, error) {
	if value, ok := datum.(bool); ok {
		return value, nil
	} else if value, ok := datum.(int); ok && value > 0 {
		return true, nil
	} else if value, ok := datum.(int); ok && value <= 0 {
		return false, nil
	} else if value, ok := datum.(float32); ok && value > 0 {
		return true, nil
	} else if value, ok := datum.(float32); ok && value <= 0 {
		return false, nil
	} else if value, ok := datum.(float64); ok && value > 0 {
		return true, nil
	} else if value, ok := datum.(float64); ok && value <= 0 {
		return false, nil
	} else if value, ok := datum.(string); ok {
		return strconv.ParseBool(value)
	} else if value, ok := datum.(fmt.Stringer); ok {
		return s.Generic(value.String())
	} else {
		return nil, fmt.Errorf("don't know how to convert datum to a bool value: %v", datum)
	}
}

// Type returns a type constant for this BooleanSchema.
func (*BooleanSchema) Type() int {
	return Boolean
}

// GetName returns a type name for this BooleanSchema.
func (*BooleanSchema) GetName() string {
	return typeBoolean
}

// Prop doesn't return anything valuable for BooleanSchema.
func (*BooleanSchema) Prop(key string) (interface{}, bool) {
	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (*BooleanSchema) Validate(v reflect.Value) bool {
	return reflect.TypeOf(dereference(v).Interface()).Kind() == reflect.Bool
}

// Canonical Schema
func (s *BooleanSchema) Canonical() (*CanonicalSchema, error) {
	return &CanonicalSchema{Type: "boolean"}, nil
}

// Standard JSON representation
func (*BooleanSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"boolean"`), nil
}

// NullSchema implements Schema and represents Avro null type.
type NullSchema struct{}

// Returns representation considering whether the same type was already declared
func (s *NullSchema) withRegistry(registry map[string]Schema) Schema {
	return s
}

// Returns a pre-computed or cached fingerprint
func (*NullSchema) Fingerprint() (*Fingerprint, error) {
	return &Fingerprint{
		240, 114, 203, 236, 59, 248, 132, 24, 113, 212, 40, 66, 48, 197, 233, 131,
		220, 33, 26, 86, 131, 122, 237, 134, 36, 135, 20, 143, 148, 125, 26, 31,
	}, nil
}

// String returns a JSON representation of NullSchema.
func (*NullSchema) String() string {
	return `{"type": "null"}`
}

// Converts go runtime datum into a value acceptable by this schema
func (s *NullSchema) Generic(datum interface{}) (interface{}, error) {
	if datum == nil {
		return nil, nil
	} else {
		return nil, fmt.Errorf("don't know how to convert datum to a null value: %v", datum)
	}
}

// Type returns a type constant for this NullSchema.
func (*NullSchema) Type() int {
	return Null
}

// GetName returns a type name for this NullSchema.
func (*NullSchema) GetName() string {
	return typeNull
}

// Prop doesn't return anything valuable for NullSchema.
func (*NullSchema) Prop(key string) (interface{}, bool) {
	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (*NullSchema) Validate(v reflect.Value) bool {
	// Check if the value is something that can be null
	switch v.Kind() {
	case reflect.Interface:
		return v.IsNil()
	case reflect.Array:
		return v.Cap() == 0
	case reflect.Slice:
		return v.IsNil() || v.Cap() == 0
	case reflect.Map:
		return len(v.MapKeys()) == 0
	case reflect.String:
		return len(v.String()) == 0
	case reflect.Float32:
		// Should NaN floats be treated as null?
		return math.IsNaN(v.Float())
	case reflect.Float64:
		// Should NaN floats be treated as null?
		return math.IsNaN(v.Float())
	case reflect.Ptr:
		return v.IsNil()
	case reflect.Invalid:
		return true
	}

	// Nothing else in particular, so this should not validate?
	return false
}

// Canonical Schema
func (s *NullSchema) Canonical() (*CanonicalSchema, error) {
	return &CanonicalSchema{Type: "null"}, nil
}

// Standard JSON representation
func (*NullSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"null"`), nil
}

// RecordSchema implements Schema and represents Avro record type.
type RecordSchema struct {
	Name        string   `json:"name,omitempty"`
	Namespace   string   `json:"namespace,omitempty"`
	Doc         string   `json:"doc,omitempty"`
	Aliases     []string `json:"aliases,omitempty"`
	Properties  map[string]interface{}
	Fields      []*SchemaField `json:"fields"`
	fingerprint *Fingerprint
}

// Returns representation considering whether the same type was already declared
func (s *RecordSchema) withRegistry(registry map[string]Schema) Schema {
	fullname := GetFullName(s)
	if schema, ok := registry[fullname]; ok {
		return &refSchema{Type_: fullname, Ref: schema}
	} else {
		//turn all repeated type declaration into references
		fields := make([]*SchemaField, len(s.Fields))
		for i, f := range s.Fields {
			fields[i] = f.withRegistry(registry)
		}
		schema = &RecordSchema{
			Name:        s.Name,
			Namespace:   s.Namespace,
			Doc:         s.Doc,
			Aliases:     s.Aliases,
			Properties:  s.Properties,
			Fields:      fields,
			fingerprint: s.fingerprint,
		}
		registry[fullname] = schema
		return schema
	}
}

// Returns a pre-computed or cached fingerprint
func (s *RecordSchema) Fingerprint() (*Fingerprint, error) {
	if s.fingerprint == nil {
		if f, err := calculateSchemaFingerprint(s); err != nil {
			return nil, err
		} else {
			s.fingerprint = f
		}
	}
	return s.fingerprint, nil
}

// String returns a JSON representation of RecordSchema.
func (s *RecordSchema) String() string {
	bytes, err := json.MarshalIndent(s, "", "    ")
	if err != nil {
		panic(err)
	}

	return string(bytes)
}

type FieldPair struct {
	Index int
	Name  string
}

type FieldPairs []FieldPair

func (p FieldPairs) Len() int {
	return len(p)
}
func (p FieldPairs) Less(a, b int) bool {
	return p[a].Name < p[b].Name
}
func (p FieldPairs) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

// Canonical Schema
func (s *RecordSchema) Canonical() (*CanonicalSchema, error) {
	fields := make([]*CanonicalSchemaField, len(s.Fields))
	pairs := make(FieldPairs, len(s.Fields))
	for i, f := range s.Fields {
		pairs[i] = FieldPair{i, f.Name}
	}
	sort.Sort(pairs)

	i := 0
	for _, pair := range pairs {
		if fc, err := s.Fields[pair.Index].Type.Canonical(); err != nil {
			return nil, err
		} else {
			fields[i] = &CanonicalSchemaField{
				Name: pair.Name,
				Type: fc,
			}
			i += 1
		}
	}
	return &CanonicalSchema{Type: "record", Name: GetFullName(s), Fields: fields}, nil
}

// MarshalJSON serializes the given schema as JSON.
func (s *RecordSchema) MarshalJSON() ([]byte, error) {
	return s.MarshalJSONWithRegistry(make(map[string]Schema))
}

func (s *RecordSchema) MarshalJSONWithRegistry(registry map[string]Schema) ([]byte, error) {
	//turn all repeated type declaration into references
	fields := make([]*SchemaField, len(s.Fields))
	for i, f := range s.Fields {
		fields[i] = f.withRegistry(registry)
	}
	return json.Marshal(struct {
		Type      string         `json:"type,omitempty"`
		Namespace string         `json:"namespace,omitempty"`
		Name      string         `json:"name,omitempty"`
		Doc       string         `json:"doc,omitempty"`
		Aliases   []string       `json:"aliases,omitempty"`
		Fields    []*SchemaField `json:"fields"`
	}{
		Type:      "record",
		Namespace: s.Namespace,
		Name:      s.Name,
		Doc:       s.Doc,
		Aliases:   s.Aliases,
		Fields:    fields,
	})
}

// Type returns a type constant for this RecordSchema.
func (*RecordSchema) Type() int {
	return Record
}

// GetName returns a record name for this RecordSchema.
func (s *RecordSchema) GetName() string {
	return s.Name
}

// Converts go runtime datum into a value acceptable by this schema
func (s *RecordSchema) Generic(datum interface{}) (interface{}, error) {

	if rec, ok := datum.(*GenericRecord); ok {
		return rec, nil
	} else if rec, ok := datum.(GenericRecord); ok {
		return &rec, nil
	} else {
		dict := make(map[string]interface{})
		for _, field := range s.Fields {
			if stringMap, ok := datum.(map[string]interface{}); ok {
				if v, ok := stringMap[field.Name]; ok {
					if val, err := field.Type.Generic(v); err != nil {
						return nil, err
					} else {
						dict[field.Name] = val
					}
					continue
				}
			} else if genericMap, ok := datum.(map[interface{}]interface{}); ok {
				if v, ok := genericMap[field.Name]; ok {
					if val, err := field.Type.Generic(v); err != nil {
						return nil, err
					} else {
						dict[field.Name] = val
					}
					continue
				}
			} else {
				return nil, fmt.Errorf("don't know how to convert datum to a generic record: %v", datum)
			}
			if val, err := field.Type.Generic(field.Default); err == nil {
				dict[field.Name] = val
			} else {
				return nil, fmt.Errorf("field doesn't have a valid default and is missing: %v", field.Name)
			}
		}
		rec := NewGenericRecord(s)
		rec.fields = dict
		return rec, nil
	}

	if datum == nil {
		return nil, nil
	} else {
		return nil, fmt.Errorf("don't know how to convert datum to a generic record: %v", datum)
	}
}

// Prop gets a custom non-reserved property from this schema and a bool representing if it exists.
func (s *RecordSchema) Prop(key string) (interface{}, bool) {
	if s.Properties != nil {
		if prop, ok := s.Properties[key]; ok {
			return prop, true
		}
	}

	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (s *RecordSchema) Validate(v reflect.Value) bool {
	v = dereference(v)
	if v.Kind() != reflect.Struct || !v.CanAddr() || !v.CanInterface() {
		return false
	}
	rec, ok := v.Interface().(GenericRecord)
	if !ok {
		// This is not a generic record and is likely a specific record. Hence
		// use the basic check.
		return v.Kind() == reflect.Struct
	}

	fieldCount := 0
	for key, val := range rec.fields {
		for idx := range s.Fields {
			// key.Name must have rs.Fields[idx].Name as a suffix
			if len(s.Fields[idx].Name) <= len(key) {
				lhs := key[len(key)-len(s.Fields[idx].Name):]
				if lhs == s.Fields[idx].Name {
					if !s.Fields[idx].Type.Validate(reflect.ValueOf(val)) {
						return false
					}
					fieldCount++
					break
				}
			}
		}
	}

	// All of the fields set must be accounted for in the union.
	if fieldCount < len(rec.fields) {
		return false
	}

	return true
}

// RecursiveSchema implements Schema and represents Avro record type without a definition (e.g. that should be looked up).
type RecursiveSchema struct {
	Actual *RecordSchema
}

// Returns representation considering whether the same type was already declared
func (s *RecursiveSchema) withRegistry(registry map[string]Schema) Schema {
	return s.Actual.withRegistry(registry)
}

// Returns a pre-computed or cached fingerprint
func (s *RecursiveSchema) Fingerprint() (*Fingerprint, error) {
	return s.Actual.Fingerprint()
}

func newRecursiveSchema(parent *RecordSchema) *RecursiveSchema {
	return &RecursiveSchema{
		Actual: parent,
	}
}

// String returns a JSON representation of RecursiveSchema.
func (s *RecursiveSchema) String() string {
	return fmt.Sprintf(`{"type": "%s"}`, s.Actual.GetName())
}

// Converts go runtime datum into a value acceptable by this schema
func (s *RecursiveSchema) Generic(datum interface{}) (interface{}, error) {
	return s.Actual.Generic(datum)
}

// Type returns a type constant for this RecursiveSchema.
func (*RecursiveSchema) Type() int {
	return Recursive
}

// GetName returns a record name for enclosed RecordSchema.
func (s *RecursiveSchema) GetName() string {
	return s.Actual.GetName()
}

// Prop doesn't return anything valuable for RecursiveSchema.
func (*RecursiveSchema) Prop(key string) (interface{}, bool) {
	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (s *RecursiveSchema) Validate(v reflect.Value) bool {
	return s.Actual.Validate(v)
}

// Canonical JSON representation
func (s *RecursiveSchema) Canonical() (*CanonicalSchema, error) {
	return s.Actual.Canonical()
}

// MarshalJSON serializes the given schema as JSON. Never returns an error.
func (s *RecursiveSchema) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, s.Actual.GetName())), nil
}

// SchemaField represents a schema field for Avro record.
type SchemaField struct {
	Name       string      `json:"name,omitempty"`
	Doc        string      `json:"doc,omitempty"`
	Default    interface{} `json:"default"`
	Type       Schema      `json:"type,omitempty"`
	Aliases    []string    `json:"aliases,omitempty"`
	Properties map[string]interface{}
}

// Returns representation considering whether the same type was already declared
func (s *SchemaField) withRegistry(registry map[string]Schema) *SchemaField {
	return &SchemaField{
		Name:       s.Name,
		Aliases:    s.Aliases,
		Default:    s.Default,
		Doc:        s.Doc,
		Properties: s.Properties,
		Type:       s.Type.withRegistry(registry),
	}
}

// Gets a custom non-reserved property from this schemafield and a bool representing if it exists.
func (s *SchemaField) Prop(key string) (interface{}, bool) {
	if s.Properties != nil {
		if prop, ok := s.Properties[key]; ok {
			return prop, true
		}
	}
	return nil, false
}

// MarshalJSON serializes the given schema field as JSON.
func (s *SchemaField) MarshalJSON() ([]byte, error) {
	if s.Type.Type() == Null || (s.Type.Type() == Union && s.Type.(*UnionSchema).Types[0].Type() == Null) {
		return json.Marshal(struct {
			Name    string      `json:"name,omitempty"`
			Doc     string      `json:"doc,omitempty"`
			Default interface{} `json:"default"`
			Type    Schema      `json:"type,omitempty"`
			Aliases []string    `json:"aliases,omitempty"`
		}{
			Name:    s.Name,
			Doc:     s.Doc,
			Default: s.Default,
			Type:    s.Type,
			Aliases: s.Aliases,
		})
	}

	return json.Marshal(struct {
		Name    string      `json:"name,omitempty"`
		Doc     string      `json:"doc,omitempty"`
		Default interface{} `json:"default,omitempty"`
		Type    Schema      `json:"type,omitempty"`
		Aliases []string    `json:"aliases,omitempty"`
	}{
		Name:    s.Name,
		Doc:     s.Doc,
		Default: s.Default,
		Type:    s.Type,
		Aliases: s.Aliases,
	})
}

// String returns a JSON representation of SchemaField.
func (s *SchemaField) String() string {
	return fmt.Sprintf("[SchemaField: Name: %s, Doc: %s, Default: %v, Type: %s]", s.Name, s.Doc, s.Default, s.Type)
}

// EnumSchema implements Schema and represents Avro enum type.
type EnumSchema struct {
	Name           string
	Namespace      string
	Aliases        []string
	Doc            string
	Symbols        []string
	Properties     map[string]interface{}
	symbolsToIndex map[string]int32
	fingerprint    *Fingerprint
}

// Returns representation considering whether the same type was already declared
func (s *EnumSchema) withRegistry(registry map[string]Schema) Schema {
	fullname := GetFullName(s)
	if schema, ok := registry[fullname]; ok {
		return &refSchema{Type_: fullname, Ref: schema}
	} else {
		registry[fullname] = s
		return s
	}
}

// Returns a pre-computed or cached fingerprint
func (s *EnumSchema) Fingerprint() (*Fingerprint, error) {
	if s.fingerprint == nil {
		if f, err := calculateSchemaFingerprint(s); err != nil {
			return nil, err
		} else {
			s.fingerprint = f
		}
	}
	return s.fingerprint, nil
}

// String returns a JSON representation of EnumSchema.
func (s *EnumSchema) String() string {
	bytes, err := json.MarshalIndent(s, "", "    ")
	if err != nil {
		panic(err)
	}

	return string(bytes)
}

// Converts go runtime datum into a value acceptable by this schema
func (s *EnumSchema) Generic(datum interface{}) (interface{}, error) {
	contains := func(symbol string) bool {
		for _, _symbol := range s.Symbols {
			if _symbol == symbol {
				return true
			}
		}
		return false
	}
	if e, ok := datum.(*EnumValue); ok {
		return *e, nil
	} else if e, ok := datum.(EnumValue); ok {
		return e, nil
	} else if symbol, ok := datum.(string); ok && contains(symbol) {
		if e, err := NewEnumValue(symbol, s); err != nil {
			return nil, err
		} else {
			return *e, nil
		}
	} else if index, ok := datum.(int32); ok && index >= 0 && int(index) < len(s.Symbols) {
		if e, err := NewEnumValue(s.Symbols[index], s); err != nil {
			return nil, err
		} else {
			return *e, nil
		}
	} else if index, ok := datum.(int); ok && index >= 0 && index < len(s.Symbols) {
		if e, err := NewEnumValue(s.Symbols[int32(index)], s); err != nil {
			return nil, err
		} else {
			return *e, nil
		}
	} else {
		return nil, fmt.Errorf("don't know how to convert datum to an enum value: %v", datum)
	}
}

var symbolsToIndexCache = make(map[[32]byte]map[string]int32)
var symbolsToIndexCacheLock sync.Mutex

// Type returns a type constant for this EnumSchema.
func (s *EnumSchema) IndexOf(symbol string) int32 {
	if s.symbolsToIndex == nil {
		f, _ := s.Fingerprint()
		symbolsToIndexCacheLock.Lock()
		if s.symbolsToIndex = symbolsToIndexCache[*f]; s.symbolsToIndex == nil {
			s.symbolsToIndex = make(map[string]int32)
			for i, symbol := range s.Symbols {
				s.symbolsToIndex[symbol] = int32(i)
			}
			symbolsToIndexCache[*f] = s.symbolsToIndex
		}
		symbolsToIndexCacheLock.Unlock()
	}
	if index, ok := s.symbolsToIndex[symbol]; ok {
		return index
	} else {
		return -1
	}
}

// Type returns a type constant for this EnumSchema.
func (*EnumSchema) Type() int {
	return Enum
}

// GetName returns an enum name for this EnumSchema.
func (s *EnumSchema) GetName() string {
	return s.Name
}

// Prop gets a custom non-reserved property from this schema and a bool representing if it exists.
func (s *EnumSchema) Prop(key string) (interface{}, bool) {
	if s.Properties != nil {
		if prop, ok := s.Properties[key]; ok {
			return prop, true
		}
	}

	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (s *EnumSchema) Validate(v reflect.Value) bool {
	if _, ok := dereference(v).Interface().(EnumValue); ok {
		return true
	} else {
		return false
	}
}

// Canonical representation
func (s *EnumSchema) Canonical() (*CanonicalSchema, error) {
	return &CanonicalSchema{Name: GetFullName(s), Type: "enum", Symbols: s.Symbols}, nil
}

// MarshalJSON serializes the given schema as JSON.
func (s *EnumSchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type      string   `json:"type,omitempty"`
		Namespace string   `json:"namespace,omitempty"`
		Name      string   `json:"name,omitempty"`
		Doc       string   `json:"doc,omitempty"`
		Symbols   []string `json:"symbols,omitempty"`
	}{
		Type:      "enum",
		Namespace: s.Namespace,
		Name:      s.Name,
		Doc:       s.Doc,
		Symbols:   s.Symbols,
	})
}

func (s *EnumSchema) Value(symbol string) (EnumValue, error) {
	if enum, err := NewEnumValue(symbol, s); err != nil {
		return EnumValue{}, err
	} else {
		return *enum, nil
	}
}

// ArraySchema implements Schema and represents Avro array type.
type ArraySchema struct {
	Items       Schema
	Properties  map[string]interface{}
	fingerprint *Fingerprint
}

// Returns representation considering whether the same type was already declared
func (s *ArraySchema) withRegistry(registry map[string]Schema) Schema {
	return &ArraySchema{
		Items:       s.Items.withRegistry(registry),
		Properties:  s.Properties,
		fingerprint: s.fingerprint,
	}
}

// Returns a pre-computed or cached fingerprint
func (s *ArraySchema) Fingerprint() (*Fingerprint, error) {
	if s.fingerprint == nil {
		if f, err := calculateSchemaFingerprint(s); err != nil {
			return nil, err
		} else {
			s.fingerprint = f
		}
	}
	return s.fingerprint, nil
}

// String returns a JSON representation of ArraySchema.
func (s *ArraySchema) String() string {
	bytes, err := json.MarshalIndent(s, "", "    ")
	if err != nil {
		panic(err)
	}

	return string(bytes)
}

// Converts go runtime datum into a value acceptable by this schema
func (s *ArraySchema) Generic(datum interface{}) (interface{}, error) {
	if a, ok := datum.([]interface{}); ok {
		if s.Items.Type() == String {
			slice := make([]string, len(a))
			for i, v := range a {
				if val, err := s.Items.Generic(v); err != nil {
					return nil, err
				} else {
					slice[i] = val.(string)
				}
			}
			return slice, nil
		} else if s.Items.Type() == Double {
			slice := make([]float64, len(a))
			for i, v := range a {
				if val, err := s.Items.Generic(v); err != nil {
					return nil, err
				} else {
					slice[i] = val.(float64)
				}
			}
			return slice, nil
		} else if s.Items.Type() == Float {
			slice := make([]float32, len(a))
			for i, v := range a {
				if val, err := s.Items.Generic(v); err != nil {
					return nil, err
				} else {
					slice[i] = val.(float32)
				}
			}
			return slice, nil
		} else if s.Items.Type() == Long {
			slice := make([]int64, len(a))
			for i, v := range a {
				if val, err := s.Items.Generic(v); err != nil {
					return nil, err
				} else {
					slice[i] = val.(int64)
				}
			}
			return slice, nil
		} else if s.Items.Type() == Int {
			slice := make([]int32, len(a))
			for i, v := range a {
				if val, err := s.Items.Generic(v); err != nil {
					return nil, err
				} else {
					slice[i] = val.(int32)
				}
			}
			return slice, nil
		} else if s.Items.Type() == Boolean {
			slice := make([]bool, len(a))
			for i, v := range a {
				if val, err := s.Items.Generic(v); err != nil {
					return nil, err
				} else {
					slice[i] = val.(bool)
				}
			}
			return slice, nil
		} else if s.Items.Type() == Bytes || s.Items.Type() == Fixed {
			slice := make([][]byte, len(a))
			for i, v := range a {
				if val, err := s.Items.Generic(v); err != nil {
					return nil, err
				} else {
					slice[i] = val.([]byte)
				}
			}
			return slice, nil
		} else {
			slice := make([]interface{}, len(a))
			for i, v := range a {
				if val, err := s.Items.Generic(v); err != nil {
					return nil, err
				} else {
					slice[i] = val
				}
			}
			return slice, nil
		}

	} else {
		return nil, fmt.Errorf("don't know how to convert datum to an array value: %v", datum)
	}
}

// Type returns a type constant for this ArraySchema.
func (*ArraySchema) Type() int {
	return Array
}

// GetName returns a type name for this ArraySchema.
func (*ArraySchema) GetName() string {
	return typeArray
}

// Prop gets a custom non-reserved property from this schema and a bool representing if it exists.
func (s *ArraySchema) Prop(key string) (interface{}, bool) {
	if s.Properties != nil {
		if prop, ok := s.Properties[key]; ok {
			return prop, true
		}
	}

	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (s *ArraySchema) Validate(v reflect.Value) bool {
	v = dereference(v)

	// This needs to be a slice
	return v.Kind() == reflect.Slice || v.Kind() == reflect.Array
}

// Canonical representation
func (s *ArraySchema) Canonical() (*CanonicalSchema, error) {
	if ic, err := s.Items.Canonical(); err != nil {
		return nil, err
	} else {
		return &CanonicalSchema{Type: "array", Items: ic}, nil
	}
}

// MarshalJSON serializes the given schema as JSON.
func (s *ArraySchema) MarshalJSON() ([]byte, error) {
	return s.MarshalJSONWithRegistry(make(map[string]Schema))
}

func (s *ArraySchema) MarshalJSONWithRegistry(registry map[string]Schema) ([]byte, error) {
	return json.Marshal(struct {
		Type  string `json:"type,omitempty"`
		Items Schema `json:"items,omitempty"`
	}{
		Type:  "array",
		Items: s.Items,
	})
}

// MapSchema implements Schema and represents Avro map type.
type MapSchema struct {
	Values      Schema
	Properties  map[string]interface{}
	fingerprint *Fingerprint
}

// Returns representation considering whether the same type was already declared
func (s *MapSchema) withRegistry(registry map[string]Schema) Schema {
	return &MapSchema{
		Values:      s.Values.withRegistry(registry),
		Properties:  s.Properties,
		fingerprint: s.fingerprint,
	}
}

// Returns a pre-computed or cached fingerprint
func (s *MapSchema) Fingerprint() (*Fingerprint, error) {
	if s.fingerprint == nil {
		if f, err := calculateSchemaFingerprint(s); err != nil {
			return nil, err
		} else {
			s.fingerprint = f
		}
	}
	return s.fingerprint, nil
}

// String returns a JSON representation of MapSchema.
func (s *MapSchema) String() string {
	bytes, err := json.MarshalIndent(s, "", "    ")
	if err != nil {
		panic(err)
	}

	return string(bytes)
}

// Converts go runtime datum into a value acceptable by this schema
func (s *MapSchema) Generic(datum interface{}) (interface{}, error) {
	dict := make(map[string]interface{})
	if stringMap, ok := datum.(map[string]interface{}); ok {
		for field, v := range stringMap {
			if val, err := s.Values.Generic(v); err != nil {
				return nil, err
			} else {
				dict[field] = val
			}
		}
	} else if genericMap, ok := datum.(map[interface{}]interface{}); ok {
		for k, v := range genericMap {
			field := ""
			if f, ok := k.(string); ok {
				field = f
			} else if f, ok := k.(fmt.Stringer); ok {
				field = f.String()
			}
			if field != "" {
				if val, err := s.Values.Generic(v); err != nil {
					return nil, err
				} else {
					dict[field] = val
				}
			}
		}
	} else {
		return nil, fmt.Errorf("don't know how to convert datum to a map value: %v", datum)
	}
	return dict, nil
}

// Type returns a type constant for this MapSchema.
func (*MapSchema) Type() int {
	return Map
}

// GetName returns a type name for this MapSchema.
func (*MapSchema) GetName() string {
	return typeMap
}

// Prop gets a custom non-reserved property from this schema and a bool representing if it exists.
func (s *MapSchema) Prop(key string) (interface{}, bool) {
	if s.Properties != nil {
		if prop, ok := s.Properties[key]; ok {
			return prop, true
		}
	}
	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (s *MapSchema) Validate(v reflect.Value) bool {
	v = dereference(v)

	return v.Kind() == reflect.Map && v.Type().Key().Kind() == reflect.String
}

// Canonical representation
func (s *MapSchema) Canonical() (*CanonicalSchema, error) {
	if vc, err := s.Values.Canonical(); err != nil {
		return nil, err
	} else {
		return &CanonicalSchema{Type: "array", Values: vc}, nil
	}
}

// MarshalJSON serializes the given schema as JSON.
func (s *MapSchema) MarshalJSON() ([]byte, error) {
	return s.MarshalJSONWithRegistry(make(map[string]Schema))
}

func (s *MapSchema) MarshalJSONWithRegistry(registry map[string]Schema) ([]byte, error) {
	return json.Marshal(struct {
		Type   string `json:"type,omitempty"`
		Values Schema `json:"values,omitempty"`
	}{
		Type:   "map",
		Values: s.Values.withRegistry(registry),
	})
}

// UnionSchema implements Schema and represents Avro union type.
type UnionSchema struct {
	Types       []Schema
	fingerprint *Fingerprint
}

// Returns representation considering whether the same type was already declared
func (s *UnionSchema) withRegistry(registry map[string]Schema) Schema {
	types := make([]Schema, len(s.Types))
	for i, t := range s.Types {
		types[i] = t.withRegistry(registry)
	}
	return &UnionSchema{
		Types:       types,
		fingerprint: s.fingerprint,
	}
}

// Returns a pre-computed or cached fingerprint
func (s *UnionSchema) Fingerprint() (*Fingerprint, error) {
	if s.fingerprint == nil {
		if f, err := calculateSchemaFingerprint(s); err != nil {
			return nil, err
		} else {
			s.fingerprint = f
		}
	}
	return s.fingerprint, nil
}

// String returns a JSON representation of UnionSchema.
func (s *UnionSchema) String() string {
	bytes, err := json.MarshalIndent(s, "", "    ")
	if err != nil {
		panic(err)
	}

	return fmt.Sprintf(`{"type": %s}`, string(bytes))
}

// Converts go runtime datum into a value acceptable by this schema
func (s *UnionSchema) Generic(datum interface{}) (interface{}, error) {
	for _, u := range s.Types {
		if val, err := u.Generic(datum); err == nil {
			return val, nil
		}
	}
	return nil, fmt.Errorf("don't know how to convert datum to an union value: %v", datum)
}

// Type returns a type constant for this UnionSchema.
func (*UnionSchema) Type() int {
	return Union
}

// GetName returns a type name for this UnionSchema.
func (*UnionSchema) GetName() string {
	return typeUnion
}

// Prop doesn't return anything valuable for UnionSchema.
func (*UnionSchema) Prop(key string) (interface{}, bool) {
	return nil, false
}

// GetType gets the index of actual union type for a given value.
func (s *UnionSchema) GetType(v reflect.Value) int {
	if s.Types != nil {
		for i := range s.Types {
			if t := s.Types[i]; t.Validate(v) {
				return i
			}
		}
	}

	return -1
}

// Validate checks whether the given value is writeable to this schema.
func (s *UnionSchema) Validate(v reflect.Value) bool {
	v = dereference(v)
	for i := range s.Types {
		if t := s.Types[i]; t.Validate(v) {
			return true
		}
	}

	return false
}

// Canonical representation
func (s *UnionSchema) Canonical() (*CanonicalSchema, error) {
	ct := make([]*CanonicalSchema, len(s.Types))
	for i, t := range s.Types {
		if c, err := t.Canonical(); err != nil {
			return nil, err
		} else {
			ct[i] = c
		}
	}
	return &CanonicalSchema{Type: "union", Types: ct}, nil
}

// MarshalJSON serializes the given schema as JSON.
func (s *UnionSchema) MarshalJSON() ([]byte, error) {
	return s.MarshalJSONWithRegistry(make(map[string]Schema))
}

func (s *UnionSchema) MarshalJSONWithRegistry(registry map[string]Schema) ([]byte, error) {
	return json.Marshal(s.Types)
}

// FixedSchema implements Schema and represents Avro fixed type.
type FixedSchema struct {
	Namespace   string                 `json:"namespace"`
	Name        string                 `json:"name"`
	Size        int                    `json:"size"`
	Properties  map[string]interface{} `json:"properties,omitempty"`
	fingerprint *Fingerprint
}

// Returns representation considering whether the same type was already declared
func (s *FixedSchema) withRegistry(registry map[string]Schema) Schema {
	fullname := GetFullName(s)
	if schema, ok := registry[fullname]; ok {
		return &refSchema{Type_: fullname, Ref: schema}
	} else {
		registry[fullname] = s
		return s
	}
}

// Returns a pre-computed or cached fingerprint
func (s *FixedSchema) Fingerprint() (*Fingerprint, error) {
	if s.fingerprint == nil {
		if f, err := calculateSchemaFingerprint(s); err != nil {
			return nil, err
		} else {
			s.fingerprint = f
		}
	}
	return s.fingerprint, nil
}

// String returns a JSON representation of FixedSchema.
func (s *FixedSchema) String() string {
	bytes, err := json.MarshalIndent(s, "", "    ")
	if err != nil {
		panic(err)
	}

	return string(bytes)
}

// Converts go runtime datum into a value acceptable by this schema
func (s *FixedSchema) Generic(datum interface{}) (interface{}, error) {
	if slice, ok := datum.([]byte); ok && len(slice) == s.Size {
		return slice, nil
	} else if plain, ok := datum.(string); ok && len(plain) == s.Size {
		return []byte(plain), nil
	} else {
		return nil, fmt.Errorf("don't know how to convert datum to a fixed value: %v", datum)
	}
}

// Type returns a type constant for this FixedSchema.
func (*FixedSchema) Type() int {
	return Fixed
}

// GetName returns a fixed name for this FixedSchema.
func (s *FixedSchema) GetName() string {
	return s.Name
}

// Prop gets a custom non-reserved property from this schema and a bool representing if it exists.
func (s *FixedSchema) Prop(key string) (interface{}, bool) {
	if s.Properties != nil {
		if prop, ok := s.Properties[key]; ok {
			return prop, true
		}
	}
	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (s *FixedSchema) Validate(v reflect.Value) bool {
	v = dereference(v)

	return (v.Kind() == reflect.Array || v.Kind() == reflect.Slice) && v.Type().Elem().Kind() == reflect.Uint8 && v.Len() == s.Size
}

// Canonical representation
func (s *FixedSchema) Canonical() (*CanonicalSchema, error) {
	return &CanonicalSchema{
		Type: "fixed",
		Name: GetFullName(s),
		Size: s.Size,
	}, nil
}

// MarshalJSON serializes the given schema as JSON.
func (s *FixedSchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type       string                 `json:"type,omitempty"`
		Size       int                    `json:"size,omitempty"`
		Name       string                 `json:"name,omitempty"`
		Namespace  string                 `json:"namespace"`
		Properties map[string]interface{} `json:"properties,omitempty"`
	}{
		Type:       "fixed",
		Size:       s.Size,
		Name:       s.Name,
		Namespace:  s.Namespace,
		Properties: s.Properties,
	})
}

// GetFullName returns a fully-qualified name for a schema if possible. The format is namespace.name.
func GetFullName(schema Schema) string {
	switch sch := schema.(type) {
	case *RecordSchema:
		return getFullName(sch.GetName(), sch.Namespace)
	case *EnumSchema:
		return getFullName(sch.GetName(), sch.Namespace)
	case *FixedSchema:
		return getFullName(sch.GetName(), sch.Namespace)
	default:
		return schema.GetName()
	}
}

// ParseSchemaFile parses a given file.
// May return an error if schema is not parsable or file does not exist.
func ParseSchemaFile(file string) (Schema, error) {
	fileContents, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}

	return ParseSchema(string(fileContents))
}

// ParseSchema parses a given schema without provided schemas to reuse.
// Equivalent to call ParseSchemaWithResistry(rawSchema, make(map[string]Schema))
// May return an error if schema is not parsable or has insufficient information about any type.
func ParseSchema(rawSchema string) (Schema, error) {
	return ParseSchemaWithRegistry(rawSchema, make(map[string]Schema))
}

// ParseSchemaWithRegistry parses a given schema using the provided registry for type lookup.
// Registry will be filled up during parsing.
// May return an error if schema is not parsable or has insufficient information about any type.
func ParseSchemaWithRegistry(rawSchema string, schemas map[string]Schema) (Schema, error) {
	var schema interface{}
	if err := json.Unmarshal([]byte(rawSchema), &schema); err != nil {
		schema = rawSchema
	}

	return schemaByType(schema, schemas, "")
}

// MustParseSchema is like ParseSchema, but panics if the given schema cannot be parsed.
func MustParseSchema(rawSchema string) Schema {
	s, err := ParseSchema(rawSchema)
	if err != nil {
		panic(err)
	}
	return s
}

func schemaByType(i interface{}, registry map[string]Schema, namespace string) (Schema, error) {
	switch v := i.(type) {
	case nil:
		return new(NullSchema), nil
	case string:
		switch v {
		case typeNull:
			return new(NullSchema), nil
		case typeBoolean:
			return new(BooleanSchema), nil
		case typeInt:
			return new(IntSchema), nil
		case typeLong:
			return new(LongSchema), nil
		case typeFloat:
			return new(FloatSchema), nil
		case typeDouble:
			return new(DoubleSchema), nil
		case typeBytes:
			return new(BytesSchema), nil
		case typeString:
			return new(StringSchema), nil
		default:
			// If a name reference contains a dot, we consider it a full name reference.
			// Otherwise, use the getFullName helper to look up the name.
			// See https://avro.apache.org/docs/1.7.7/spec.html#Names
			fullName := v
			if !strings.ContainsRune(fullName, '.') {
				fullName = getFullName(v, namespace)
			}
			schema, ok := registry[fullName]
			if !ok {
				return nil, fmt.Errorf("Unknown type name: %s", v)
			}

			return schema, nil
		}
	case map[string][]interface{}:
		return parseUnionSchema(v[schemaTypeField], registry, namespace)
	case map[string]interface{}:
		switch v[schemaTypeField] {
		case typeNull:
			return new(NullSchema), nil
		case typeBoolean:
			return new(BooleanSchema), nil
		case typeInt:
			return new(IntSchema), nil
		case typeLong:
			return new(LongSchema), nil
		case typeFloat:
			return new(FloatSchema), nil
		case typeDouble:
			return new(DoubleSchema), nil
		case typeBytes:
			return new(BytesSchema), nil
		case typeString:
			return new(StringSchema), nil
		case typeArray:
			items, err := schemaByType(v[schemaItemsField], registry, namespace)
			if err != nil {
				return nil, err
			}
			return &ArraySchema{Items: items, Properties: getProperties(v)}, nil
		case typeMap:
			values, err := schemaByType(v[schemaValuesField], registry, namespace)
			if err != nil {
				return nil, err
			}
			return &MapSchema{Values: values, Properties: getProperties(v)}, nil
		case typeEnum:
			return parseEnumSchema(v, registry, namespace)
		case typeFixed:
			return parseFixedSchema(v, registry, namespace)
		case typeRecord:
			return parseRecordSchema(v, registry, namespace)
		default:
			// Type references can also be done as {"type": "otherType"}.
			// Just call back in so we can handle this scenario in the string matcher above.
			return schemaByType(v[schemaTypeField], registry, namespace)
		}
	case []interface{}:
		return parseUnionSchema(v, registry, namespace)
	}

	return nil, ErrInvalidSchema
}

func parseEnumSchema(v map[string]interface{}, registry map[string]Schema, namespace string) (Schema, error) {
	symbols := make([]string, len(v[schemaSymbolsField].([]interface{})))
	for i, symbol := range v[schemaSymbolsField].([]interface{}) {
		symbols[i] = symbol.(string)
	}

	schema := &EnumSchema{Name: v[schemaNameField].(string), Symbols: symbols}
	setOptionalField(&schema.Namespace, v, schemaNamespaceField)
	setOptionalField(&schema.Doc, v, schemaDocField)
	schema.Properties = getProperties(v)

	return addSchema(getFullName(v[schemaNameField].(string), namespace), schema, registry), nil
}

func parseFixedSchema(v map[string]interface{}, registry map[string]Schema, namespace string) (Schema, error) {
	size, ok := v[schemaSizeField].(float64)
	if !ok {
		return nil, ErrInvalidFixedSize
	}

	schema := &FixedSchema{Name: v[schemaNameField].(string), Size: int(size), Properties: getProperties(v)}
	setOptionalField(&schema.Namespace, v, schemaNamespaceField)
	return addSchema(getFullName(v[schemaNameField].(string), namespace), schema, registry), nil
}

func parseUnionSchema(v []interface{}, registry map[string]Schema, namespace string) (Schema, error) {
	types := make([]Schema, len(v))
	var err error
	for i := range v {
		types[i], err = schemaByType(v[i], registry, namespace)
		if err != nil {
			return nil, err
		}
	}
	return &UnionSchema{Types: types}, nil
}

func parseRecordSchema(v map[string]interface{}, registry map[string]Schema, namespace string) (Schema, error) {
	schema := &RecordSchema{Name: v[schemaNameField].(string)}
	setOptionalField(&schema.Namespace, v, schemaNamespaceField)
	setOptionalField(&namespace, v, schemaNamespaceField)
	setOptionalField(&schema.Doc, v, schemaDocField)
	addSchema(getFullName(v[schemaNameField].(string), namespace), newRecursiveSchema(schema), registry)
	fields := make([]*SchemaField, len(v[schemaFieldsField].([]interface{})))
	for i := range fields {
		field, err := parseSchemaField(v[schemaFieldsField].([]interface{})[i], registry, namespace)
		if err != nil {
			return nil, err
		}
		fields[i] = field
	}
	schema.Fields = fields
	schema.Properties = getProperties(v)

	return schema, nil
}

func parseSchemaField(i interface{}, registry map[string]Schema, namespace string) (*SchemaField, error) {
	switch v := i.(type) {
	case map[string]interface{}:
		name, ok := v[schemaNameField].(string)
		if !ok {
			return nil, fmt.Errorf("Schema field name missing")
		}
		schemaField := &SchemaField{Name: name, Properties: getProperties(v)}
		setOptionalField(&schemaField.Doc, v, schemaDocField)
		if aliases, ok := v[schemaAliasesField]; ok {
			for _, a := range aliases.([]interface{}) {
				schemaField.Aliases = append(schemaField.Aliases, a.(string))
			}
		}
		fieldType, err := schemaByType(v[schemaTypeField], registry, namespace)
		if err != nil {
			return nil, err
		}
		schemaField.Type = fieldType
		if def, exists := v[schemaDefaultField]; exists {
			switch def.(type) {
			case float64:
				// JSON treats all numbers as float64 by default
				switch schemaField.Type.Type() {
				case Int:
					var converted = int32(def.(float64))
					schemaField.Default = converted
				case Long:
					var converted = int64(def.(float64))
					schemaField.Default = converted
				case Float:
					var converted = float32(def.(float64))
					schemaField.Default = converted

				default:
					schemaField.Default = def
				}
			default:
				schemaField.Default = def
			}
		}
		return schemaField, nil
	}

	return nil, ErrInvalidSchema
}

func setOptionalField(where *string, v map[string]interface{}, fieldName string) {
	if field, exists := v[fieldName]; exists {
		*where = field.(string)
	}
}

func addSchema(name string, schema Schema, schemas map[string]Schema) Schema {
	if schemas != nil {
		if sch, ok := schemas[name]; ok {
			return sch
		}

		schemas[name] = schema
	}

	return schema
}

func getFullName(name string, namespace string) string {
	if len(namespace) > 0 && !strings.ContainsRune(name, '.') {
		return namespace + "." + name
	}

	return name
}

type refSchema struct {
	Type_ string
	Ref   Schema
}

// Returns representation considering whether the same type was already declared
func (s *refSchema) withRegistry(registry map[string]Schema) Schema {
	return s
}

// MarshalJSON serializes the given schema as JSON.
func (s *refSchema) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", s.Type_)), nil
}

func (s *refSchema) Type() int {
	return s.Ref.Type()
}

func (s *refSchema) GetName() string {
	return s.Ref.GetName()
}

func (s *refSchema) Generic(datum interface{}) (interface{}, error) {
	return s.Ref.Generic(datum)
}

func (s *refSchema) Prop(key string) (interface{}, bool) {
	return s.Ref.Prop(key)
}

func (s *refSchema) Validate(v reflect.Value) (bool) {
	return s.Ref.Validate(v)
}

// Canonical Schema
func (s *refSchema) Canonical() (*CanonicalSchema, error) {
	return s.Canonical()
}

// Returns a pre-computed or cached fingerprint
func (s *refSchema) Fingerprint() (*Fingerprint, error) {
	return s.Fingerprint()
}

func (s *refSchema) String() string {
	return fmt.Sprintf("{%q: %q}", "type", s.Type_)
}

// gets custom string properties from a given schema
func getProperties(v map[string]interface{}) map[string]interface{} {
	props := make(map[string]interface{})
	for name, value := range v {
		if !isReserved(name) {
			props[name] = value
		}
	}
	return props
}

func isReserved(name string) bool {
	switch name {
	case schemaAliasesField, schemaDocField, schemaFieldsField, schemaItemsField, schemaNameField,
		schemaNamespaceField, schemaSizeField, schemaSymbolsField, schemaTypeField, schemaValuesField:
		return true
	}

	return false
}

func dereference(v reflect.Value) reflect.Value {
	if v.Kind() == reflect.Ptr {
		return v.Elem()
	}

	return v
}

func calculateSchemaFingerprint(s Schema) (*Fingerprint, error) {
	if canonical, err := s.Canonical(); err != nil {
		return nil, err
	} else if bytes, err := canonical.MarshalJSON(); err != nil {
		return nil, err
	} else {
		f := Fingerprint(sha256.Sum256(bytes))
		return &f, nil
	}
}

type CanonicalSchema struct {
	Name    string                  `json:"name,omitempty"`
	Type    string                  `json:"type,omitempty"`
	Fields  []*CanonicalSchemaField `json:"fields,omitempty"`
	Symbols []string                `json:"symbols,omitempty"`
	Items   *CanonicalSchema        `json:"items,omitempty"`
	Values  *CanonicalSchema        `json:"values,omitempty"`
	Size    int                     `json:"size,omitempty"`
	Types   []*CanonicalSchema      `json:"size,omit"`
}

type CanonicalSchemaField struct {
	Name string           `json:"name,omitempty"`
	Type *CanonicalSchema `json:"type,omitempty"`
}

func (c *CanonicalSchema) MarshalJSON() ([]byte, error) {
	switch c.Type {
	case "string", "bytes", "int", "long", "float", "double", "boolean", "null":
		return []byte("\"" + c.Type + "\""), nil
	case "union":
		return json.Marshal(c.Types)
	default:
		return json.Marshal(struct {
			Name    string                  `json:"name,omitempty"`
			Type    string                  `json:"type,omitempty"`
			Fields  []*CanonicalSchemaField `json:"fields,omitempty"`
			Symbols []string                `json:"symbols,omitempty"`
			Items   *CanonicalSchema        `json:"items,omitempty"`
			Values  *CanonicalSchema        `json:"values,omitempty"`
			Size    int                     `json:"size,omitempty"`
		}{
			c.Name,
			c.Type,
			c.Fields,
			c.Symbols,
			c.Items,
			c.Values,
			c.Size,
		})
	}

}
