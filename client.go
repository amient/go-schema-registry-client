package registry

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/amient/avro"
	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"reflect"
	"sync"
	"time"
)

const schemaTypeDefault = ""
const schemaTypeAvro = "AVRO"
const schemaTypeProtobuf = "PROTOBUF"

const schemaRegistryRequestTimeout = 30 * time.Second

const magicByte = 0

var cache1lock = sync.Mutex{}

func NewClient(baseUrl string) *Client {
	return NewClientWith(&Config{
		Url:      baseUrl,
		LogLevel: LogNothing,
	})
}

func NewClientWith(config *Config) *Client {
	return &Client{
		config:     config,
		cache1:     make(map[uint32]Schema),
		cacheProto: make(map[protoreflect.MessageType]uint32),
		cacheAvro:  make(map[avro.Fingerprint]uint32),
	}
}

type Client struct {
	config     *Config
	cache1     map[uint32]Schema                   //deserialization cache for schema ids (updated by GetSchema and GetSubjectVersion)
	cacheProto map[protoreflect.MessageType]uint32 //one-off serialization cache for compiled protobuf types
	cacheAvro  map[avro.Fingerprint]uint32         //one-off serialization cache for avro schemas
}

func (c *Client) Serialize(ctx context.Context, subject string, value interface{}) ([]byte, error) {
	var schemaId uint32
	var err error
	var wireBytes []byte
	switch typedValue := value.(type) {
	case avro.AvroRecord:
		writerSchema := typedValue.Schema()
		schemaId, err = c.RegisterAvroType(ctx, subject, writerSchema)
		if err != nil {
			return nil, fmt.Errorf("RegisterAvroType: %v", err)
		}
		writer := avro.NewGenericDatumWriter().SetSchema(writerSchema)
		buf := new(bytes.Buffer)
		if err := writer.Write(typedValue, avro.NewBinaryEncoder(buf)); err != nil {
			return nil, err
		}
		wireBytes = buf.Bytes()
	case proto.Message:
		protoType := proto.MessageReflect(typedValue)
		schemaId, err = c.RegisterProtobufType(ctx, subject, protoType)
		if err != nil {
			return nil, err
		}
		wireBytes, err = proto.Marshal(typedValue)
	default:
		err = fmt.Errorf("cannot serialize type: %v", reflect.TypeOf(typedValue))
	}
	if err != nil {
		return nil, err
	}
	if schemaId == 0 {
		panic("no schema registered for type " + reflect.TypeOf(value).String())
	}
	buf := new(bytes.Buffer)
	err = buf.WriteByte(magicByte) // write magic byte
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.BigEndian, schemaId) //write schema id
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(wireBytes) //write payload
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *Client) Deserialize(ctx context.Context, data []byte) (interface{}, error) {
	if data[0] != magicByte {
		return nil, fmt.Errorf("invalid magic byte expecting %v got: %v", magicByte, data[0])
	}
	schemaId := binary.BigEndian.Uint32(data[1:])
	if schemaId < 1 {
		return nil, fmt.Errorf("invalid schema id in the serialized value: %v", schemaId)
	}
	schema, err := c.GetSchema(ctx, schemaId)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema id %d: %v", schemaId, err)
	}

	if protoSchema, ok := schema.(*ProtobufSchema); ok {
		msg, err := c.deserializeProtobuf(ctx, protoSchema, data)
		if err != nil {
			err = fmt.Errorf("deserializeProtobuf: %v", err)
		}
		return msg, err
	}
	if avroSchema, ok := schema.(*AvroSchema); ok {
		msg, err := c.deserializeAvro(ctx, avroSchema, data)
		if err != nil {
			err = fmt.Errorf("deserializeAvro: %v", err)
		}
		return msg, err
	}
	return nil, fmt.Errorf("unsupported schema type: %v", reflect.TypeOf(schema))

}

func (c *Client) DeserializeInto(ctx context.Context, data []byte, into interface{}) error {
	if data[0] != magicByte {
		return fmt.Errorf("invalid magic byte expecting %v got: %v", magicByte, data[0])
	}
	lookup := func() (Schema, error) {
		schemaId := binary.BigEndian.Uint32(data[1:])
		if schemaId < 1 {
			return nil, fmt.Errorf("invalid schema id in the serialized value: %v", schemaId)
		}
		schema, err := c.GetSchema(ctx, schemaId)
		if err != nil {
			return nil, fmt.Errorf("failed to get schema id %d: %v", schemaId, err)
		}
		return schema, nil
	}
	switch typedValue := into.(type) {
	case proto.Message:
		return c.deserializeProtobufInto(ctx, data[5:], typedValue)
	case avro.AvroRecord:
		schema, err := lookup()
		if err != nil {
			return err
		}
		return c.deserializeAvroInto(ctx, schema.(*AvroSchema), data[5:], typedValue)
	default:
		return fmt.Errorf("unsupported value type: %v", reflect.TypeOf(typedValue))
	}
}

func (c *Client) GetSchema(ctx context.Context, schemaId uint32) (Schema, error) {
	cache1lock.Lock()
	result, ok := c.cache1[schemaId]
	if ok {
		cache1lock.Unlock()
		return result, nil
	}
	httpClient := c.getHttpClient()
	var uri = fmt.Sprintf("%s/schemas/ids/%d", c.config.Url, schemaId)
	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		cache1lock.Unlock()
		return nil, fmt.Errorf("error constructing schema registry http request: %v", err)
	}
	ctxTimeout, cancel := context.WithTimeout(ctx, schemaRegistryRequestTimeout)
	defer cancel()
	if c.config.LogHttp() {
		log.Println("schema_registry_client", req.Method, req.URL)
	}
	resp, err := httpClient.Do(req.WithContext(ctxTimeout))
	if err != nil {
		cache1lock.Unlock()
		return nil, fmt.Errorf("error calling schema registry http client: %v", err)
	}
	if resp.StatusCode != 200 {
		cache1lock.Unlock()
		return nil, fmt.Errorf("unexpected response from the schema registry: %v", resp.StatusCode)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		cache1lock.Unlock()
		return nil, err
	}
	err = resp.Body.Close()
	if err != nil {
		cache1lock.Unlock()
		return nil, err
	}
	response := new(getSchemaResponse)
	err = json.Unmarshal(body, response)
	cache1lock.Unlock()
	if err != nil {
		return nil, fmt.Errorf("error while unmarshalling schema registry: %v", err)
	}
	switch response.SchemaType {
	case schemaTypeProtobuf:
		result, err = c.parseProtobufSchema(ctxTimeout, response.Schema, response.References, nil)
	case schemaTypeDefault:
		fallthrough
	case schemaTypeAvro:
		result, err = c.parseAvroSchema(ctxTimeout, response.Schema, response.References, nil)
	default:
		return nil, fmt.Errorf("unexpected schema type: %v", response.SchemaType)
	}

	if err != nil {
		return nil, fmt.Errorf("error parsing schema registry response: %v", err)
	}
	cache1lock.Lock()
	c.cache1[schemaId] = result
	cache1lock.Unlock()

	return result, nil
}

func (c *Client) GetSubjectVersionBySchemaId(ctx context.Context, subject string, schemaId uint32) (int, error) {
	httpClient := c.getHttpClient()
	var uri = fmt.Sprintf("%s/schemas/ids/%d/versions", c.config.Url, schemaId)
	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return 0, fmt.Errorf("error while creating http request for retreiving subject version: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	ctxTimeout, cancel := context.WithTimeout(ctx, schemaRegistryRequestTimeout)
	defer cancel()
	if c.config.LogHttp() {
		log.Println("schema_registry_client", req.Method, req.URL)
	}
	resp, err := httpClient.Do(req.WithContext(ctxTimeout))
	if err != nil {
		return 0, fmt.Errorf("error while making a http request for retreiving subject version: %v", err)
	}
	if resp.StatusCode != 200 {
		return 0, fmt.Errorf(resp.Status)
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("error reading schema registry http response body: %v", err)
	}
	response := make(getSchemaSubjectsResponse, 0)
	if err := json.Unmarshal(data, &response); err != nil {
		return 0, fmt.Errorf("error unmarshaling schema registry response json: %v", err)
	}
	for _, subjectVersion := range response {
		if subjectVersion.Subject == subject {
			return subjectVersion.Version, nil
		}
	}
	return 0, fmt.Errorf("subject: %v version not found for schema id: %v", subject, schemaId)
}

func (c *Client) GetSubjectVersion(ctx context.Context, subject string, version int) (Schema, error) {
	httpClient := c.getHttpClient()
	var uri = fmt.Sprintf("%s/subjects/%s/versions/%d", c.config.Url, url.PathEscape(subject), version)
	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return nil, fmt.Errorf("error while creating http request for retreiving schema by subject version: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	ctxTimeout, cancel := context.WithTimeout(ctx, schemaRegistryRequestTimeout)
	defer cancel()
	if c.config.LogHttp() {
		log.Println("schema_registry_client", req.Method, req.URL)
	}
	resp, err := httpClient.Do(req.WithContext(ctxTimeout))
	if err != nil {
		return nil, fmt.Errorf("error while making a http request for retreiving schema by subject version: %v", err)
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf(resp.Status)
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading schema registry http response body: %v", err)
	}
	response := new(getSchemaResponse)
	if err := json.Unmarshal(data, &response); err != nil {
		return nil, fmt.Errorf("error unmarshaling schema registry response json: %v", err)
	}
	var result Schema
	switch response.SchemaType {
	case schemaTypeDefault:
		fallthrough
	case schemaTypeAvro:
		result, err = c.parseAvroSchema(ctxTimeout, response.Schema, response.References, &subject)
	case schemaTypeProtobuf:
		result, err = c.parseProtobufSchema(ctxTimeout, response.Schema, response.References, &subject)
	default:
		return nil, fmt.Errorf("unsupported schema type: %v", response.SchemaType)
	}

	if err != nil {
		return nil, fmt.Errorf("error parsing schema registry response: %v", err)
	}
	cache1lock.Lock()
	if c.cache1 == nil {
		c.cache1 = make(map[uint32]Schema)
	}
	c.cache1[response.Id] = result
	cache1lock.Unlock()
	return result, nil
}

func (c *Client) registerSchemaUnderSubject(ctx context.Context, subject, schemaType string, definition string, refs references) (uint32, error) {
	request := &newSchemaRequest{
		SchemaType: schemaType,
		Schema:     definition,
		References: refs,
	}
	jsonRequest, err := json.Marshal(request)
	if err != nil {
		return 0, fmt.Errorf("could not marshal schema registry request: %v", err)
	}
	httpClient := c.getHttpClient()
	var u = fmt.Sprintf("%s/subjects/%s/versions", c.config.Url, url.PathEscape(subject))
	j := new(newSchemaResponse)
	req, err := http.NewRequest("POST", u, bytes.NewReader(jsonRequest))
	if err != nil {
		return 0, fmt.Errorf("error while creating http request for registering a new schema: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	ctxTimeout, cancel := context.WithTimeout(ctx, schemaRegistryRequestTimeout)
	defer cancel()
	if c.config.LogHttp() {
		log.Println("schema_registry_client", req.Method, req.URL)
	}
	resp, err := httpClient.Do(req.WithContext(ctxTimeout))
	if err != nil {
		return 0, fmt.Errorf("error while making a http request for registering a new schema: %v", err)
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("error reading schema registry http response body: %v", err)
	}
	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("error reading schema registry, status: %s, body: %s", resp.Status, string(data))
	}

	if err := json.Unmarshal(data, &j); err != nil {
		return 0, fmt.Errorf("error unmarshaling schema registry response json: %v", err)
	}
	return j.Id, nil

}

func (c *Client) getHttpClient() *http.Client {
	transport := new(http.Transport)
	transport.TLSClientConfig = c.config.Tls
	return &http.Client{Transport: transport}
}
