package schema_registry

import (
	"bytes"
	"context"
	"crypto/tls"
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
	"time"
)

const schemaTypeDefault = ""
const schemaTypeAvro = "AVRO"
const schemaTypeProtobuf = "PROTOBUF"

const schemaRegistryRequestTimeout = 30 * time.Second

func NewClient(baseUrl string) *Client {
	return &Client{baseUrl: baseUrl}
}

func NewClientWithTls(baseUrl string, config *tls.Config) *Client {
	return &Client{baseUrl: baseUrl, tlsConfig: config}
}

type Client struct {
	baseUrl    string
	cacheProto map[protoreflect.MessageType]uint32 //one-off serialization cache dependent only on the compiled types
	cache1     map[uint32]Schema                   //deserialization cache for schema ids (updated by GetSchema and GetSubjectVersion)
	//cache2     map[string]map[Fingerprint]uint32
	tlsConfig  *tls.Config
}

func (c *Client) Serialize(ctx context.Context, subject string, value interface{}) ([]byte, error) {
	var schemaId uint32
	var err error
	var wireBytes []byte
	switch typedValue := value.(type) {
	case avro.GenericRecord:
		schemaId, err = c.RegisterAvroType(ctx, subject, typedValue.Schema())
		if err != nil {
			return nil, err
		}
		err = fmt.Errorf("implement me:  serialize binary avro")
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
	buf := new(bytes.Buffer)
	err = buf.WriteByte(0) // write magic byte
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
	schemaId := binary.BigEndian.Uint32(data[1:])
	if schemaId < 1 {
		return nil, fmt.Errorf("invalid schema id in the serialized value: %v", schemaId)
	}
	schema, err := c.GetSchema(ctx, schemaId)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema id %d: %v", schemaId, err)
	}

	if protoSchema, ok := schema.(*ProtobufSchema); ok {
		return c.deserializeProtobuf(ctx, protoSchema, data)
	}
	if avroSchema, ok := schema.(*AvroSchema); ok {
		return c.deserializeAvro(ctx, avroSchema, data)
	}
	return nil, fmt.Errorf("unsupported schema type: %v", reflect.TypeOf(schema))

}

func (c *Client) GetSchema(ctx context.Context, schemaId uint32) (Schema, error) {
	if c.cache1 == nil {
		c.cache1 = make(map[uint32]Schema)
	}
	result, ok := c.cache1[schemaId]
	if !ok {
		httpClient := c.getHttpClient()
		var uri = fmt.Sprintf("%s/schemas/ids/%d", c.baseUrl, schemaId)
		req, err := http.NewRequest("GET", uri, nil)
		if err != nil {
			return nil, fmt.Errorf("error constructing schema registry http request: %v", err)
		}
		ctxTimeout, cancel := context.WithTimeout(ctx, schemaRegistryRequestTimeout)
		defer cancel()
		log.Println(req.Method, req.URL)
		resp, err := httpClient.Do(req.WithContext(ctxTimeout))
		if err != nil {
			return nil, fmt.Errorf("error calling schema registry http client: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return nil, fmt.Errorf("unexpected response from the schema registry: %v", resp.StatusCode)
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		response := new(getSchemaResponse)
		err = json.Unmarshal(body, response)
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
		c.cache1[schemaId] = result
	}
	return result, nil
}

func (c *Client) GetSubjectVersionBySchemaId(ctx context.Context, subject string, schemaId uint32) (int, error) {
	httpClient := c.getHttpClient()
	var uri = fmt.Sprintf("%s/schemas/ids/%d/versions", c.baseUrl, schemaId)
	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return 0, fmt.Errorf("error while creating http request for retreiving subject version: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	ctxTimeout, cancel := context.WithTimeout(ctx, schemaRegistryRequestTimeout)
	defer cancel()
	log.Println(req.Method, req.URL)
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
	var uri = fmt.Sprintf("%s/subjects/%s/versions/%d", c.baseUrl, url.PathEscape(subject), version)
	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return nil, fmt.Errorf("error while creating http request for retreiving schema by subject version: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	ctxTimeout, cancel := context.WithTimeout(ctx, schemaRegistryRequestTimeout)
	defer cancel()
	log.Println(req.Method, req.URL)
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
	if c.cache1 == nil {
		c.cache1 = make(map[uint32]Schema)
	}
	c.cache1[response.Id] = result
	return result, nil
}

//func (c *Client) registerSchema(ctx context.Context, subject string, schema Schema) (uint32, error) {
//	if c.cache2 == nil {
//		c.cache2 = make(map[string]map[Fingerprint]uint32)
//	}
//	var subjectCache map[Fingerprint]uint32
//	if subjectCache = c.cache2[subject]; subjectCache == nil {
//		subjectCache = make(map[Fingerprint]uint32)
//		c.cache2[subject] = subjectCache
//	}
//
//	f, err := schema.Fingerprint()
//	if err != nil {
//		return 0, err
//	}
//
//	result, ok := subjectCache[*f]
//	if ok {
//		return result, nil
//	}
//
//	//refs := make(references, 0)
//	//messages := schema.descriptor.Messages()
//	//for i := 0; i < messages.Len(); i++ {
//	//	message := messages.Get(i)
//	//	fields := message.Fields()
//	//	for f := 0; f < fields.Len(); f++ {
//	//		field := fields.Get(f)
//	//		switch field.Kind() {
//	//		case protoreflect.MessageKind:
//	//			fallthrough
//	//		case protoreflect.GroupKind:
//	//			drefs, err := c.registerReferencedSchemas(ctx, field.Message().ParentFile())
//	//			if err != nil {
//	//				return 0, err
//	//			}
//	//			for _, r := range drefs {
//	//				refs = append(refs, r)
//	//			}
//	//		}
//	//	}
//	//}
//	body, err := schema.Render()
//	if err != nil {
//		return 0, err
//	}
//	result, err = c.registerSchemaUnderSubject(ctx, subject, schema.Type(), body, refs)
//	if err != nil {
//		return 0, err
//	}
//	log.Printf("Got Schema ID: %v for subject %v type %v", result, subject, schema.descriptor.FullName())
//	subjectCache[*f] = result
//
//
//	return result, nil
//
//}

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
	var u = c.baseUrl + "/subjects/" + url.PathEscape(subject) + "/versions"
	j := new(newSchemaResponse)
	req, err := http.NewRequest("POST", u, bytes.NewReader(jsonRequest))
	if err != nil {
		return 0, fmt.Errorf("error while creating http request for registering a new schema: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	ctxTimeout, cancel := context.WithTimeout(ctx, schemaRegistryRequestTimeout)
	defer cancel()
	log.Println(req.Method, req.URL)
	resp, err := httpClient.Do(req.WithContext(ctxTimeout))
	if err != nil {
		return 0, fmt.Errorf("error while making a http request for registering a new schema: %v", err)
	}
	if resp.StatusCode != 200 {
		return 0, fmt.Errorf(resp.Status)
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("error reading schema registry http response body: %v", err)
	}
	if err := json.Unmarshal(data, &j); err != nil {
		return 0, fmt.Errorf("error unmarshaling schema registry response json: %v", err)
	}
	return j.Id, nil

}

func (c *Client) getHttpClient() *http.Client {
	transport := new(http.Transport)
	transport.TLSClientConfig = c.tlsConfig
	return &http.Client{Transport: transport}
}

