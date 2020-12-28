package schema_registry

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/builder"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/desc/protoprint"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const schemaTypeProtobuf = "PROTOBUF"
const schemaRegistryRequestTimeout = 30 * time.Second

func NewClient(baseUrl string) *Client {
	return &Client{baseUrl: baseUrl}
}

type Client struct {
	baseUrl   string
	cache0    map[protoreflect.MessageType]uint32 //one-off serialization cache dependent only on the compiled types
	cache1    map[uint32]*Schema                  //deserialization cache for schema ids (updated by GetSchema and GetSubjectVersion)
	cache2    map[string]map[Fingerprint]uint32
	tlsConfig *tls.Config
}

func (c *Client) WithTls(config *tls.Config) *Client {
	c.tlsConfig = config
	return c
}

func (c *Client) AutoSerialize(ctx context.Context, subject string, value proto.Message) ([]byte, error) {
	protoType := proto.MessageReflect(value)
	schemaId, err := c.RegisterSchemaForType(ctx, subject, protoType)
	if err != nil {
		return nil, err
	}
	return c.Serialize(schemaId, value)
}

func (c *Client) Serialize(schemaId uint32, value proto.Message) ([]byte, error) {
	wireBytes, err := proto.Marshal(value)
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

func (c *Client) Deserialize(ctx context.Context, data []byte) (proto.Message, error) {
	schemaId := binary.BigEndian.Uint32(data[1:])
	if schemaId < 1 {
		return nil, fmt.Errorf("invalid schema id in the serialized value: %v", schemaId)
	}
	schema, err := c.GetSchema(ctx, schemaId)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema id %d: %v", schemaId, err)
	}

	//serialized data gave file descriptors containing exactly one message
	m := schema.descriptor.Messages()
	if m.Len() != 1 {
		return nil, fmt.Errorf("serialized types are expected to have proto descriptors containing exactly one message, got: %d", m.Len())
	}
	md := m.Get(0)

	var result proto.Message

	//try specific proto message type first
	fn := md.FullName()
	p, err := protoregistry.GlobalTypes.FindMessageByName(fn)
	if err != nil {
		return nil, fmt.Errorf("failed to locate global message type %v: %v", fn, err)
	}
	if p != nil {
		result = proto.MessageV1(p.New())
	} else {
		//otherwise resolve with dynamicpb
		result = dynamicpb.NewMessage(md)
	}
	err = proto.Unmarshal(data[5:], result)
	return result, err
}

func (c *Client) RegisterSchemaForType(ctx context.Context, subject string, protoType protoreflect.Message) (uint32, error) {

	if c.cache0 == nil {
		c.cache0 = make(map[protoreflect.MessageType]uint32)
	}
	if id, ok := c.cache0[protoType.Type()]; ok {
		return id, nil
	}

	md := protoType.Descriptor()
	refs, err := c.registerReferencedSchemas(ctx, md.ParentFile())
	if err != nil {
		return 0, fmt.Errorf("RegisterSchemaForValue.registerReferencedSchemas: %v", err)
	}

	parentFileSchema, err := NewSchema(md.ParentFile())
	if err != nil {
		return 0, fmt.Errorf("RegisterSchemaForValue.parentFileSchema: %v", err)
	}
	b := builder.NewFile(fmt.Sprintf("%v.proto", md.FullName())).
		SetPackageName(parentFileSchema.definition.GetPackage()).
		SetProto3(parentFileSchema.definition.IsProto3())
	msg := parentFileSchema.definition.FindMessage(string(md.FullName()))
	if msg == nil {
		return 0, fmt.Errorf("RegisterSchemaForValue.FindMessage: not  found: %s", md.FullName())
	}
	m, err := builder.FromMessage(msg)
	if err != nil {
		return 0, fmt.Errorf("RegisterSchemaForValue.FromMessage: %v", err)
	}
	b.AddMessage(m)
	f, err := b.Build()
	if err != nil {
		return 0, fmt.Errorf("RegisterSchemaForValue.Build: %v", err)
	}
	pfd, err := protodesc.NewFile(f.AsFileDescriptorProto(), c.resolverWithReferences(ctx, refs))
	if err != nil {
		return 0, fmt.Errorf("RegisterSchemaForValue.NewFile: %v", err)
	}
	schema := &Schema{
		definition: f,
		descriptor: pfd,
	}
	id, err := c.registerSchema(ctx, subject, schema)
	if err != nil {
		return 0, err
	}
	c.cache0[protoType.Type()] = id
	return id, nil
}

func (c *Client) GetSchema(ctx context.Context, schemaId uint32) (*Schema, error) {
	if c.cache1 == nil {
		c.cache1 = make(map[uint32]*Schema)
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
		if response.SchemaType != schemaTypeProtobuf {
			return nil, fmt.Errorf("unexpected schema type: %v", response.SchemaType)
		}
		result, err = c.parseSchema(ctxTimeout, response.Schema, response.References, nil)
		if err != nil {
			return nil, fmt.Errorf("error parsing schema registry response: %v", err)
		}
		c.cache1[schemaId] = result
	}
	return result, nil
}

func (c *Client) parseSchema(ctx context.Context, definition string, refs references, name *string) (*Schema, error) {

	var uniqueName string
	if name == nil {
		hash := sha256.Sum256([]byte(definition))
		uniqueName = base64.StdEncoding.EncodeToString(hash[:])
	} else {
		uniqueName = *name
	}
	parser := &protoparse.Parser{
		Accessor: func(filename string) (io.ReadCloser, error) {
			if filename == uniqueName {
				return ioutil.NopCloser(strings.NewReader(definition)), nil
			} else {
				return nil, protoregistry.NotFound
			}
		},
	}

	fds, err := parser.ParseFilesButDoNotLink(uniqueName)
	fd := fds[0]
	if err != nil {
		return nil, err
	}
	//for _, m := range fd.MessageType {
	//	fmt.Println("???", m.GetName())
	//}

	file, err := protodesc.NewFile(fd, c.resolverWithReferences(ctx, refs))
	if err != nil {
		return nil, fmt.Errorf("could not resolve proto message by parsing: %v", err)
	}
	//messages := file.Messages()
	//for i := 0; i < messages.Len(); i++ {
	//	fmt.Println("###", messages.Get(i).Name())
	//}
	return NewSchema(file)
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

func (c *Client) GetSubjectVersion(ctx context.Context, subject string, version int) (*Schema, error) {
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
	result, err := c.parseSchema(ctxTimeout, response.Schema, response.References, &subject)
	if err != nil {
		return nil, fmt.Errorf("error parsing schema registry response: %v", err)
	}
	if c.cache1 == nil {
		c.cache1 = make(map[uint32]*Schema)
	}
	c.cache1[response.Id] = result
	return result, nil
}

func (c *Client) registerSchema(ctx context.Context, subject string, schema *Schema) (uint32, error) {
	if c.cache2 == nil {
		c.cache2 = make(map[string]map[Fingerprint]uint32)
	}
	var s map[Fingerprint]uint32
	if s = c.cache2[subject]; s == nil {
		s = make(map[Fingerprint]uint32)
		c.cache2[subject] = s
	}

	f, err := schema.Fingerprint()
	if err != nil {
		return 0, err
	}

	result, ok := s[*f]
	if !ok {
		refs := make(references, 0)
		messages := schema.descriptor.Messages()
		for i := 0; i < messages.Len(); i++ {
			message := messages.Get(i)
			fields := message.Fields()
			for f := 0; f < fields.Len(); f++ {
				field := fields.Get(f)
				switch field.Kind() {
				case protoreflect.MessageKind:
					fallthrough
				case protoreflect.GroupKind:
					drefs, err := c.registerReferencedSchemas(ctx, field.Message().ParentFile())
					if err != nil {
						return 0, err
					}
					for _, r := range drefs {
						refs = append(refs, r)
					}
				}
			}
		}
		result, err = c.registerSchemaUnderSubject(ctx, subject, schema.definition, refs)
		if err != nil {
			return 0, err
		}
		log.Printf("Got Schema ID: %v for subject %v type %v", result, subject, schema.descriptor.FullName())
		s[*f] = result
	}

	return result, nil

}

func (c *Client) registerReferencedSchemas(ctx context.Context, in protoreflect.FileDescriptor) (references, error) {
	var register func(in protoreflect.FileDescriptor) (*desc.FileDescriptor, references, error)
	register = func(in protoreflect.FileDescriptor) (*desc.FileDescriptor, references, error) {
		fdpb := protodesc.ToFileDescriptorProto(in)
		imports := in.Imports()
		refs := make(references, 0)
		var deps []*desc.FileDescriptor
		for i := 0; i < imports.Len(); i++ {
			imp := imports.Get(i)
			dp, drefs, err := register(imp)
			if err != nil {
				return nil, nil, err
			}
			for _, r := range drefs {
				refs = append(refs, r)
			}
			deps = append(deps, dp)
		}
		fd, err := desc.CreateFileDescriptor(fdpb, deps...)
		if err != nil {
			return nil, nil, err
		}
		id, err := c.registerSchemaUnderSubject(ctx, in.Path(), fd, refs)
		if err != nil {
			return nil, nil, err
		}
		version, err := c.GetSubjectVersionBySchemaId(ctx, in.Path(), id)
		if err != nil {
			return nil, nil, err
		}
		refs = append(refs, reference{
			Name:    in.Path(),
			Subject: in.Path(),
			Version: version,
		})
		return fd, refs, nil
	}
	_, ids, err := register(in)
	return ids, err
}

func (c *Client) registerSchemaUnderSubject(ctx context.Context, subject string, definition *desc.FileDescriptor, refs references) (uint32, error) {
	printer := new(protoprint.Printer)
	buf := new(bytes.Buffer)
	err := printer.PrintProtoFile(definition, buf)
	if err != nil {
		return 0, fmt.Errorf("could not render proto file: %v", err)
	}
	request := &newSchemaRequest{
		SchemaType: schemaTypeProtobuf,
		Schema:     buf.String(),
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

func (c *Client) resolverWithReferences(ctx context.Context, refs references) protodesc.Resolver {
	return &versionedResolver{ctx: ctx, registry: c, refs: refs}
}




