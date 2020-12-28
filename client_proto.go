package schema_registry

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
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
	"strings"
)

func (c *Client) RegisterProtobufType(ctx context.Context, subject string, protoType protoreflect.Message) (uint32, error) {

	if c.cacheProto == nil {
		c.cacheProto = make(map[protoreflect.MessageType]uint32)
	}
	if id, ok := c.cacheProto[protoType.Type()]; ok {
		return id, nil
	}

	md := protoType.Descriptor()
	refs, err := c.registerReferencedProtoSchemas(ctx, md.ParentFile())
	if err != nil {
		return 0, fmt.Errorf("RegisterSchemaForValue.registerReferencedSchemas: %v", err)
	}

	parentFileSchema, err := NewProtobufSchema(md.ParentFile())
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
	schema := &ProtobufSchema{
		definition: f,
		descriptor: pfd,
	}
	body, err := schema.Render()
	if err != nil {
		return 0, err
	}
	id, err := c.registerSchemaUnderSubject(ctx, subject, schemaTypeProtobuf, body, refs)
	if err != nil {
		return 0, err
	}
	c.cacheProto[protoType.Type()] = id
	return id, nil
}

func (c *Client) deserializeProtobuf(ctx context.Context, schema *ProtobufSchema, data []byte) (proto.Message, error) {
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

func (c *Client) parseProtobufSchema(ctx context.Context, definition string, refs references, name *string) (*ProtobufSchema, error) {

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
	return NewProtobufSchema(file)
}

func (c *Client) registerReferencedProtoSchemas(ctx context.Context, in protoreflect.FileDescriptor) (references, error) {
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
		printer := new(protoprint.Printer)
		buf := new(bytes.Buffer)
		err = printer.PrintProtoFile(fd, buf)
		if err != nil {
			return nil, nil, err
		}
		id, err := c.registerSchemaUnderSubject(ctx, in.Path(), schemaTypeProtobuf, buf.String(), refs)
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

func (c *Client) resolverWithReferences(ctx context.Context, refs references) protodesc.Resolver {
	return &versionedResolver{ctx: ctx, registry: c, refs: refs}
}
