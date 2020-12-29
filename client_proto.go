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
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"io"
	"io/ioutil"
	"strings"
)

func (c *Client) RegisterProtobufType(ctx context.Context, subject string, protoType protoreflect.Message) (uint32, error) {

	if id, ok := c.cacheProto[protoType.Type()]; ok {
		return id, nil
	}

	md := protoType.Descriptor()
	refs, err := c.registerReferencedProtoSchemas(ctx, md.ParentFile())
	if err != nil {
		return 0, fmt.Errorf("RegisterProtobufType.registerReferencedProtoSchemas: %v", err)
	}

	parentFileSchema, err := NewProtobufSchema(md.ParentFile())
	if err != nil {
		return 0, fmt.Errorf("RegisterProtobufType.NewProtobufSchema: %v", err)
	}

	b := builder.NewFile(fmt.Sprintf("%v", md.ParentFile().Path())).
		SetPackageName(parentFileSchema.definition.GetPackage()).
		SetProto3(parentFileSchema.definition.IsProto3())
	msg := parentFileSchema.definition.FindMessage(string(md.FullName()))
	if msg == nil {
		return 0, fmt.Errorf("RegisterProtobufType.FindMessage: not  found: %s", md.FullName())
	}
	err = c.addMessage(b, msg)
	if err != nil {
		return 0, fmt.Errorf("RegisterProtobufType.addMessage: not  found: %s", md.FullName())
	}
	f, err := b.Build()
	if err != nil {
		return 0, fmt.Errorf("RegisterProtobufType.Build: %v", err)
	}
	fd := f.AsFileDescriptorProto()
	pfd, err := protodesc.NewFile(fd, c.resolverWithReferences(ctx, refs))
	if err != nil {
		return 0, fmt.Errorf("RegisterProtobufType.NewFile: %v", err)
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

func (c *Client) addMessage(b *builder.FileBuilder, d *desc.MessageDescriptor) error {

	messages := make(map[string]*builder.MessageBuilder)
	var process func(field *desc.FieldDescriptor) (*builder.FieldBuilder, error)
	traverse := func(msg *desc.MessageDescriptor) (*builder.MessageBuilder, error) {
		m, ok := messages[msg.GetName()]
		if ok {
			return m, nil
		}
		m = builder.NewMessage(msg.GetName())
		messages[msg.GetName()] = m
		for _, field := range msg.GetFields() {
			f, err := process(field)
			if err != nil {
				return nil, err
			}
			m.AddField(f)
		}
		return m, nil
	}
	process = func(field *desc.FieldDescriptor) (*builder.FieldBuilder, error) {
		if field.GetType() == descriptorpb.FieldDescriptorProto_TYPE_MESSAGE {
			ref := field.GetMessageType()
			if ref.GetFile().GetFullyQualifiedName() == b.GetFile().GetName() {
				if field.IsMap() {
					//map keys and values are treated recursively
					mk, err := process(field.GetMapKeyType())
					if err != nil {
						return nil, err
					}
					mv, err := process(field.GetMapValueType())
					if err != nil {
						return nil, err
					}
					return builder.NewMapField(field.GetName(), mk.GetType(), mv.GetType()), nil
				}
				mb, err := traverse(ref)
				if err != nil {
					return nil, err
				}
				//adopt locally referenced message from the same file
				f := builder.NewField(field.GetName(), builder.FieldTypeMessage(mb))
				if field.IsRepeated() {
					f.SetRepeated()
				}
				if field.IsRequired() {
					f.SetRepeated()
				}
				if field.IsProto3Optional() {
					f.SetProto3Optional(true)
				}
				f.SetJsonName(field.GetJSONName())
				f.SetLabel(field.GetLabel())
				f.SetNumber(field.GetNumber())
				f.SetOptions(field.GetFieldOptions())
				return f, nil
			}
		}
		//otherwise just clone the original field
		return builder.FromField(field)
	}
	_, err := traverse(d)
	if err != nil {
		return err
	}
	for _, local := range messages {
		b.AddMessage(local)
	}
	return nil

}

func (c *Client) deserializeProtobuf(ctx context.Context, schema *ProtobufSchema, data []byte) (proto.Message, error) {
	//serialized data gave file descriptors containing exactly one message
	m := schema.descriptor.Messages()
	if m.Len() == 0 {
		return nil, fmt.Errorf("serialized types are expected to have at least one message descriptor")
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
	err = c.deserializeProtobufInto(ctx, data[5:], result)
	return result, err
}

func (c *Client) deserializeProtobufInto(_ context.Context, payload []byte, result proto.Message) error {
	return proto.Unmarshal(payload, result)
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

	file, err := protodesc.NewFile(fd, c.resolverWithReferences(ctx, refs))
	if err != nil {
		return nil, fmt.Errorf("could not resolve proto message by parsing: %v", err)
	}

	return NewProtobufSchema(file)

}

func (c *Client) registerReferencedProtoSchemas(ctx context.Context, file protoreflect.FileDescriptor) (references, error) {
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

	refs := make(references, 0)
	imports := file.Imports()
	for i := 0; i < imports.Len(); i ++ {
		_, irefs, err := register(imports.Get(i))
		if err != nil {
			return nil, err
		}
		for _, r := range irefs {
			refs = append(refs, r)
		}
	}

	return refs, nil
}

func (c *Client) resolverWithReferences(ctx context.Context, refs references) protodesc.Resolver {
	return &versionedResolver{ctx: ctx, registry: c, refs: refs}
}
