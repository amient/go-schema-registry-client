package schema_registry

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"github.com/golang/protobuf/jsonpb"
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
	"log"
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

	if c.config.LogEntities() {
		m := &jsonpb.Marshaler{Indent: " "}
		s, _ := m.MarshalToString(fd)
		log.Println(s)
	}

	pfd, err := protodesc.NewFile(fd, c.resolverWithReferences(ctx, refs))
	if err != nil {
		return 0, fmt.Errorf("RegisterProtobufType.NewFile: %v", err)
	}

	//verify that the first type defined in the resultant schema is the requested to be registered
	primaryType := pfd.Messages().Get(0)
	if primaryType.FullName() != protoType.Descriptor().FullName() {
		return 0, fmt.Errorf("registration of proto message %v resulted in a definition with primary message %v",
			protoType.Descriptor().FullName(), primaryType.FullName())
	}

	schema := &ProtobufSchema{definition: f, descriptor: pfd}
	body, err := schema.Render()
	if err != nil {
		return 0, err
	}
	id, err := c.registerSchemaUnderSubject(ctx, subject, schemaTypeProtobuf, body, refs)
	if err != nil {
		return 0, err
	}
	c.cacheProto[protoType.Type()] = id
	if c.config.LogCaches() {
		log.Println("schema_registry_client", "cached schema id", id, protoType.Type())
	}
	return id, nil
}

func (c *Client) addMessage(b *builder.FileBuilder, d *desc.MessageDescriptor) error {

	messages := make([]*builder.MessageBuilder, 0)
	defined := func (name string) *builder.MessageBuilder {
		for _, m := range messages {
			if m.GetName() == name {
				return m
			}
		}
		return nil
	}
	var process func(field *desc.FieldDescriptor, enums []*builder.EnumBuilder) (*builder.FieldBuilder, error)
	traverse := func(msg *desc.MessageDescriptor) (*builder.MessageBuilder, error) {
		m := defined(msg.GetName())
		if m != nil {
			return m, nil
		}
		m = builder.NewMessage(msg.GetName())
		messages = append(messages, m)
		var enums []*builder.EnumBuilder
		for _, enum := range msg.GetNestedEnumTypes() {
			eb := builder.NewEnum(enum.GetName())
			for _, evb := range enum.GetValues() {
				eb.AddValue(builder.NewEnumValue(evb.GetName()).SetNumber(evb.GetNumber()))
			}

			enums = append(enums, eb)
			m.AddNestedEnum(eb)
		}
		//TODO msg.GetNestedMessageTypes()
		for _, field := range msg.GetFields() {
			f, err := process(field, enums)
			if err != nil {
				return nil, err
			}
			m.AddField(f)
		}
		return m, nil
	}
	process = func(field *desc.FieldDescriptor, enums []*builder.EnumBuilder) (*builder.FieldBuilder, error) {
		clone := func(src *desc.FieldDescriptor, dst *builder.FieldBuilder) {
			if src.IsRepeated() {
				dst.SetRepeated()
			}
			if src.IsRequired() {
				dst.SetRepeated()
			}
			if src.IsProto3Optional() {
				dst.SetProto3Optional(true)
			}
			dst.SetJsonName(src.GetJSONName())
			dst.SetLabel(src.GetLabel())
			dst.SetNumber(src.GetNumber())
			dst.SetOptions(src.GetFieldOptions())
		}
		if field.GetType() == descriptorpb.FieldDescriptorProto_TYPE_MESSAGE {
			ref := field.GetMessageType()
			if ref.GetFile().GetFullyQualifiedName() == b.GetFile().GetName() {
				if field.IsMap() {
					//map keys and values are treated recursively
					mk, err := process(field.GetMapKeyType(), enums)
					if err != nil {
						return nil, err
					}
					mv, err := process(field.GetMapValueType(), enums)
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
				clone(field, f)
				return f, nil
			}
		}
		//otherwise just clone the original field
		if field.GetType() == descriptorpb.FieldDescriptorProto_TYPE_ENUM {
			en := field.GetEnumType()
			var eb *builder.EnumBuilder
			var err error
			//if the enum's parent is same as the field's parent then it's a nested enum
			if en.GetParent().GetFullyQualifiedName() == field.GetParent().GetFullyQualifiedName() {
				// look for the equivalent adopted nested enum and use it
				for _, enum := range enums {
					if enum.GetName() == en.GetName() {
						eb = enum
						break
					}
				}
			}
			if eb == nil {
				//use imported enum if it's not nested
				eb, err = builder.FromEnum(en)
				if err != nil {
					return nil, err
				}
			}
			f := builder.NewField(field.GetName(), builder.FieldTypeEnum(eb))
			clone(field, f)
			return f, nil
		}
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
