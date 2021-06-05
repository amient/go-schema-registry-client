package registry

import (
	"bytes"
	"fmt"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoprint"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type ProtobufSchema struct {
	descriptor protoreflect.FileDescriptor
	definition *desc.FileDescriptor
}

func NewProtobufSchema(d protoreflect.FileDescriptor) (*ProtobufSchema, error) {
	var convert func(protoreflect.FileDescriptor) (*desc.FileDescriptor, references, error)
	convert = func(in protoreflect.FileDescriptor) (*desc.FileDescriptor, references, error) {
		fdpb := protodesc.ToFileDescriptorProto(in)
		imports := in.Imports()
		refs := make(references, 0)
		var deps []*desc.FileDescriptor
		for i := 0; i < imports.Len(); i++ {
			imp := imports.Get(i)
			dp, rs, err := convert(imp)
			if err != nil {
				return nil, nil, err
			}
			for _, r := range rs {
				refs = append(refs, r)
			}
			deps = append(deps, dp)
		}
		fd, err := desc.CreateFileDescriptor(fdpb, deps...)
		if err != nil {
			err = fmt.Errorf("CreateFileDescriptor %s error:%v", *fdpb.Name, err)
		}
		return fd, refs, err
	}
	file, _, err := convert(d)
	if err != nil {
		return nil, fmt.Errorf("NewSchema.CreateFileDescriptor: %v", err)
	}
	return &ProtobufSchema{
		definition: file,
		descriptor: d,
	}, nil
}

func (s *ProtobufSchema) Type() string {
	return schemaTypeProtobuf
}

func (s *ProtobufSchema) Render() (string, error) {
	printer := new(protoprint.Printer)
	buf := new(bytes.Buffer)
	err := printer.PrintProtoFile(s.definition, buf)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

func (s *ProtobufSchema) Print() {
	r, err := s.Render()
	if err != nil {
		panic(err)
	}
	fmt.Println(r)
}

