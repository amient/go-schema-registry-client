package schema_registry

import (
	"bytes"
	"fmt"
	"github.com/amient/avro"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoprint"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
)

//type Fingerprint [32]byte

type Schema interface {
	//Fingerprint() (*Fingerprint, error)
	Render() (string, error)
	Type() string
}

type AvroSchema struct {
	avro avro.Schema
}

func (s *AvroSchema) Type() string {
	return schemaTypeAvro
}

//func (s *AvroSchema) Fingerprint() (*Fingerprint, error) {
//	f, err := s.schema.Fingerprint()
//	if err != nil {
//		return nil, err
//	}
//	fp := Fingerprint(*f)
//	return &fp, nil
//}

func (s *AvroSchema) Render() (string, error) {
	return s.avro.String(), nil
}

type ProtobufSchema struct {
	descriptor protoreflect.FileDescriptor
	definition *desc.FileDescriptor
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

//func (s *ProtobufSchema) Fingerprint() (*Fingerprint, error) {
//	m := &jsonpb.Marshaler{
//		OrigName:     true,
//		EnumsAsInts:  true,
//		EmitDefaults: false,
//	}
//	pb := protodesc.ToFileDescriptorProto(s.descriptor)
//	d, err := m.MarshalToString(pb)
//	if err != nil {
//		return nil, err
//	}
//	f := Fingerprint(sha256.Sum256([]byte(d)))
//	return &f, nil
//}

func NewProtobufSchema(d protoreflect.FileDescriptor) (*ProtobufSchema, error) {
	var convert func(protoreflect.FileDescriptor, bool) (*desc.FileDescriptor, references, error)
	convert = func(in protoreflect.FileDescriptor, dive bool) (*desc.FileDescriptor, references, error) {
		fdpb := protodesc.ToFileDescriptorProto(in)
		imports := in.Imports()
		refs := make(references, 0)
		var deps []*desc.FileDescriptor
		if dive {
			for i := 0; i < imports.Len(); i++ {
				imp := imports.Get(i)
				dp, rs, err := convert(imp, false)
				if err != nil {
					return nil, nil, err
				}
				for _, r := range rs {
					refs = append(refs, r)
				}
				deps = append(deps, dp)
			}
		}
		fd, err := desc.CreateFileDescriptor(fdpb, deps...)
		return fd, refs, err
	}
	file, _, err := convert(d, true)
	if err != nil {
		return nil, fmt.Errorf("NewSchema.CreateFileDescriptor: %v", err)
	}
	return &ProtobufSchema{
		definition: file,
		descriptor: d,
	}, nil
}

//func (f *Fingerprint) Equal(other *Fingerprint) bool {
//	for i, b := range f {
//		if other[i] != b {
//			return false
//		}
//	}
//	return true
//}



