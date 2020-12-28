package schema_registry

import (
	"crypto/sha256"
	"fmt"
	"github.com/golang/protobuf/jsonpb"
	"github.com/jhump/protoreflect/desc"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type Schema struct {
	descriptor protoreflect.FileDescriptor
	definition *desc.FileDescriptor
}

func NewSchema(d protoreflect.FileDescriptor) (*Schema, error) {
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
	return &Schema{
		definition: file,
		descriptor: d,
	}, nil
}

func (s Schema) Fingerprint() (*Fingerprint, error) {
	m := &jsonpb.Marshaler{
		OrigName:     true,
		EnumsAsInts:  true,
		EmitDefaults: false,
	}
	pb := protodesc.ToFileDescriptorProto(s.descriptor)
	d, err := m.MarshalToString(pb)
	if err != nil {
		return nil, err
	}
	f := Fingerprint(sha256.Sum256([]byte(d)))
	return &f, nil
}

type Fingerprint [32]byte

func (f *Fingerprint) Equal(other *Fingerprint) bool {
	for i, b := range f {
		if other[i] != b {
			return false
		}
	}
	return true
}

