package schema_registry

import (
	"context"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"strings"
)

type versionedResolver struct {
	ctx      context.Context
	registry *Client
	refs     references
}

func (r *versionedResolver) FindFileByPath(subject string) (protoreflect.FileDescriptor, error) {
	for _, ref := range r.refs {
		if ref.Subject == subject {
			schema, err := r.registry.GetSubjectVersion(r.ctx, subject, ref.Version)
			if err != nil {
				return nil, err
			}
			return schema.descriptor, nil
		}
	}
	return nil, protoregistry.NotFound
}

func (r *versionedResolver) FindDescriptorByName(name protoreflect.FullName) (protoreflect.Descriptor, error) {
	for _, ref := range r.refs {
		schema, err := r.registry.GetSubjectVersion(r.ctx, ref.Subject, ref.Version)
		if err != nil {

		}
		if schema == nil {
			return nil, protoregistry.NotFound
		}
		matchParent := false
		for p := name.Parent(); p != ""; p = p.Parent() {
			if strings.HasSuffix(string(p), string(schema.descriptor.FullName())) {
				matchParent = true
			}
		}
		if !matchParent {
			continue
		}
		if m := schema.descriptor.Messages().ByName(name.Name()); m != nil {
			return m, nil
		}

	}
	return nil, protoregistry.NotFound
}

