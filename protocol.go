package schema_registry

type newSchemaRequest struct {
	SchemaType string     `json:"schemaType"`
	References references `json:"references"`
	Schema     string     `json:"schema"`
}

type references []reference

type reference struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

type newSchemaResponse struct {
	Id uint32 `json:"id"`
}

type getSchemaResponse struct {
	Id         uint32     `json:"id"`
	SchemaType string     `json:"schemaType"`
	Schema     string     `json:"schema"`
	References references `json:"references"`
}

type getSchemaSubjectsResponse []subjectVersion

type subjectVersion struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
}


