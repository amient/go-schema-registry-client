package registry

type Schema interface {
	Render() (string, error)
	Type() string
}


