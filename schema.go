package registry

//type Fingerprint [32]byte

type Schema interface {
	//Fingerprint() (*Fingerprint, error)
	Render() (string, error)
	Type() string
}

//func (f *Fingerprint) Equal(other *Fingerprint) bool {
//	for i, b := range f {
//		if other[i] != b {
//			return false
//		}
//	}
//	return true
//}



