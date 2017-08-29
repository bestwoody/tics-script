package analysys

type CompressType uint16

const (
	CompressNone CompressType = iota + 1
	CompressLz4
)

func GetCompressType(name string) CompressType {
	return CompressNone
}
