package analysys

import (
	"fmt"
	"github.com/golang/snappy"
)

var RegisteredCompressers = NewCompressers()

func NewCompressers() *Compressers {
	self := &Compressers{
		map[string]CompressType {},
		map[CompressType]Compresser {},
	}
	self.Reg(CompresserNone {CompresserBase {"none", CompressNone}})
	self.Reg(CompresserSnappy {CompresserBase {"snappy", CompressSnappy}})
	return self
}

func (self *Compressers) Get(ct CompressType) (Compresser, error) {
	compresser, ok := self.compressers[ct]
	if !ok {
		return nil, fmt.Errorf("compress method #%v not supported", ct)
	}
	return compresser, nil
}

func (self *Compressers) GetCompressType(name string) (CompressType, error) {
	t, ok := self.names[name]
	if !ok {
		return CompressNone, fmt.Errorf("compress method \"%v\" not supported", name)
	}
	return t, nil
}

func (self *Compressers) Reg(compresser Compresser) {
	t := compresser.Type()
	self.names[compresser.Name()] = t
	self.compressers[t] = compresser
}

type Compressers struct {
	names map[string]CompressType
	compressers map[CompressType]Compresser
}

func (self CompresserSnappy) Compress(src []byte) ([]byte, error) {
	coded := snappy.Encode(nil, src)
	return coded, nil
}

func (self CompresserSnappy) Decompress(src []byte) ([]byte, error) {
	return snappy.Decode(nil, src)
}

type CompresserSnappy struct {
	CompresserBase
}

func (self CompresserNone) Compress(src []byte) ([]byte, error) {
	return src, nil
}

func (self CompresserNone) Decompress(src []byte) ([]byte, error) {
	return src, nil
}

type CompresserNone struct {
	CompresserBase
}

func (self CompresserBase) Type() CompressType {
	return self.ct
}

func (self CompresserBase) Name() string {
	return self.name
}

type CompresserBase struct {
	name string
	ct CompressType
}

type Compresser interface {
	Name() string
	Type() CompressType
	Compress(src []byte) ([]byte, error)
	Decompress(src []byte) ([]byte, error)
}

const (
	CompressNone = CompressType(1)
	CompressSnappy = CompressType(5)
)

type CompressType uint16
