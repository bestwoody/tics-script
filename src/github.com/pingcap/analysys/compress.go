package analysys

import (
	"io"
	"fmt"
	"compress/gzip"
	"compress/flate"
	"github.com/golang/snappy"
)

var RegisteredCompressers = NewCompressers()

func NewCompressers() *Compressers {
	self := &Compressers{
		map[string]CompressType {},
		map[CompressType]Compresser {},
	}
	self.Reg(CompresserNone {CompresserBase {"", CompressNone}})
	self.Reg(CompresserNone {CompresserBase {"none", CompressNone}})
	self.Reg(CompresserGzip {CompresserBase {"gzip", CompressGzip}})
	self.Reg(CompresserFlate {CompresserBase {"flate3", CompressFlate3}, 3})
	self.Reg(CompresserFlate {CompresserBase {"flate1", CompressFlate1}, 1})
	self.Reg(CompresserSnappy {CompresserBase {"snappy", CompressSnappy}})
	return self
}

func (self *Compressers) GetCompressType(name string) (CompressType, error) {
	t, ok := self.names[name]
	if !ok {
		return CompressNone, fmt.Errorf("compress method \"%v\" not supported", name)
	}
	return t, nil
}

func (self *Compressers)  Decompress(t CompressType, r io.Reader) (io.Reader, io.Closer, error) {
	compresser, ok := self.compressers[t]
	if !ok {
		return nil, nil, fmt.Errorf("compress method #%v not supported", t)
	}
	return compresser.Decompress(r)
}

func (self *Compressers) Compress(t CompressType, w io.Writer) (io.Writer, io.Closer, error) {
	compresser, ok := self.compressers[t]
	if !ok {
		return nil, nil, fmt.Errorf("compress method #%v not supported", t)
	}
	return compresser.Compress(w)
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

func (self CompresserSnappy) Compress(w io.Writer) (io.Writer, io.Closer, error) {
	s := snappy.NewBufferedWriter(w)
	return s, s, nil
}

func (self CompresserSnappy) Decompress(r io.Reader) (io.Reader, io.Closer, error) {
	s := snappy.NewReader(r)
	return s, nil, nil
}

type CompresserSnappy struct {
	CompresserBase
}

func (self CompresserFlate) Compress(w io.Writer) (io.Writer, io.Closer, error) {
	f, err := flate.NewWriter(w, self.level)
	if err != nil {
		return nil, nil, err
	}
	return f, f, nil
}

func (self CompresserFlate) Decompress(r io.Reader) (io.Reader, io.Closer, error) {
	f := flate.NewReader(r)
	return f, f, nil
}

type CompresserFlate struct {
	CompresserBase
	level int
}

func (self CompresserGzip) Compress(w io.Writer) (io.Writer, io.Closer, error) {
	g := gzip.NewWriter(w)
	return g, g, nil
}

func (self CompresserGzip) Decompress(r io.Reader) (io.Reader, io.Closer, error) {
	g, err := gzip.NewReader(r)
	if err != nil {
		return nil, nil, err
	}
	return g, g, nil
}

type CompresserGzip struct {
	CompresserBase
}

func (self CompresserNone) Compress(w io.Writer) (io.Writer, io.Closer, error) {
	return w, nil, nil
}

func (self CompresserNone) Decompress(r io.Reader) (io.Reader, io.Closer, error) {
	return r, nil, nil
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
	Compress(w io.Writer) (io.Writer, io.Closer, error)
	Decompress(r io.Reader) (io.Reader, io.Closer, error)
}

const (
	CompressNone CompressType = iota + 1
	CompressGzip
	CompressZlib
	CompressFlate3
	CompressSnappy
	CompressFlate1
)

type CompressType uint16
