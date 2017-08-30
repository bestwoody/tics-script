package analysys

import (
	"errors"
	"io"
	"strconv"
	"compress/gzip"
)

func Compress(t CompressType, w io.Writer) (io.Writer, io.Closer, error) {
	if t == CompressGzip {
		g := gzip.NewWriter(w)
		return g, g, nil
	}
	if t == CompressNone {
		return w, nil, nil
	}
	return nil, nil, errors.New("compress method " + strconv.Itoa(int(t)) + " not supported")
}

func Decompress(t CompressType, r io.Reader) (io.Reader, io.Closer, error) {
	if t == CompressGzip {
		g, err := gzip.NewReader(r)
		if err != nil {
			return nil, nil, err
		}
		return g, g, nil
	}
	if t == CompressNone {
		return r, nil, nil
	}
	return nil, nil, errors.New("compress method " + strconv.Itoa(int(t)) + " not supported")
}

func GetCompressType(name string) (CompressType, error) {
	names := map[string]CompressType {
		"": CompressNone,
		"none": CompressNone,
		"gzip": CompressGzip,
		"zlib": CompressZlib,
		"flate": CompressFlate,
	}
	t, ok := names[name]
	if !ok {
		return CompressNone, errors.New("compress method " + name + " not supported")
	}
	return t, nil
}

type CompressType uint16

const (
	CompressNone CompressType = iota + 1
	CompressGzip
	CompressZlib
	CompressFlate
)
