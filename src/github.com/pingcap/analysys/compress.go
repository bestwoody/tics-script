package analysys

import (
	"errors"
	"io"
	"strconv"
	"compress/gzip"
	"compress/flate"
	"github.com/golang/snappy"
)

func Compress(t CompressType, w io.Writer) (io.Writer, io.Closer, error) {
	if t == CompressGzip {
		g := gzip.NewWriter(w)
		return g, g, nil
	}
	if t == CompressFlate3 || t == CompressFlate1 {
		level := 3
		if t == CompressFlate1 {
			level = 1
		}
		f, err := flate.NewWriter(w, level)
		if err != nil {
			return nil, nil, err
		}
		return f, f, nil
	}
	if t == CompressSnappy {
		s := snappy.NewBufferedWriter(w)
		return s, s, nil
	}
	if t == CompressNone {
		return w, nil, nil
	}
	return nil, nil, errors.New("compress method #" + strconv.Itoa(int(t)) + " not supported")
}

func Decompress(t CompressType, r io.Reader) (io.Reader, io.Closer, error) {
	if t == CompressGzip {
		g, err := gzip.NewReader(r)
		if err != nil {
			return nil, nil, err
		}
		return g, g, nil
	}
	if t == CompressFlate3 || t == CompressFlate1 {
		f := flate.NewReader(r)
		return f, f, nil
	}
	if t == CompressSnappy {
		s := snappy.NewReader(r)
		return s, nil, nil
	}
	if t == CompressNone {
		return r, nil, nil
	}
	return nil, nil, errors.New("compress method #" + strconv.Itoa(int(t)) + " not supported")
}

func GetCompressType(name string) (CompressType, error) {
	names := map[string]CompressType {
		"": CompressNone,
		"none": CompressNone,
		"gzip": CompressGzip,
		"zlib": CompressZlib,
		"flate3": CompressFlate3,
		"snappy": CompressSnappy,
		"flate1": CompressFlate1,
	}
	t, ok := names[name]
	if !ok {
		return CompressNone, errors.New("compress method \"" + name + "\" not supported")
	}
	return t, nil
}

type CompressType uint16

const (
	CompressNone CompressType = iota + 1
	CompressGzip
	CompressZlib
	CompressFlate3
	CompressSnappy
	CompressFlate1
)
