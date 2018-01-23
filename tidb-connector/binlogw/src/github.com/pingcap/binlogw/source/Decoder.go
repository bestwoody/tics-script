package source

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"github.com/pingcap/tipb/go-binlog"
)

type Decoder struct {
	br *bufio.Reader
	pos binlog.Pos
	crcTable *crc32.Table
}

func NewDecoder(pos binlog.Pos, r io.Reader) *Decoder {
	return &Decoder{
		bufio.NewReader(r),
		pos,
		crc32.MakeTable(crc32.Castagnoli),
	}
}

func (self *Decoder) Decode(ent *binlog.Entity) error {
	if self.br == nil {
		return io.EOF
	}

	// read and chekc magic number
	magicNum, err := readInt32(self.br)
	if err == io.EOF {
		self.br = nil
		return io.EOF
	}

	err = checkMagic(magicNum)
	if err != nil {
		return err
	}

	// read payload+crc  length
	size, err := readInt64(self.br)
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	data := make([]byte, size+4)

	// read payload+crc
	if _, err = io.ReadFull(self.br, data); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return err
	}

	// decode bytes to ent struct and validate crc
	entryCrc := binary.LittleEndian.Uint32(data[size:])
	ent.Payload = data[:size]
	crc := crc32.Checksum(ent.Payload, self.crcTable)
	if crc != entryCrc {
		return ErrCRCMismatch
	}

	ent.Pos = binlog.Pos{
		Suffix: self.pos.Suffix,
		Offset: self.pos.Offset,
	}

	// 12 is size + magic length
	self.pos.Offset += size + 16

	return nil
}

func checkMagic(mgicNum uint32) error {
	if mgicNum != magic {
		return ErrCRCMismatch
	}

	return nil
}

func readInt64(r io.Reader) (int64, error) {
	var n int64
	err := binary.Read(r, binary.LittleEndian, &n)
	return n, err
}

func readInt32(r io.Reader) (uint32, error) {
	var n uint32
	err := binary.Read(r, binary.LittleEndian, &n)
	return n, err
}

var magic uint32 = 471532804
var ErrCRCMismatch = errors.New("binlogger: crc mismatch")