package analysys

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"fmt"
	"os"
	"unsafe"
	"github.com/golang/snappy"
)

func IndexingLoad(path string) (*Indexing, error) {
	index, err := IndexLoad(path + IndexFileSuffix)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("opening indexing, reading data: " + err.Error())
	}
	info, err := f.Stat()
	if err != nil {
		return nil, errors.New("opening indexing, reading data: " + err.Error())
	}
	return &Indexing {index, f, info.Size()}, nil
}

func (self *Indexing) Interset(lower, upper TimestampBound) []int {
	blocks := make([]int, 0)
	for i := 0; i < self.Blocks(); i++ {
		begin := self.Index[i].Ts
		end := self.Index[i + 1].Ts
		if !lower.IsOpen() && (end < lower.Ts || (end == lower.Ts && !lower.Included)) {
			continue
		}
		if !upper.IsOpen() && (begin > upper.Ts || (begin == upper.Ts && !upper.Included)) {
			continue
		}
		blocks = append(blocks, i)
	}
	return blocks
}

func (self *Indexing) Close() error {
	return self.file.Close()
}

func (self *Indexing) Blocks() int {
	if len(self.Index) == 0 {
		return 0
	}
	return len(self.Index) - 1
}

func (self *Indexing) BulkLoad(i int) ([]byte, error) {
	if i >= len(self.Index) - 1 {
		return nil, fmt.Errorf("indexing load block: #%v >= %v", i, len(self.Index))
	}
	entry := self.Index[i]
	data := make([]byte, entry.Size)
	r := io.NewSectionReader(self.file, int64(entry.Offset), int64(entry.Size))
	_, err := io.ReadFull(r, data)
	return data, err
}

func (self *Indexing) Load(i int) (Block, error) {
	if i >= len(self.Index) - 1 {
		return nil, fmt.Errorf("indexing load block: #%v >= %v", i, len(self.Index))
	}
	entry := self.Index[i]
	r := io.NewSectionReader(self.file, int64(entry.Offset), int64(entry.Size))
	return BlockLoad(bufio.NewReaderSize(r, BufferSizeRead))
}

type Indexing struct {
	Index Index
	file *os.File
	total int64
}

func PartWrite(rows Rows, path string, compress string, gran, align int) (Index, error) {
	f, err := os.OpenFile(path, os.O_RDWR | os.O_CREATE | os.O_EXCL, 0644)
	if err != nil {
		return nil, errors.New("writing sorted data: " + err.Error())
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, BufferSizeWrite)

	offset := uint32(0)
	count := (len(rows) + gran - 1) / gran
	index := make(Index, count)

	var padding []byte
	if align > 0 {
		padding = make([]byte, align)
	}

	ts := Timestamp(0)
	for i := 0; len(rows) != 0; i++ {
		count := gran
		if len(rows) < gran {
			count = len(rows)
		}

		if align > 0 && (int(offset) % align != 0) {
			n := align - int(offset) % align
			_, err = w.Write(padding[0: n])
			if err != nil {
				return nil, fmt.Errorf("writing padding: %v, %s", n, err.Error())
			}
			offset += uint32(n)
		}

		block := Block(rows[0: count])

		var n int
		if compress == "snappy" {
			n, err = block.BulkWrite(w, compress)
		} else {
			n, err = block.Write(w, compress)
		}
		if err != nil {
			return nil, err
		}
		rows = rows[count: len(rows)]
		index[i] = IndexEntry {block[0].Ts, offset, uint32(n)}
		offset += uint32(n)
		ts = block[len(block) - 1].Ts
	}

	index = append(index, IndexEntry {ts, offset, 0})

	err = w.Flush()
	return index, err
}

func (self Index) Write(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR | os.O_CREATE | os.O_EXCL, 0644)
	if err != nil {
		return errors.New("writing index: " + err.Error())
	}
	defer f.Close()

	crc32 := crc32.NewIEEE()
	w := bufio.NewWriterSize(io.MultiWriter(f, crc32), BufferSizeWrite)

	err = binary.Write(w, binary.LittleEndian, uint32(len(self)))
	if err != nil {
		return errors.New("writing index entry count: " + err.Error())
	}
	for _, v := range self {
		err = binary.Write(w, binary.LittleEndian, v)
		if err != nil {
			return errors.New("writing index: " + err.Error())
		}
	}

	err = w.Flush()
	if err != nil {
		return err
	}
	err = binary.Write(f, binary.LittleEndian, crc32.Sum32())
	if err != nil {
		return errors.New("writing index checksum: " + err.Error())
	}
	return nil
}

func IndexLoad(path string) (Index, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("reading index: " + err.Error())
	}
	defer f.Close()

	crc32 := crc32.NewIEEE()
	r := io.TeeReader(bufio.NewReader(f), crc32)

	count := uint32(0)
	err = binary.Read(r, binary.LittleEndian, &count)
	if err != nil {
		return nil, errors.New("reading index entry count: " + err.Error())
	}

	index := make(Index, count)
	for i := uint32(0); i < count; i++ {
		err = binary.Read(r, binary.LittleEndian, &index[i])
		if err != nil {
			return nil, errors.New("reading index: " + err.Error())
		}
	}

	c1 := crc32.Sum32()
	c2 := uint32(0)
	err = binary.Read(r, binary.LittleEndian, &c2)
	if err != nil {
		return index, errors.New("reading index checksum: " + err.Error())
	}
	if c1 != c2 {
		return index, fmt.Errorf("index checksum not matched: cal:%v VS read:%v", c1, c2)
	}

	return index, nil
}

type Index []IndexEntry

type IndexEntry struct {
	Ts Timestamp
	Offset uint32
	Size uint32
}

func (self *Rows) Add(v Row) {
	*self = append(*self, v)
}

func (self Rows) Len() int {
	return len(self)
}

func (self Rows) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

func (self Rows) Less(i, j int) bool {
	return self[i].Ts < self[j].Ts
}

type Rows []Row

func (self Block) BulkWrite(w io.Writer, compress string) (int, error) {
	written := 0

	err := binary.Write(w, binary.LittleEndian, MagicFlag)
	if err != nil {
		return 0, errors.New("writing block magic flag: " + err.Error())
	}
	written += 2

	ct, err := RegisteredCompressers.GetCompressType(compress)
	if err != nil {
		return written, err
	}
	if ct != CompressSnappy {
		return written, errors.New("bulk write only support snappy")
	}

	err = binary.Write(w, binary.LittleEndian, ct)
	if err != nil {
		return written, errors.New("writing block compress type: " + err.Error())
	}
	written += 2

	err = binary.Write(w, binary.LittleEndian, uint32(len(self)))
	if err != nil {
		return 0, errors.New("writing block size: " + err.Error())
	}
	written += 4

	buf := bytes.NewBuffer(nil)
	for i, row := range self {
		_, err := row.Write(buf)
		if err != nil {
			return written, fmt.Errorf("writing block row #%v: %s", i, err.Error())
		}
	}
	data := buf.Bytes()
	crc := crc32.ChecksumIEEE(data)

	coded := snappy.Encode(nil, data)
	n, err := w.Write(coded)
	if err != nil {
		return written, errors.New("writing compressed data: " + err.Error())
	}
	written += int(n)

	err = binary.Write(w, binary.LittleEndian, crc)
	if err != nil {
		return written, errors.New("writing block checksum: " + err.Error())
	}
	written += 4

	return written, err
}

func (self Block) Write(w io.Writer, compress string) (int, error) {
	written := 0

	err := binary.Write(w, binary.LittleEndian, MagicFlag)
	if err != nil {
		return 0, errors.New("writing block magic flag: " + err.Error())
	}
	written += 2

	ct, err := RegisteredCompressers.GetCompressType(compress)
	if err != nil {
		return written, err
	}
	err = binary.Write(w, binary.LittleEndian, ct)
	if err != nil {
		return written, errors.New("writing block compress type: " + err.Error())
	}
	written += 2

	err = binary.Write(w, binary.LittleEndian, uint32(len(self)))
	if err != nil {
		return 0, errors.New("writing block size: " + err.Error())
	}
	written += 4

	origin := w

	// TODO: pipeline write
	buf := bytes.NewBuffer(nil)
	w, closer, err := RegisteredCompressers.Compress(ct, buf)
	if err != nil {
		return written, err
	}

	crc32 := crc32.NewIEEE()
	w = io.MultiWriter(crc32, w)

	for i, row := range self {
		_, err := row.Write(w)
		if err != nil {
			return written, fmt.Errorf("writing block row #%v: %s", i, err.Error())
		}
	}

	err = closer.Close()
	if err != nil {
		return written, errors.New("closing compresser: " + err.Error())
	}

	n, err := io.Copy(origin, buf)
	if err != nil {
		return written, errors.New("writing compressed data: " + err.Error())
	}
	written += int(n)

	crc := crc32.Sum32()
	err = binary.Write(origin, binary.LittleEndian, crc)
	if err != nil {
		return written, errors.New("writing block checksum: " + err.Error())
	}
	written += 4

	return written, err
}

func BlockBulkLoad(data []byte) (Block, []byte, error) {
	magic := *(*uint16)(unsafe.Pointer(&data[0]))
	if magic != MagicFlag {
		return nil, nil, fmt.Errorf("magic flag not matched: real:%v VS read:%v", MagicFlag, magic)
	}
	compress := *(*CompressType)(unsafe.Pointer(&data[2]))
	count := *(*uint32)(unsafe.Pointer(&data[4]))
	crc := *(*uint32)(unsafe.Pointer(&data[len(data) - 4]))

	data = data[8: len(data) - 4]
	var decoded []byte

	if compress == CompressNone {
		decoded = data
	} else if compress == CompressSnappy {
		var err error
		decoded, err = snappy.Decode(nil, data)
		if err != nil {
			return nil, nil, fmt.Errorf("snappy decompress failed: %s", err.Error())
		}
	} else {
		buf := bytes.NewBuffer(nil)
		r, closer, err := RegisteredCompressers.Decompress(compress, bytes.NewReader(data))
		if err != nil {
			return nil, nil, fmt.Errorf("opening decompresser: %s", err.Error())
		}
		defer closer.Close()

		_, err = io.Copy(buf, r)
		if err != nil {
			return nil, nil, fmt.Errorf("decompressing: %s", err.Error())
		}
		decoded = buf.Bytes()
	}

	c2 := crc32.ChecksumIEEE(decoded)
	if crc != c2 {
		return nil, nil, fmt.Errorf("block checksum not matched: cal:%v VS read:%v", crc, c2)
	}

	curr := 0
	block := make(Block, count)
	for i := 0; i < len(block); i++ {
		var row Row
		n := RowBulkLoad(decoded[curr: len(decoded)], &row)
		curr += int(n)
		block[i] = row
	}
	return block, decoded, nil
}

func BlockLoad(r io.Reader) (Block, error) {
	origin := r
	magic := uint16(0)
	err := binary.Read(r, binary.LittleEndian, &magic)
	if err != nil {
		return nil, errors.New("reading block magic flag: " + err.Error())
	}
	if magic != MagicFlag {
		return nil, fmt.Errorf("magic flag not matched: real:%v VS read:%v", MagicFlag, magic)
	}

	compress := CompressType(0)
	err = binary.Read(r, binary.LittleEndian, &compress)
	if err != nil {
		return nil, errors.New("reading block compress type: " + err.Error())
	}

	count := uint32(0)
	err = binary.Read(r, binary.LittleEndian, &count)
	if err != nil {
		return nil, errors.New("reading block size: " + err.Error())
	}

	r, closer, err := RegisteredCompressers.Decompress(compress, r)
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	crc32 := crc32.NewIEEE()
	r = io.TeeReader(r , crc32)

	block := make(Block, count)
	for i := uint32(0); i < count; i++ {
		err = RowLoad(r, &block[i])
		if err != nil {
			return nil, fmt.Errorf("reading block row #%v: %s", i, err.Error())
		}
	}

	c1 := crc32.Sum32()
	c2 := uint32(0)
	err = binary.Read(origin, binary.LittleEndian, &c2)
	if err != nil {
		return nil, errors.New("reading block checksum: " + err.Error())
	}
	if c1 != c2 {
		return nil, fmt.Errorf("block checksum not matched: cal:%v VS read:%v", c1, c2)
	}

	return block, nil
}

type Block []Row

var TimestampNoBound = TimestampBound {TimestampOpenBound, true}

const (
	BufferSizeRead  = 1024 * 64
	BufferSizeWrite = 1024 * 64

	IndexFileSuffix = ".idx"

	MagicFlag = uint16(37492)

	TimestampOpenBound = Timestamp(0)
)

func (self TimestampBound) IsOpen() bool {
	return self.Ts == TimestampOpenBound
}

type TimestampBound struct {
	Ts Timestamp
	Included bool
}

func ToInnerUnit(t Timestamp) Timestamp {
	return t
}
const TimestampLen = 8
type Timestamp uint64
