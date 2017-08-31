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

func (self *Indexing) Load(i int) (Block, error) {
	if i >= len(self.Index) - 1 {
		return nil, fmt.Errorf("indexing load block: #%v >= %v", i, len(self.Index))
	}
	entry := self.Index[i]
	end := int64(self.Index[i + 1].Offset)
	r := io.NewSectionReader(self.file, int64(entry.Offset), end - int64(entry.Offset))
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

		n, err := block.Write(w, compress)
		if err != nil {
			return nil, err
		}
		rows = rows[count: len(rows)]
		index[i] = IndexEntry {block[0].Ts, offset}
		offset += n
		ts = block[len(block) - 1].Ts
	}

	index = append(index, IndexEntry {ts, offset})

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

func (self Block) Write(w io.Writer, compress string) (uint32, error) {
	dest := w
	written := uint32(0)

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

	buf := bytes.NewBuffer(nil)
	w, closer, err := RegisteredCompressers.Compress(ct, buf)
	if err != nil {
		return written, err
	}

	crc32 := crc32.NewIEEE()
	w = io.MultiWriter(crc32, w)

	err = binary.Write(w, binary.LittleEndian, uint32(len(self)))
	if err != nil {
		return 0, errors.New("writing block size: " + err.Error())
	}
	for _, row := range self {
		_, err := row.Write(w)
		if err != nil {
			return written, errors.New("writing block row: " + err.Error())
		}
	}
	crc := crc32.Sum32()
	err = binary.Write(w, binary.LittleEndian, crc)
	if err != nil {
		return written, errors.New("writing block checksum: " + err.Error())
	}

	if closer != nil {
		err = closer.Close()
		if err != nil {
			return written, errors.New("closing block compresser: " + err.Error())
		}
	}

	n, err := io.Copy(dest, buf)
	if err != nil {
		return written, errors.New("writing compressed data: " + err.Error())
	}
	written += uint32(n)

	return written, err
}

func BlockLoad(r io.Reader) (Block, error) {
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

	w, closer, err := RegisteredCompressers.Decompress(compress, r)
	if err != nil {
		return nil, err
	}
	crc32 := crc32.NewIEEE()
	w = io.TeeReader(w , crc32)

	count := uint32(0)
	err = binary.Read(w, binary.LittleEndian, &count)
	if err != nil {
		return nil, errors.New("reading block size: " + err.Error())
	}

	block := make(Block, count)
	for i := uint32(0); i < count; i++ {
		err = RowLoad(w, &block[i])
		if err != nil {
			return nil, errors.New("reading block row: " + err.Error())
		}
	}

	c1 := crc32.Sum32()
	c2 := uint32(0)
	err = binary.Read(w, binary.LittleEndian, &c2)
	if err != nil {
		return nil, errors.New("reading block checksum: " + err.Error())
	}
	if c1 != c2 {
		return nil, fmt.Errorf("block checksum not matched: cal:%v VS read:%v", c1, c2)
	}

	if closer != nil {
		err = closer.Close()
		if err != nil {
			return nil, errors.New("closing block decompresser: " + err.Error())
		}
	}

	return block, nil
}

type Block []Row

func (self *Row) Write(w io.Writer) (uint32, error) {
	written := uint32(0)
	err := binary.Write(w, binary.LittleEndian, self.Ts)
	if err != nil {
		return written, err
	}
	written += TimestampLen

	err = binary.Write(w, binary.LittleEndian, self.Id)
	if err != nil {
		return written, err
	}
	written += 4

	err = binary.Write(w, binary.LittleEndian, self.Event)
	if err != nil {
		return written, err
	}
	written += 2

	err = binary.Write(w, binary.LittleEndian, uint16(len(self.Props)))
	if err != nil {
		return written, err
	}
	written += 2

	_, err = w.Write(self.Props)
	if err != nil {
		return written, errors.New("writing event props: " + err.Error())
	}
	written += uint32(len(self.Props))

	if written != self.PersistSize() {
		panic("Row.Write: wrong written size")
	}
	return written, nil
}

func (self *Row) PersistSize() uint32 {
	return uint32(TimestampLen + 4 + 2 + 2 + len(self.Props))
}

func RowLoad(r io.Reader, row *Row) error {
	err := binary.Read(r, binary.LittleEndian, &row.Ts)
	if err != nil {
		return err
	}
	err = binary.Read(r, binary.LittleEndian, &row.Id)
	if err != nil {
		return err
	}
	err = binary.Read(r, binary.LittleEndian, &row.Event)
	if err != nil {
		return err
	}
	cbp := uint16(0)
	err = binary.Read(r, binary.LittleEndian, &cbp)
	if err != nil {
		return err
	}
	row.Props = make([]byte, cbp)
	_, err = io.ReadFull(r, row.Props)
	if err != nil {
		err = fmt.Errorf("reading event props, size:%v %s", cbp, err.Error())
	}
	return err
}

func (self *Row) String() string {
	return fmt.Sprintf("%v %v %v %s", self.Ts, self.Id, self.Event, string(self.Props))
}

type Row struct {
	Ts Timestamp
	Id uint32
	Event uint16
	Props []byte
}

var TimestampNoBound = TimestampBound {TimestampOpenBound, true}

const (
	BufferSizeRead  = 1024 * 64
	BufferSizeWrite = 1024 * 64

	IndexFileSuffix = ".idx"

	MagicFlag = uint16(37492)

	TimestampLen = 4
	TimestampOpenBound = Timestamp(0)
)

func (self TimestampBound) IsOpen() bool {
	return self.Ts == TimestampOpenBound
}

type TimestampBound struct {
	Ts Timestamp
	Included bool
}

type Timestamp uint32
