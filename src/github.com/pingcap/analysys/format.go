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
	"sort"
	"strconv"
	"github.com/pingcap/analysys/tools"
)

func Build(in, out string, compress string, gran, align int) error {
	rows, err := OriginLoad(in)
	if err != nil {
		return err
	}

	sort.Sort(rows)

	index, err := DataWrite(rows, out, compress, gran, align)
	if err != nil {
		return err
	}
	return index.Write(out + IndexFileSuffix);
}

func OriginLoad(path string) (Rows, error) {
	rows := make(Rows, 0)
	err := tools.IterLines(path, BufferSizeRead, func(line []byte) error {
		row, err := OriginParse(line)
		rows.Add(row)
		return err
	})
	return rows, err
}

func OriginParse(line []byte) (row Row, err error) {
	i := bytes.Index(line, PropsBeginMark)
	if i < 0 {
		err = errors.New("props begin mark not found: " + string(line))
		return
	}
	j := bytes.LastIndex(line, PropsEndMark)
	if j < 0 {
		err = errors.New("props end mark not found: " + string(line))
		return
	}

	props := make([]byte, j + 1 - i)
	copy(props, line[i: j + 1])
	line = line[0: i]

	fields := bytes.Fields(line)
	if len(fields) != 4 {
		err = errors.New("field number not matched: " + string(line))
		return
	}

	id, err := strconv.ParseUint(string(fields[0]), 10, 32)
	if err != nil {
		return
	}
	ts, err := strconv.ParseUint(string(fields[1]), 10, 64)
	if err != nil {
		return
	}
	event, err := strconv.ParseUint(string(fields[2]), 10, 16)
	if err != nil {
		return
	}

	row.Id = uint32(id)
	row.Ts = Timestamp(ts / 1000)
	row.Event = uint16(event)
	row.Props = props
	return row, err
}

func DataWrite(rows Rows, path string, compress string, gran, align int) (Index, error) {
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

	for i := 0; len(rows) != 0; i++ {
		count := gran
		if len(rows) < gran {
			count = len(rows)
		}

		if align > 0 && (int(offset) % align != 0) {
			n := align - int(offset) % align
			_, err = w.Write(padding[0: n])
			if err != nil {
				return nil, errors.New("writing padding: " + strconv.Itoa(n) + ", " + err.Error())
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
	}

	err = w.Flush()
	return index, err
}

func DataDump(path string, w io.Writer) error {
	indexing, err := IndexingLoad(path)
	if err != nil {
		return err
	}
	ts := Timestamp(0)
	for i, _ := range(indexing.Index) {
		block, err := indexing.Load(i)
		if err != nil {
			return err
		}
		for j, row := range block {
			s := row.String()
			_, err = w.Write([]byte(s + "\n"))
			if err != nil {
				return err
			}
			if row.Ts < ts {
				return errors.New("backward timestamp: block:" +
					strconv.Itoa(i) + ", row:" + strconv.Itoa(j) + " " + s)
			}
			ts = row.Ts
		}
	}
	return nil
}

func IndexDump(path string, w io.Writer) error {
	index, err := IndexLoad(path)
	for _, entry := range index {
		_, err := w.Write([]byte(fmt.Sprintf("%v %v\n", entry.Ts, entry.Offset)))
		if err != nil {
			return err
		}
	}
	return err
}

func IndexToBlockCheck(path string, w io.Writer) error {
	indexing, err := IndexingLoad(path)
	if err != nil {
		return err
	}
	for i, _ := range(indexing.Index) {
		_, err := indexing.Load(i)
		if err != nil {
			return err
		}
	}
	return nil
}

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

func (self *Indexing) Close() error {
	return self.file.Close()
}

func (self *Indexing) Load(i int) (Block, error) {
	if i >= len(self.Index) {
		return nil, errors.New("indexing load block: #" +
			strconv.Itoa(i) + " >= " + strconv.Itoa(len(self.Index)))
	}
	entry := self.Index[i]
	end := self.total
	if i + 1 != len(self.Index) {
		end = int64(self.Index[i + 1].Offset)
	}
	r := io.NewSectionReader(self.file, int64(entry.Offset), end - int64(entry.Offset))
	return BlockLoad(bufio.NewReaderSize(r, BufferSizeRead))
}

type Indexing struct {
	Index Index
	file *os.File
	total int64
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
		return index, errors.New("index checksum not matched: cal:" +
			strconv.Itoa(int(c1)) + " VS read:" + strconv.Itoa(int(c2)))
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

	ct, err := GetCompressType(compress)
	if err != nil {
		return written, err
	}
	err = binary.Write(w, binary.LittleEndian, ct)
	if err != nil {
		return written, errors.New("writing block compress type: " + err.Error())
	}
	written += 2

	buf := bytes.NewBuffer(nil)
	w, closer, err := Compress(ct, buf)
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
		return nil, errors.New("magic flag not matched: real:" +
			strconv.Itoa(int(MagicFlag)) + " VS read:" + strconv.Itoa(int(magic)))
	}

	compress := CompressType(0)
	err = binary.Read(r, binary.LittleEndian, &compress)
	if err != nil {
		return nil, errors.New("reading block compress type: " + err.Error())
	}

	w, closer, err := Decompress(compress, r)
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
		return nil, errors.New("block checksum not matched: cal:" +
			strconv.Itoa(int(c1)) + " VS read:" + strconv.Itoa(int(c2)))
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
	err := binary.Write(w, binary.LittleEndian, self.Ts)
	if err != nil {
		return 0, err
	}
	err = binary.Write(w, binary.LittleEndian, self.Id)
	if err != nil {
		return 0, err
	}
	err = binary.Write(w, binary.LittleEndian, self.Event)
	if err != nil {
		return 0, err
	}
	err = binary.Write(w, binary.LittleEndian, uint16(len(self.Props)))
	if err != nil {
		return 0, err
	}
	_, err = w.Write(self.Props)
	if err != nil {
		return 0, errors.New("writing event props: " + err.Error())
	}
	return self.PersistSize(), nil
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
		err = errors.New("reading event props: " + strconv.Itoa(int(cbp))  + ", " + err.Error())
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

var (
	PropsBeginMark = []byte("{")
	PropsEndMark   = []byte("}")
)

const (
	BufferSizeRead  = 1024 * 1024
	BufferSizeWrite = 1024 * 1024
	IndexFileSuffix = ".idx"
	TimestampLen = 4
	MagicFlag = uint16(37492)
)

type Timestamp uint32
