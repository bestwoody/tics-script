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

func Build(in, out string, compress string, gran int) error {
	rows, err := LoadOrigin(in)
	if err != nil {
		return err
	}

	sort.Sort(rows)

	index, err := Write(rows, out, compress, gran)
	if err != nil {
		return err
	}
	return index.Write(out + IndexFileSuffix);
}

func LoadOrigin(path string) (Rows, error) {
	rows := make(Rows, 0)
	err := tools.IterLines(path, BufferSizeRead, func(line []byte) error {
		row, err := Parse(line)
		rows.Add(row)
		return err
	})
	return rows, err
}

func Parse(line []byte) (row Row, err error) {
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

func Write(rows Rows, path string, compress string, gran int) (Index, error) {
	f, err := os.OpenFile(path, os.O_RDWR | os.O_CREATE | os.O_EXCL, 0644)
	if err != nil {
		return nil, errors.New("writing sorted data: " + err.Error())
	}
	w := bufio.NewWriterSize(f, BufferSizeWrite)

	offset := uint32(0)
	count := (len(rows) + gran - 1) / gran
	index := make(Index, count)

	err = binary.Write(w, binary.LittleEndian, uint16(count))
	if err != nil {
		return nil, errors.New("writing block count: " + err.Error())
	}

	for i := 0; len(rows) != 0; i++ {
		count := gran
		if len(rows) < gran {
			count = len(rows)
		}
		block := Block(rows[0: count])
		// TODO: do real compress
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

func Dump(path string, w io.Writer) error {
	rows, err := Load(path)
	if err != nil {
		return err
	}

	ts := Timestamp(0)
	for i, row := range rows {
		s := row.String()
		_, err = w.Write([]byte(s + "\n"))
		if err != nil {
			return err
		}
		if row.Ts < ts {
			return errors.New("backward timestamp: #" + strconv.Itoa(i) + " " + s)
		}
		ts = row.Ts
	}
	return nil
}

func Load(path string) (Rows, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("reading data: " + err.Error())
	}
	r := bufio.NewReaderSize(f, BufferSizeRead)

	count := uint16(0)
	err = binary.Read(r, binary.LittleEndian, &count)
	if err != nil {
		return nil, errors.New("reading block count: " + err.Error())
	}

	rows := make(Rows, 0)
	for i := uint16(0); i < count; i++ {
		block, err := LoadBlock(r)
		if err != nil {
			return nil, err
		}
		if block == nil {
			break
		}
		rows = append(rows, block...)
	}

	return rows, nil
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
		return errors.New("writing index size: " + err.Error())
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
	return err
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
	crc32 := crc32.NewIEEE()
	w = io.MultiWriter(crc32, w)
	written := uint32(4)

	err := binary.Write(w, binary.LittleEndian, MagicFlag)
	if err != nil {
		return 0, errors.New("writing block magic flag: " + err.Error())
	}
	written += 2

	ct := GetCompressType(compress)
	err = binary.Write(w, binary.LittleEndian, ct)
	if err != nil {
		return 0, errors.New("writing block compress type: " + err.Error())
	}
	written += 2

	err = binary.Write(w, binary.LittleEndian, uint32(len(self)))
	if err != nil {
		return 0, errors.New("writing block size: " + err.Error())
	}
	written += 4

	for _, row := range self {
		n, err := row.Write(w)
		if err != nil {
			return written, errors.New("writing block row: " + err.Error())
		}
		written += n
	}

	err = binary.Write(w, binary.LittleEndian, crc32.Sum32())
	if err != nil {
		return written, errors.New("writing block checksum: " + err.Error())
	}
	written += 4
	return written, nil
}

func LoadBlock(r io.Reader) (Block, error) {
	crc32 := crc32.NewIEEE()
	r = io.TeeReader(r, crc32)

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

	count := uint32(0)
	err = binary.Read(r, binary.LittleEndian, &count)
	if err != nil {
		return nil, errors.New("reading block size: " + err.Error())
	}

	block := make(Block, count)
	for i := uint32(0); i < count; i++ {
		err = LoadRow(r, &block[i])
		if err != nil {
			return nil, errors.New("reading block row: " + err.Error())
		}
	}

	c1 := crc32.Sum32()
	c2 := uint32(0)
	err = binary.Read(r, binary.LittleEndian, &c2)
	if err != nil {
		return nil, errors.New("reading block checksum: " + err.Error())
	}

	if c1 != c2 {
		return nil, errors.New("block checksum not matched: cal:" +
			strconv.Itoa(int(c1)) + " VS read:" + strconv.Itoa(int(c2)))
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

func LoadRow(r io.Reader, row *Row) error {
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
	props := make([]byte, cbp)
	_, err = io.ReadFull(r, props)
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
