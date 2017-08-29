package analysys

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"strconv"
	"github.com/pingcap/analysys/tools"
)

func Build(in, out string, compress string, gran int) error {
	rows, err := Load(in)
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

func Load(in string) (Rows, error) {
	rows := make(Rows, 0)
	err := tools.IterLines(in, BufferSizeRead, func(line []byte) error {
		row, err := Parse(line)
		rows.Add(row)
		return err
	})
	return rows, err
}

func Write(rows Rows, out string, compress string, gran int) (Index, error) {
	f, err := os.OpenFile(out, os.O_RDWR | os.O_CREATE | os.O_EXCL, 0644)
	if err != nil {
		return nil, errors.New("writing sorted data: " + err.Error())
	}
	w := bufio.NewWriterSize(f, BufferSizeWrite)

	offset := uint32(0)
	index := make(Index, (len(rows) + gran - 1) / gran)
	for i := 0; len(rows) != 0; i++ {
		count := gran
		if len(rows) < gran {
			count = len(rows)
		}
		block := Block(rows[0: count])
		n, err := block.Write(w)
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

type IndexEntry struct {
	Ts Timestamp
	Offset uint32
}

type Index []IndexEntry

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

type Row struct {
	Ts Timestamp
	Id uint32
	Event uint16
	Props []byte
}

func (self *Row) PersistSize() uint32 {
	return uint32(TimestampLen + 4 + 2 + 2 + len(self.Props))
}

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
		return 0, err
	}
	return self.PersistSize(), nil
}

type Rows []Row

type Block []Row

func (self Block) Write(w io.Writer) (uint32, error) {
	err := binary.Write(w, binary.LittleEndian, uint32(len(self)))
	if err != nil {
		return 0, errors.New("writing block size: " + err.Error())
	}

	written := uint32(4)
	crc32 := crc32.NewIEEE()
	w = io.MultiWriter(crc32, w)

	for _, row := range self {
		n, err := row.Write(w)
		if err != nil {
			return written, errors.New("writing block row: " + err.Error())
		}
		written += n
		err = binary.Write(w, binary.LittleEndian, crc32.Sum32())
		if err != nil {
			return written, errors.New("writing block checksum: " + err.Error())
		}
		written += 4
	}
	return written, nil
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

var (
	PropsBeginMark = []byte("{")
	PropsEndMark   = []byte("}")
)

const (
	BufferSizeRead  = 1024 * 1024
	BufferSizeWrite = 1024 * 1024
	IndexFileSuffix = ".idx"
	TimestampLen = 4
)

type Timestamp uint32

