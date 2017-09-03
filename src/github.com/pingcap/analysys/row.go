package analysys

import (
	"encoding/binary"
	"errors"
	"io"
	"fmt"
	"unsafe"
)

func ByBlock(fun FunOnRow) FunOnBlock {
	sink := BlockSink {fun}
	return sink.OnBlock
}

func (self BlockSink) OnBlock() (blocks chan LoadedBlock, result chan error) {
	blocks = make(chan LoadedBlock)
	result = make(chan error)
	go func() {
		var err error
		for block := range blocks {
			for i, row := range block.Block {
				err = self.OnRow(block.File, block.Order, i, row)
				if err != nil {
					break
				}
			}
			result <-err
		}
	}()
	return blocks, result
}

type BlockSink struct {
	OnRow func(file string, block int, line int, row Row) error
}

func (self *RowCounter) OnRow(file string, block int, line int, row Row) error {
	self.Rows += 1
	if self.fast {
		return nil
	}
	self.users[row.Id] = true
	self.events[row.Event] = true
	self.files[file] = true
	self.blocks[BlockLocation{file, block}] = true
	self.Users = len(self.users)
	self.Events = len(self.events)
	self.Files = len(self.files)
	self.Blocks = len(self.blocks)
	return nil
}

func (self *RowCounter) ByRow() ScanSink {
	return ScanSink {self.OnRow, nil, false}
}

func NewRowCounter(fast bool) *RowCounter {
	return &RowCounter{
		fast: fast,
		users: map[UserId]bool {},
		events: map[EventId]bool {},
		files: map[string]bool {},
		blocks: map[BlockLocation]bool {},
	}
}

type RowCounter struct {
	fast bool
	users map[UserId]bool
	events map[EventId]bool
	files map[string]bool
	blocks map[BlockLocation]bool
	Rows int
	Users int
	Events int
	Files int
	Blocks int
}

type BlockLocation struct {
	File string
	Block int
}

func (self *RowPrinter) Print(file string, block int, line int, row Row) error {
	if self.verify && row.Ts < self.ts {
		return fmt.Errorf("backward timestamp, file:%v block%v line:[%v]", file, block, line)
	}
	self.ts = row.Ts
	if !self.dry && (self.user == 0 || self.user == row.Id) && (self.event == 0 || self.event == row.Event) {
		err := row.Dump(self.w)
		if err != nil {
			return err
		}
		_, err = self.w.Write(Endl)
		if err != nil {
			return err
		}
	}
	return nil
}

func (self RowPrinter) ByRow() ScanSink {
	return ScanSink {self.Print, nil, false}
}

type RowPrinter struct {
	w io.Writer
	ts Timestamp
	user UserId
	event EventId
	verify bool
	dry bool
}

func (self *Row) Write(w io.Writer) (int, error) {
	written := 0
	err := binary.Write(w, binary.LittleEndian, self.Ts)
	if err != nil {
		return written, err
	}
	written += TimestampLen

	err = binary.Write(w, binary.LittleEndian, self.Id)
	if err != nil {
		return written, err
	}
	written += UserIdLen

	err = binary.Write(w, binary.LittleEndian, self.Event)
	if err != nil {
		return written, err
	}
	written += EventIdLen

	err = binary.Write(w, binary.LittleEndian, uint16(len(self.Props)))
	if err != nil {
		return written, err
	}
	written += CbpLen

	_, err = w.Write(self.Props)
	if err != nil {
		return written, errors.New("writing event props: " + err.Error())
	}
	written += len(self.Props)

	if written != self.PersistSize() {
		panic("Row.Write: wrong written size")
	}
	return written, nil
}

func (self *Row) PersistSize() int {
	return TimestampLen + UserIdLen + EventIdLen + CbpLen + len(self.Props)
}

func RowBulkLoad(data []byte, row *Row) int {
	info := (*RowInfo)(unsafe.Pointer(&data[0]))
	dest := (*RowInfo)(unsafe.Pointer(row))
	*dest = *info
	cbi := uint16(unsafe.Sizeof(*info))
	row.Props = data[cbi: cbi + info.Cbp]
	return row.PersistSize()
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

func (self *Row) Dump(w io.Writer) error {
	_, err := fmt.Fprintf(w, "%d %d %d ", self.Ts, self.Id, self.Event)
	if err != nil {
		return err
	}
	_, err = w.Write(self.Props)
	return err
}

type RowInfo struct {
	Ts Timestamp
	Id UserId
	Event EventId
	Cbp uint16
}

type Row struct {
	Ts Timestamp
	Id UserId
	Event EventId
	Props []byte
}

var Endl = []byte("\n")

var UserIdLen = int(unsafe.Sizeof(UserId(0)))
var EventIdLen = int(unsafe.Sizeof(EventId(0)))
var CbpLen = int(unsafe.Sizeof(uint16(0)))

type UserId uint32
type EventId uint16
