package analysys

import (
	"encoding/binary"
	"errors"
	"io"
	"fmt"
	"unsafe"
)

func (self TraceUsers) Result() map[int]ScoredUsers {
	result := map[int]ScoredUsers {}
	for i, _ := range self.query.seq {
		result[i] = ScoredUsers {0, 0}
	}
	for _, user := range self.users {
		score := user.score
		users := result[score]
		result[score] = ScoredUsers {users.Val + 1, 0}
	}
	acc := 0
	for i := len(self.query.seq); i >= 0; i-- {
		users := result[i]
		acc += users.Val
		result[i] = ScoredUsers {users.Val, acc}
	}
	return result
}

type ScoredUsers struct {
	Val int
	Acc int
}

func (self *TraceUsers) OnRow(file string, block int, line int, row Row) error {
	user, ok := self.users[row.Id]
	if !ok {
		user = NewTraceUser(self.query)
		self.users[row.Id] = user
	}
	user.OnEvent(row.Ts, row.Event)
	return nil
}

func (self *TraceUsers) OnBlock() (blocks chan LoadedBlock, result chan error) {
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

func (self *TraceUsers) ByBlock() ScanSink {
	return ScanSink {nil, self.OnBlock, true}
}

func (self *TraceUsers) ByRow() ScanSink {
	return ScanSink {self.OnRow, nil, false}
}

func NewTraceUsers(events []EventId, window Timestamp) (*TraceUsers, error) {
	tq := &TraceQuery {events, map[EventId]int{}, window}
	for i, event := range events {
		tq.events[event] = i
	}
	if len(tq.events) != len(events) {
		return nil, fmt.Errorf("duplicated event: %v", events)
	}
	return &TraceUsers {tq, map[UserId]*TraceUser {}}, nil
}

type TraceUsers struct {
	query *TraceQuery
	users map[UserId]*TraceUser
}

func (self *TraceUser) OnEvent(ts Timestamp, event EventId) {
	if self.score >= len(self.query.events) {
		return
	}

	index, ok := self.query.events[event]
	if !ok {
		return
	}
	self.events[index] = append(self.events[index], ts)

	score := 0
	lower := ts - self.query.window

	for i, _ := range self.query.seq {
		blank := true
		for j, et := range self.events[i] {
			if et > lower {
				lower = et
				blank = false
				score = i + 1
				if j != 0 {
					self.events[i] = self.events[i][j:]
				}
				break
			}
		}
		if blank {
			self.events[i] = nil
			break
		}
	}

	if score > self.score {
		self.score = score
	}
}

func NewTraceUser(query *TraceQuery) *TraceUser {
	return &TraceUser {query, make(EventLinks, len(query.events)), 0}
}

type TraceUser struct {
	query *TraceQuery
	events EventLinks
	score int
}

type TraceQuery struct {
	seq []EventId
	events map[EventId]int
	window Timestamp
}

type EventLinks []EventLink
type EventLink []Timestamp

func (self *RowCounter) OnRow(file string, block int, line int, row Row) error {
	self.users[row.Id] = true
	self.events[row.Event] = true
	self.files[file] = true
	self.blocks[BlockLocation{file, block}] = true
	self.Rows += 1
	self.Users = len(self.users)
	self.Events = len(self.events)
	self.Files = len(self.files)
	self.Blocks = len(self.blocks)
	return nil
}

func (self *RowCounter) ByRow() ScanSink {
	return ScanSink {self.OnRow, nil, false}
}

func NewRowCounter() *RowCounter {
	return &RowCounter{
		users: map[UserId]bool {},
		events: map[EventId]bool {},
		files: map[string]bool {},
		blocks: map[BlockLocation]bool {},
	}
}

type RowCounter struct {
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
	cbi := int(unsafe.Sizeof(*info))
	row.Props = data[cbi: info.Cbp]
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
