package analysys

import (
	"encoding/binary"
	"errors"
	"io"
	"fmt"
)

func (self TraceUsers) Result() map[uint16]int {
	result := map[uint16]int {}
	for _, user := range self.users {
		score := user.score
		if _, ok := result[score]; !ok {
			result[score] = 1
		} else {
			result[score] += 1
		}
	}
	return result
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

func (self TraceUsers) ByBlock() ScanSink {
	return ScanSink {nil, self.OnBlock, true}
}

func (self TraceUsers) ByRow() ScanSink {
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
	if self.score >= uint16(len(self.query.events)) {
		return
	}

	index, ok := self.query.events[event]
	if !ok {
		return
	}
	self.events[index] = append(self.events[index], ts)

	score := uint16(0)
	lower := ts - self.query.window

	for i, _ := range self.query.seq {
		blank := true
		for j, et := range self.events[i] {
			if et > lower {
				lower = et
				blank = false
				score = uint16(i + 1)
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
	score uint16
}

type TraceQuery struct {
	seq []EventId
	events map[EventId]int
	window Timestamp
}

type EventLinks []EventLink
type EventLink []Timestamp

func (self *RowPrinter) Print(file string, block int, line int, row Row) error {
	if self.verify && row.Ts < self.ts {
		return fmt.Errorf("backward timestamp, file:%v block%v line:[%v]", file, block, line)
	}
	self.ts = row.Ts
	if !self.dry {
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
	verify bool
	dry bool
}

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

func (self *Row) Dump(w io.Writer) error {
	_, err := fmt.Fprintf(w, "%d %d %d ", self.Ts, self.Id, self.Event)
	if err != nil {
		return err
	}
	_, err = w.Write(self.Props)
	return err
}

type Row struct {
	Ts Timestamp
	Id UserId
	Event EventId
	Props []byte
}

type UserId uint32
type EventId uint16

var Endl = []byte("\n")
