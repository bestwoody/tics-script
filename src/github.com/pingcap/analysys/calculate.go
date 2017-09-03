package analysys

import (
	"encoding/binary"
	"errors"
	"io"
	"fmt"
	"unsafe"
)

func (self PageTraceUsers) Result(result AccResult) {
	for _, page := range self.pages {
		page.Result(self.usersPerPage, result)
	}
	result.CalAcc(len(self.query.seq))
}

func (self *PageTraceUsers) OnRow(file string, block int, line int, row Row) error {
	eventIndex, ok := self.query.events[row.Event]
	if !ok {
		return nil
	}

	pageIndex := int(row.Id) / self.usersPerPage
	if pageIndex >= len(self.pages) {
		self.pages = append(self.pages, make([]*PageUsers, pageIndex + 1 - len(self.pages))...)
	}

	page := self.pages[pageIndex]
	if page == nil {
		page = NewPageUsers(UserId(pageIndex * self.usersPerPage),
			self.usersPerPage, self.userIdInterval, self.eventsMaxLen, self.query)
		self.pages[pageIndex] = page
	}

	err := page.OnEvent(row.Id, row.Ts, eventIndex)
	if err == ErrRingBufferOverflow {
		// TODO: save to exception
	}
	return err
}

func (self *PageTraceUsers) OnBlock() (blocks chan LoadedBlock, result chan error) {
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

func (self *PageTraceUsers) ByBlock() ScanSink {
	return ScanSink {nil, self.OnBlock, true}
}

func (self *PageTraceUsers) ByRow() ScanSink {
	return ScanSink {self.OnRow, nil, false}
}

// TODO: sampling
func NewPageTraceUsers(
	events []EventId,
	window Timestamp,
	userIdInterval int,
	usersPerPage int,
	eventsMaxLen int) (*PageTraceUsers, error) {

	tq := &TraceQuery {events, map[EventId]int{}, window}
	for i, event := range events {
		tq.events[event] = i
	}
	if len(tq.events) != len(events) {
		return nil, fmt.Errorf("duplicated event: %v", events)
	}
	return &PageTraceUsers {
		tq,
		make([]*PageUsers, 0),
		userIdInterval,
		usersPerPage,
		eventsMaxLen,
	}, nil
}

type PageTraceUsers struct {
	query *TraceQuery
	pages []*PageUsers
	userIdInterval int
	usersPerPage int
	eventsMaxLen int
}

func (self *PageUsers) Result(usersPerPage int, result AccResult) {
	for i := 0; i < usersPerPage; i ++ {
		user := self.user(i)
		if user.id != 0 {
			result.Increase(int(user.score))
		}
	}
}

func (self *PageUsers) OnEvent(id UserId, ts Timestamp, event int) error {
	userIndex := int(id - self.start) / self.userIdInterval
	user := self.user(userIndex)
	if user.id == 0 {
		user.id = id
	} else if user.id != id {
		return fmt.Errorf("wrong page write, should never happen")
	}
	if int(user.score) >= len(self.query.events) {
		return nil
	}

	ring := self.ring(userIndex, event)
	overflow := RingBufferAdd(ring, self.event(userIndex, event), ts)
	if overflow {
		return ErrRingBufferOverflow
	}

	score := uint16(0)
	lower := ts - self.query.window
	blank := false

	for i, _ := range self.query.seq {
		lower, blank = RingBufferPruge(ring, self.event(userIndex, i), lower)
		if !blank {
			score = uint16(i) + 1
		} else {
			break
		}
	}

	if score > user.score {
		user.score = score
	}
	return nil
}

var ErrRingBufferOverflow = errors.New("ring buffer overflow")

func (self *PageUsers) user(user int) *PageUser {
	return (*PageUser)(unsafe.Pointer(&self.data[user]))
}

func (self *PageUsers) ring(user int, event int) *PageEventRing {
	return (*PageEventRing)(unsafe.Pointer(&self.data[user + PageUserLen]))
}

func (self *PageUsers) event(user int, event int) unsafe.Pointer {
	return unsafe.Pointer(&self.data[user + PageUserLen + self.unitEvent * event])
}

func (self *PageUsers) Alloc() {
	if self.data == nil {
		self.data = make([]byte, self.usersPerPage * self.unitUser)
	}
}

func RingBufferAdd(ring *PageEventRing, buf unsafe.Pointer, ts Timestamp) (overflow bool) {
	// TODO
	return
}

func RingBufferPruge(ring *PageEventRing, buf unsafe.Pointer, lower Timestamp) (newLower Timestamp, blank bool) {
	// TODO
	return
}

type PageEventRing struct {
	start int16
	end int16
}
const PageEventRingLen = 4

func NewPageUsers(start UserId, usersPerPage int, userIdInterval int, eventsMaxLen int, query *TraceQuery) *PageUsers {
	unitEvent := eventsMaxLen * TimestampLen + PageEventRingLen
	return &PageUsers {
		start,
		usersPerPage,
		userIdInterval,
		eventsMaxLen,
		unitEvent * len(query.seq) + PageUserLen,
		unitEvent,
		query,
		nil,
	}
}

const PageUserLen = 8

type PageUser struct {
	id UserId
	score uint16
	reserved uint16
}

type PageUsers struct {
	start UserId
	usersPerPage int
	userIdInterval int
	eventsMaxLen int
	unitUser int
	unitEvent int
	query *TraceQuery
	data []byte
}

func (self TraceUsers) Result(result AccResult) {
	for _, user := range self.users {
		result.Increase(int(user.score))
	}
	result.CalAcc(len(self.query.seq))
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

func (self AccResult) CalAcc(max int) {
	acc := 0
	for i := max; i >= 0; i-- {
		users := self[i]
		acc += users.Val
		self[i] = ScoredUsers {users.Val, acc}
	}
}

func (self AccResult) Increase(score int) {
	self[score] = ScoredUsers {self[score].Val + 1, 0}
}

func NewAccResult(max int) AccResult {
	result := make(AccResult)
	for i := 0; i <= max; i++ {
		result[i] = ScoredUsers {0, 0}
	}
	return result
}

type AccResult map[int]ScoredUsers

type ScoredUsers struct {
	Val int
	Acc int
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
				score = uint16(i) + 1
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
