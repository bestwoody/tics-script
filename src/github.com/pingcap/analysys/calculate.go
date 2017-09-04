package analysys

import (
	"errors"
	"fmt"
	"unsafe"
)

func (self PagedCalc) Result(result AccResult) {
	if self.samples != nil {
		self.sampleCalc.Result(result)
		return
	}
	for _, page := range self.pages {
		if page != nil {
			page.Result(self.usersPerPage, result)
		}
	}
	result.CalAcc(len(self.query.seq))
}

func (self *PagedCalc) OnRow(file string, block int, line int, row Row) error {
	eventIndex := self.query.EventIndex(row.Event)
	if eventIndex < 0 {
		return nil
	}

	if self.eventsMaxLen > 0 {
		return self.handle(file, block, line, row, eventIndex)
	}

	err := self.sampleCalc.OnRow(file, block, line, row)
	if err != nil {
		return err
	}

	self.samples = append(self.samples, row)
	self.sampleCalc.OnRow(file, block, line, row)

	if self.sampleCalc.sampling.Count >= MaxSamplingCount ||
		(self.sampleCalc.sampling.Count > 0 && len(self.samples) > MaxSamplesCount) {

		self.eventsMaxLen = int(float64(self.sampleCalc.sampling.Max) * 1.5)
		if self.eventsMaxLen < EventsMinLen {
			self.eventsMaxLen = EventsMinLen
		}
		self.samples = nil
		self.sampleCalc = nil

		var err error
		for _, row := range self.samples {
			err = self.handle("", -1, -1, row, self.query.EventIndex(row.Event))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (self *PagedCalc) handle(file string, block int, line int, row Row, event int) error {
	accepted, err := self.tooLongs.TryAccept(file, block, line, row)
	if err != nil {
		return err
	}
	if accepted {
		return nil
	}

	pageIndex := int(row.Id) / self.usersPerPage
	if pageIndex >= len(self.pages) {
		self.pages = append(self.pages, make([]*Page, pageIndex + 1 - len(self.pages))...)
	}

	page := self.pages[pageIndex]
	if page == nil {
		page = NewPage(UserId(pageIndex * self.usersPerPage),
			self.usersPerPage, self.userIdInterval, self.eventsMaxLen, self.query)
		self.pages[pageIndex] = page
	}

	err = page.OnEvent(row.Id, row.Ts, event)
	if err == ErrRingBufferOverflow {
		events := page.Detach(row.Id, event)
		self.tooLongs.Add(row.Id, event, events)
	}
	return err
}

func (self *PagedCalc) ByBlock() ScanSink {
	return ScanSink {nil, ByBlock(self.OnRow), true}
}

func (self *PagedCalc) ByRow() ScanSink {
	return ScanSink {self.OnRow, nil, false}
}

func NewPagedCalc(
	query *CalcQuery,
	userIdInterval int,
	usersPerPage int,
	eventsMaxLen int) *PagedCalc {

	var samples []Row
	var sampleCalc *BaseCalc
	if eventsMaxLen < 0 {
		samples = make([]Row, 0, MaxSamplesCount)
		sampleCalc = NewBaseCalc(query, true)
	}

	return &PagedCalc {
		query,
		make([]*Page, 0),
		userIdInterval,
		usersPerPage,
		eventsMaxLen,
		NewTooLongsCalc(query, usersPerPage),
		samples,
		sampleCalc,
	}
}

const MaxSamplingCount = 1024 * 8
const MaxSamplesCount = 1024 * 64
const EventsMinLen = 4

type PagedCalc struct {
	query *CalcQuery
	pages []*Page
	userIdInterval int
	usersPerPage int
	eventsMaxLen int
	tooLongs *TooLongsCalc
	samples []Row
	sampleCalc *BaseCalc
}

func (self *TooLongsCalc) TryAccept(file string, block int, line int, row Row) (bool, error) {
	pageIndex := int(row.Id) / self.usersPerPage
	if pageIndex >= len(self.pages) {
		return false, nil
	}
	if self.pages[pageIndex][int(row.Id) - pageIndex] {
		return false, nil
	}
	err := self.base.OnRow(file, block, line, row)
	return true, err
}

func (self *TooLongsCalc) Add(id UserId, event int, events []Timestamp) {
	pageIndex := int(id) / self.usersPerPage
	if pageIndex >= len(self.pages) {
		self.pages = append(self.pages, make([][]bool, pageIndex + 1 - len(self.pages))...)
	}
	self.pages[pageIndex][int(id) - pageIndex] = true
	self.base.Merge(id, event, events)
}

func NewTooLongsCalc(query *CalcQuery, usersPerPage int) *TooLongsCalc {
	return &TooLongsCalc {NewBaseCalc(query, false), usersPerPage, nil}
}

type TooLongsCalc struct {
	base *BaseCalc
	usersPerPage int
	pages [][]bool
}

func (self *Page) Detach(id UserId, event int) []Timestamp {
	userIndex := int(id - self.start) / self.userIdInterval
	user := self.user(userIndex)
	ring := self.ring(userIndex, event)
	events := RingBufferDump(ring, uint16(self.eventsMaxLen))
	user.id = 0
	return events
}

func (self *Page) Result(usersPerPage int, result AccResult) {
	for i := 0; i < usersPerPage; i ++ {
		user := self.user(i)
		if user.id != 0 {
			result.Increase(int(user.score))
		}
	}
}

func (self *Page) OnEvent(id UserId, ts Timestamp, event int) error {
	userIndex := int(id - self.start) / self.userIdInterval
	user := self.user(userIndex)
	if user.id == 0 {
		user.id = id
	} else if user.id != id {
		return fmt.Errorf("wrong page write, should never happen. %v VS %v", user.id, id)
	}
	if int(user.score) >= len(self.query.seq) {
		return nil
	}

	ring := self.ring(userIndex, event)
	overflow := RingBufferAdd(ring, uint16(self.eventsMaxLen), ts)
	if overflow {
		return ErrRingBufferOverflow
	}

	score := uint16(0)
	lower := ts - self.query.window
	blank := false

	for i, _ := range self.query.seq {
		ring := self.ring(userIndex, i)
		lower, blank = RingBufferPruge(ring, uint16(self.eventsMaxLen), lower)
		if blank {
			break
		}
		score = uint16(i) + 1
	}

	if score > user.score {
		user.score = score
	}
	return nil
}

func (self *Page) user(user int) *PageUser {
	return (*PageUser)(unsafe.Pointer(&self.data[user * self.unitUser]))
}

func (self *Page) ring(user int, event int) (*PageEventRing) {
	return (*PageEventRing)(unsafe.Pointer(&self.data[user * self.unitUser + PageUserLen + event * self.unitEvent]))
}

func NewPage(start UserId, usersPerPage int, userIdInterval int, eventsMaxLen int, query *CalcQuery) *Page {
	unitEvent := PageEventRingLen + eventsMaxLen * TimestampLen
	unitUser := PageUserLen + unitEvent * len(query.seq)
	return &Page {
		start,
		usersPerPage,
		userIdInterval,
		eventsMaxLen,
		unitEvent,
		unitUser,
		query,
		make([]byte, usersPerPage * unitUser),
	}
}

type Page struct {
	start UserId
	usersPerPage int
	userIdInterval int
	eventsMaxLen int
	unitEvent int
	unitUser int
	query *CalcQuery
	data []byte
}

func RingBufferDump(ring *PageEventRing, size uint16) ([]Timestamp) {
	result := make([]Timestamp, ring.count)
	data := (*[MaxRingBufferSize]Timestamp)(unsafe.Pointer(uintptr(unsafe.Pointer(ring)) + PageEventRingLen))
	pos := ring.start
	for i := uint16(0); i < ring.count; i++ {
		if pos >= size {
			pos -= size
		}
		result[i] = data[pos]
		pos += 1
	}
	return result
}

func RingBufferAdd(ring *PageEventRing, size uint16, ts Timestamp) (overflow bool) {
	if ring.count >= size {
		return true
	}
	p := uintptr(unsafe.Pointer(ring)) + uintptr(PageEventRingLen + (ring.start + ring.count) % size * TimestampLen)
	*((*Timestamp)(unsafe.Pointer(p))) = ts
	ring.count += 1
	return
}

func RingBufferPruge(ring *PageEventRing, size uint16, lower Timestamp) (newLower Timestamp, blank bool) {
	data := (*[MaxRingBufferSize]Timestamp)(unsafe.Pointer(uintptr(unsafe.Pointer(ring)) + PageEventRingLen))
	blank = true
	count := ring.count
	pos := ring.start

	for i := uint16(0); i < count; i++ {
		if pos >= size {
			pos -= size
		}
		// TODO: > or >= ?
		if data[pos] > lower {
			lower = data[pos]
			blank = false
			ring.start = (ring.start + i) % size
			ring.count -= i
		}
		pos += 1
	}

	if blank {
		ring.count = 0
		ring.start = 0
	}
	return lower, ring.count == 0
}

const MaxRingBufferSize = 1024
var ErrRingBufferOverflow = errors.New("ring buffer overflow")

type PageEventRing struct {
	start uint16
	count uint16
}
const PageEventRingLen = 4

type PageUser struct {
	id UserId
	score uint16
	reserved uint16
}
const PageUserLen = 8

func (self BaseCalc) Result(result AccResult) {
	for _, user := range self.users {
		result.Increase(int(user.score))
	}
	result.CalAcc(len(self.query.seq))
}

func (self *BaseCalc) OnRow(file string, block int, line int, row Row) error {
	eventIndex := self.query.EventIndex(row.Event)
	if eventIndex < 0 {
		return nil
	}
	user, ok := self.users[row.Id]
	if !ok {
		user = NewBaseCalcUser(self.query)
		self.users[row.Id] = user
	}
	user.OnEvent(row.Ts, eventIndex, self.sampling)
	return nil
}

func (self *BaseCalc) Merge(id UserId, event int, events []Timestamp) {
	user, ok := self.users[id]
	if !ok {
		user = NewBaseCalcUser(self.query)
		self.users[id] = user
	}
	user.Merge(event, events)
}

func (self *BaseCalc) ByBlock() ScanSink {
	return ScanSink {nil, ByBlock(self.OnRow), true}
}

func (self *BaseCalc) ByRow() ScanSink {
	return ScanSink {self.OnRow, nil, false}
}

func NewBaseCalc(query *CalcQuery, sampling bool) *BaseCalc {
	var dsp *Sampling
	if sampling {
		dsp = &Sampling {}
	}
	return &BaseCalc {query, map[UserId]*BaseCalcUser {}, dsp}
}

type BaseCalc struct {
	query *CalcQuery
	users map[UserId]*BaseCalcUser
	sampling *Sampling
}

func (self *BaseCalcUser) OnEvent(ts Timestamp, index int, sampling *Sampling) {
	if self.score >= uint16(len(self.query.seq)) {
		return
	}

	self.events[index] = append(self.events[index], ts)

	score := uint16(0)
	lower := ts - self.query.window

	for i, _ := range self.query.seq {
		if sampling != nil {
			sampling.Add(len(self.events[i]))
		}
		blank := true
		for j, et := range self.events[i] {
			// TODO: > or >= ?
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

func (self *BaseCalcUser) Merge(event int, events []Timestamp) {
	self.events[event] = append(self.events[event], events...)
}

func NewBaseCalcUser(query *CalcQuery) *BaseCalcUser {
	return &BaseCalcUser {query, make(EventLinks, len(query.seq)), 0}
}

type BaseCalcUser struct {
	query *CalcQuery
	events EventLinks
	score uint16
}

func (self *Sampling) Add(n int) {
	if n > self.Max {
		self.Max = n
	}
	self.Count += 1
}

type Sampling struct {
	Max int
	Count int
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

func (self CalcQuery) EventIndex(event EventId) int {
	if int(event) >= len(self.events) {
		return -1
	}
	return self.events[int(event)]
}

func NewCalcQuery(events []EventId, window Timestamp) *CalcQuery {
	self := &CalcQuery {events, nil, window}

	// TODO: page can save some mem
	for i, event := range events {
		if int(event) >= len(self.events) {
			padding := make([]int, int(event) + 1 - len(self.events))
			for i, _ := range padding {
				padding[i] = -1
			}
			self.events = append(self.events, padding...)
		}
		self.events[event] = i
	}
	return self
}

type CalcQuery struct {
	seq []EventId
	events []int
	window Timestamp
}

type EventLinks []EventLink
type EventLink []Timestamp
