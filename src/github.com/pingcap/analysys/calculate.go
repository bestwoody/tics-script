package analysys

import (
	"errors"
	"fmt"
	"unsafe"
)

func (self PagedCalc) Result(result AccResult) {
	for _, page := range self.pages {
		page.Result(self.usersPerPage, result)
	}
	result.CalAcc(len(self.query.seq))
}

func (self *PagedCalc) OnRow(file string, block int, line int, row Row) error {
	eventIndex, ok := self.query.events[row.Event]
	if !ok {
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

	err := page.OnEvent(row.Id, row.Ts, eventIndex)
	if err == ErrRingBufferOverflow {
		// TODO: save to exception
	}
	return err
}

func (self *PagedCalc) ByBlock() ScanSink {
	return ScanSink {nil, ByBlock(self.OnRow), true}
}

func (self *PagedCalc) ByRow() ScanSink {
	return ScanSink {self.OnRow, nil, false}
}

// TODO: sampling
func NewPagedCalc(
	events []EventId,
	window Timestamp,
	userIdInterval int,
	usersPerPage int,
	eventsMaxLen int) (*PagedCalc, error) {

	tq := &TraceQuery {events, map[EventId]int{}, window}
	for i, event := range events {
		tq.events[event] = i
	}
	if len(tq.events) != len(events) {
		return nil, fmt.Errorf("duplicated event: %v", events)
	}
	return &PagedCalc {
		tq,
		make([]*Page, 0),
		userIdInterval,
		usersPerPage,
		eventsMaxLen,
	}, nil
}

type PagedCalc struct {
	query *TraceQuery
	pages []*Page
	userIdInterval int
	usersPerPage int
	eventsMaxLen int
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

func (self *Page) user(user int) *PageUser {
	return (*PageUser)(unsafe.Pointer(&self.data[user]))
}

func (self *Page) ring(user int, event int) *PageEventRing {
	return (*PageEventRing)(unsafe.Pointer(&self.data[user + PageUserLen]))
}

func (self *Page) event(user int, event int) unsafe.Pointer {
	return unsafe.Pointer(&self.data[user + PageUserLen + self.unitEvent * event])
}

func NewPage(start UserId, usersPerPage int, userIdInterval int, eventsMaxLen int, query *TraceQuery) *Page {
	unitEvent := eventsMaxLen * TimestampLen + PageEventRingLen
	unitUser := unitEvent * len(query.seq) + PageUserLen
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
	query *TraceQuery
	data []byte
}

func RingBufferAdd(ring *PageEventRing, buf unsafe.Pointer, ts Timestamp) (overflow bool) {
	// TODO
	return
}

func RingBufferPruge(ring *PageEventRing, buf unsafe.Pointer, lower Timestamp) (newLower Timestamp, blank bool) {
	// TODO
	return
}

var ErrRingBufferOverflow = errors.New("ring buffer overflow")

type PageEventRing struct {
	start int16
	end int16
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
	user, ok := self.users[row.Id]
	if !ok {
		user = NewBaseCalcUser(self.query)
		self.users[row.Id] = user
	}
	user.OnEvent(row.Ts, row.Event)
	return nil
}

func (self *BaseCalc) ByBlock() ScanSink {
	return ScanSink {nil, ByBlock(self.OnRow), true}
}

func (self *BaseCalc) ByRow() ScanSink {
	return ScanSink {self.OnRow, nil, false}
}

func NewBaseCalc(events []EventId, window Timestamp) (*BaseCalc, error) {
	tq := &TraceQuery {events, map[EventId]int{}, window}
	for i, event := range events {
		tq.events[event] = i
	}
	if len(tq.events) != len(events) {
		return nil, fmt.Errorf("duplicated event: %v", events)
	}
	return &BaseCalc {tq, map[UserId]*BaseCalcUser {}}, nil
}

type BaseCalc struct {
	query *TraceQuery
	users map[UserId]*BaseCalcUser
}

func (self *BaseCalcUser) OnEvent(ts Timestamp, event EventId) {
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

func NewBaseCalcUser(query *TraceQuery) *BaseCalcUser {
	return &BaseCalcUser {query, make(EventLinks, len(query.events)), 0}
}

type BaseCalcUser struct {
	query *TraceQuery
	events EventLinks
	score uint16
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

type TraceQuery struct {
	seq []EventId
	events map[EventId]int
	window Timestamp
}

type EventLinks []EventLink
type EventLink []Timestamp
