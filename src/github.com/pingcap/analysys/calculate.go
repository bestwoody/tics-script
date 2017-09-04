package analysys

import "fmt"

func (self *PagedCalc) OnRow(file string, block int, line int, row Row) error {
	return fmt.Errorf("row mode not supported")
}

func (self *PagedCalc) handle(block LoadedBlock) {
	els := len(self.query.seq)
	pge := len(self.query.seq) * self.pgs

	for _, row := range block.Block {
		index := self.query.EventIndex(row.Event)
		if index < 0 {
			continue
		}

		pgi := int(row.Id) / self.pgs
		if pgi >= len(self.pages) {
			self.pages = append(self.pages, make([]Timestamps, pgi + 1 - len(self.pages))...)
		}

		page := self.pages[pgi]
		if page == nil {
			page = make(Timestamps, pge)
			self.pages[pgi] = page
		}

		pos := int(row.Id) % self.pgs * els
		user := page[pos: pos + els]

		if index == 0 {
			if user[index] == InvalidTimestamp {
				self.result[index] += 1
			}
			user[index] = row.Ts
		} else {
			prev := user[index - 1]
			if row.Ts - prev <= self.query.window {
				if user[index] == InvalidTimestamp {
					self.result[index] += 1
				}
				user[index] = prev
			}
		}
	}
}

func (self PagedCalc) Result(result AccResult) {
	result.Asign(self.result)
}

func (self *PagedCalc) ByBlock() ScanSink {
	return ScanSink {nil, ByBlock(self.handle), true}
}

func (self *PagedCalc) ByRow() ScanSink {
	return ScanSink {self.OnRow, nil, false}
}

func NewPagedCalc(query *CalcQuery) *PagedCalc {
	pgs := 1024
	return &PagedCalc {query, nil, pgs, make([]int, len(query.seq))}
}

type PagedCalc struct {
	query *CalcQuery
	pages []Timestamps
	pgs int
	result []int
}

func (self *StartLinkCalc) OnRow(file string, block int, line int, row Row) error {
	return fmt.Errorf("row mode not supported")
}

func (self *StartLinkCalc) handle(block LoadedBlock) {
	for _, row := range block.Block {
		index := self.query.EventIndex(row.Event)
		if index < 0 {
			continue
		}
		user, ok := self.users[row.Id]
		if !ok {
			user = make(Timestamps, len(self.query.seq))
			self.users[row.Id] = user
		}
		if index == 0 {
			if user[index] == InvalidTimestamp {
				self.result[index] += 1
			}
			user[index] = row.Ts
		} else {
			prev := user[index - 1]
			if row.Ts - prev <= self.query.window {
				if user[index] == InvalidTimestamp {
					self.result[index] += 1
				}
				user[index] = prev
			}
		}
	}
}

func (self StartLinkCalc) Result(result AccResult) {
	result.Asign(self.result)
}

func (self *StartLinkCalc) ByBlock() ScanSink {
	return ScanSink {nil, ByBlock(self.handle), true}
}

func (self *StartLinkCalc) ByRow() ScanSink {
	return ScanSink {self.OnRow, nil, false}
}

func NewStartLinkCalc(query *CalcQuery) *StartLinkCalc {
	return &StartLinkCalc {
		query,
		map[UserId]Timestamps {},
		make([]int, len(query.seq)),
	}
}

type StartLinkCalc struct {
	query *CalcQuery
	users map[UserId]Timestamps
	result []int
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
	user.OnEvent(row.Ts, eventIndex)
	return nil
}

func (self BaseCalc) Result(result AccResult) {
	for _, user := range self.users {
		result.Increase(int(user.score))
	}
	result.CalAcc(len(self.query.seq))
}

func (self *BaseCalc) ByBlock() ScanSink {
	return ScanSink {nil, BlockToRow(self.OnRow), true}
}

func (self *BaseCalc) ByRow() ScanSink {
	return ScanSink {self.OnRow, nil, false}
}

func NewBaseCalc(query *CalcQuery) *BaseCalc {
	return &BaseCalc {query, map[UserId]*BaseCalcUser {}}
}

type BaseCalc struct {
	query *CalcQuery
	users map[UserId]*BaseCalcUser
}

func (self *BaseCalcUser) OnEvent(ts Timestamp, index int) {
	if self.score >= uint16(len(self.query.seq)) {
		return
	}

	self.events[index] = append(self.events[index], ts)

	score := uint16(0)
	lower := ts - self.query.window

	for i, _ := range self.query.seq {
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

func NewBaseCalcUser(query *CalcQuery) *BaseCalcUser {
	return &BaseCalcUser {query, make([]Timestamps, len(query.seq)), 0}
}

type BaseCalcUser struct {
	query *CalcQuery
	events []Timestamps
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

func (self AccResult) Asign(users []int) {
	for i, v := range users {
		self[i + 1] = ScoredUsers {0, v}
	}
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

	// TODO: page it, can save some mem
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

type Timestamps []Timestamp
