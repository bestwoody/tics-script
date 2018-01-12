package executor

import (
	"io"
	"github.com/pingcap/tipb/go-binlog"
	"github.com/pingcap/binlogw/sql"
)

type Printer struct {
	w io.Writer
	g sql.SqlGenerator
}

func NewPrinter(w io.Writer, g sql.SqlGenerator) *Printer {
	return &Printer{w, g}
}

func (self *Printer) On(bl *binlog.Binlog) error {
	sql, err := self.g.ToSql(bl)
	if err != nil {
		return err
	}
	println(sql)
	return nil
}
