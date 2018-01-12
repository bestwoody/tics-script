package sql

import (
	"fmt"
	"github.com/pingcap/tipb/go-binlog"
)

type DebugSql struct {
}

func (self *DebugSql) ToSql(bl *binlog.Binlog) (string, error) {
	return fmt.Sprint("Got ", bl.Tp), nil
}
