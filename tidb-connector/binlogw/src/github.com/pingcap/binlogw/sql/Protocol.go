package sql

import (
	"github.com/pingcap/tipb/go-binlog"
)

type SqlGenerator interface {
	ToSql(*binlog.Binlog) (string, error)
}
