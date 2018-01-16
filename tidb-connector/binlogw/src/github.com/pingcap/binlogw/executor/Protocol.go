package executor

import (
	"github.com/pingcap/tipb/go-binlog"
)

type Executor interface {
	On(*binlog.Binlog) error
}
