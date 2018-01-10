package source

import (
	"github.com/pingcap/tipb/go-binlog"
)

type Source interface {
	Poll() (out chan BinlogEvents)
	Close() error
}

type BinlogEvents struct {
	Logs []binlog.Binlog
	Err error
}
