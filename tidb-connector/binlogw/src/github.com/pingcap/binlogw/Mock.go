package binlogw

import (
	"flag"
	"os"
	"github.com/pingcap/binlogw/source"
	"github.com/pingcap/binlogw/executor"
	"github.com/pingcap/binlogw/sql"
)

func Mock() {
	err := MockRun(os.Args[1:])
	if err != nil {
		println("ERROR: " + err.Error())
		os.Exit(1)
	}
}

func MockRun(args []string) (err error) {
	var path string
	flag := flag.NewFlagSet("TiDB binlog syncer", flag.ContinueOnError)
	flag.StringVar(&path, "path", "data.drainer/binlog-0000000000000000", "file/dir path to read")

	err = flag.Parse(args)
	if err != nil {
		return
	}

	source, err := source.NewSourceFromPath(path)
	if err != nil {
		return
	}

	handler := executor.NewPrinter(os.Stdout, &sql.DebugSql{})
	in := source.Poll()
	for events := range in {
		if events.Err != nil {
			return events.Err
		}
		for _, bl := range events.Logs {
			err = handler.On(&bl)
			if err != nil {
				return err
			}
		}
	}
	return
}
