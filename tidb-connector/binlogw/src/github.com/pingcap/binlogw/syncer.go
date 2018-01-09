package binlogw

import (
	"os"
	"flag"
	pb "github.com/pingcap/tipb/go-binlog"
)

func Sync() {
}

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

	source, err := NewSourceFromPath(path)
	if err != nil {
		return
	}

	handler := &CHSqlPrinter{}
	in := source.Poll()
	for event := range in {
		if event.Err != nil {
			return event.Err
		}
		err = handler.On(event.Log)
		if err != nil {
			return err
		}
	}
	return
}

type Executor interface {
	On(*pb.Binlog) error
}

type Source interface {
	Poll() (out chan BinlogEvent)
	Close() error
}

type BinlogEvent struct {
	Log *pb.Binlog
	Err error
}

type CHSqlPrinter struct {
}

func (p *CHSqlPrinter) On(log *pb.Binlog) error {
	sql, err := ToCHSql(log)
	if err != nil {
		return err
	}
	println(sql)
	return nil
}

func NewSourceFromPath(path string) (source Source, err error) {
	info, err := os.Stat(path)
	if err != nil {
		return
	}
	if info.IsDir() {
		source, err = NewDrainerFiles(path)
	} else {
		source, err = NewDrainerFile(path)
	}
	return
}

type DrainerFiles struct {
}

func NewDrainerFiles(path string) (Source, error) {
	// TODO
	return nil, nil
}

func (self *DrainerFiles) Poll() (out chan BinlogEvent) {
	// TODO
	return nil
}

func (self *DrainerFiles) Close() error {
	return nil
}

type DrainerFile struct {
	file *os.File
}

func NewDrainerFile(path string) (self Source, err error) {
	file, err := os.Open(path)
	if err != nil {
		return
	}
	self = &DrainerFile{file}
	return
}

func (self *DrainerFile) Poll() (out chan BinlogEvent) {
	out = make(chan BinlogEvent, 2)
	go func() {
		// TODO
	}()
	return out
}

func (self *DrainerFile) Close() error {
	return self.file.Close()
}

func ToCHSql(log *pb.Binlog) (string, error) {
	return "TODO", nil
}
