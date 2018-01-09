package binlogw

import (
	pb "github.com/pingcap/tipb/go-binlog"
)

func Mock() {
}

func Sync() {
}

type Executor interface {
	Handle(*pb.Binlog)
}

type CHSqlPrinter struct {
}

func (p *CHSqlPrinter) Handle(log *pb.Binlog) {
	sql, err := ToCHSql(log)
	if err != nil {
		println("ERROR: " + err.Error())
	} else {
		println(sql)
	}
}

func PollFromDrainerFiles(path string) (*pb.Binlog, error) {
	return nil, nil
}

type DrainerFile struct {
}

func NewDrainerFile(path string) (*DrainerFile, error) {
	return nil, nil
}

func ToCHSql(log *pb.Binlog) (string, error) {
	return "TODO", nil
}
