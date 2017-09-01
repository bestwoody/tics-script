package analysys

import (
	"flag"
	"fmt"
	"runtime"
	"os"
	"strconv"
	"strings"
	"time"
	"github.com/pingcap/analysys/tools"
)

func Main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	cmds := tools.NewCmds(false)
	cmds.Reg("query", "execute query", CmdQuery)

	data := cmds.Sub("data", "data commands")
	data.Reg("dump", "dump data and verify", CmdDataDump)

	index := cmds.Sub("index", "index commands")
	index.Reg("build", "build index from origin data", CmdIndexBuild)
	index.Reg("dump", "dump index and verify", CmdIndexDump)

	cmds.Run(os.Args[1:])
}

func CmdQuery(args []string) {
	var path string
	var from string
	var to string
	var events string
	var window int
	var exp string
	var conc int

	flag := flag.NewFlagSet("", flag.ContinueOnError)
	flag.StringVar(&path, "path", "db", "file path")
	flag.StringVar(&from, "from", "", "data begin time, '-YYYY-MM-DD HH:MM:SS', starts with '-' means not included")
	flag.StringVar(&to, "to", "", "data end time, '-YYYY-MM-DD HH:MM:SS', starts with '-' means not included" )
	flag.StringVar(&events, "events", "", "query events, seperated by ','")
	flag.IntVar(&window, "window", 60 * 60 * 24, "window size")
	flag.StringVar(&exp, "exp", "", "query data where expression is true")
	flag.IntVar(&conc, "conc", 0, "conrrent threads, '0' means auto detect")

	tools.ParseFlagOrDie(flag, args, "path", "from", "to", "events", "window", "exp", "conc")

	pred, err := ParseArgsPredicate(from, to)
	if err != nil {
		return
	}
	isdir, err := IsDir(path)
	if err != nil {
		return
	}
	eseq, err := ParseArgsEvents(events)
	if err != nil {
		return
	}
	conc = AutoDectectConc(conc, isdir)

	query := func() error {
		tracer := NewTraceUsers(eseq, Timestamp(window))
		sink := tracer.ByRow()
		if isdir {
			return FolderScan(path, conc, pred, sink)
		} else {
			return FilesScan([]string {path}, conc, pred, sink)
		}
	}
	err = query()
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}
}

func CmdDataDump(args []string) {
	var path string
	var from string
	var to string
	var conc int
	var verify bool
	var dry bool

	flag := flag.NewFlagSet("", flag.ContinueOnError)
	flag.StringVar(&path, "path", "db", "file path")
	flag.StringVar(&from, "from", "", "data begin time, '-YYYY-MM-DD HH:MM:SS', starts with '-' means not included")
	flag.StringVar(&to, "to", "", "data end time, '-YYYY-MM-DD HH:MM:SS', starts with '-' means not included" )
	flag.IntVar(&conc, "conc", 0, "conrrent threads, '0' means auto detect")
	flag.BoolVar(&verify, "verify", true, "verify timestamp ascending")
	flag.BoolVar(&dry, "dry", false, "dry run, for correctness check and benchmark")

	tools.ParseFlagOrDie(flag, args, "path", "from", "to", "conc", "verify", "dry")

	pred, err := ParseArgsPredicate(from, to)
	if err != nil {
		return
	}
	isdir, err := IsDir(path)
	if err != nil {
		return
	}
	conc = AutoDectectConc(conc, isdir)

	dump := func() error {
		sink := RowPrinter{os.Stdout, Timestamp(0), verify, dry}.ByRow()
		if isdir {
			return FolderScan(path, conc, pred, sink)
		} else {
			return FilesScan([]string {path}, conc, pred, sink)
		}
	}
	err = dump()
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}
}

func CmdIndexDump(args []string) {
	var path string
	flag := flag.NewFlagSet("", flag.ContinueOnError)
	flag.StringVar(&path, "path", "db.idx", "file path")
	tools.ParseFlagOrDie(flag, args, "path")

	err := IndexDump(path, os.Stdout)
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}
}

func CmdIndexBuild(args []string) {
	var out string
	var in string
	var compress string
	var conc int
	var gran int
	var align int

	flag := flag.NewFlagSet("", flag.ContinueOnError)
	flag.StringVar(&in, "in", "origin", "input file path")
	flag.StringVar(&out, "out", "db", "output path")
	flag.StringVar(&compress, "compress", "snappy", "compress method, '' means no compress")
	flag.IntVar(&conc, "conc", 0, "conrrent threads, '0' means auto detect")
	flag.IntVar(&gran, "gran", 1024 * 8, "index granularity")
	flag.IntVar(&align, "align", 512, "block size/offset align")

	tools.ParseFlagOrDie(flag, args, "in", "out", "compress", "conc", "gran", "align")
	compress = strings.ToLower(compress)

	build := func(in, out string, compress string, gran, align int) error {
		isdir, err := IsDir(in)
		if err != nil {
			return err
		}
		if isdir {
			err = os.MkdirAll(out, 0744)
			if err != nil && !os.IsNotExist(err) {
				return err
			}
			conc = AutoDectectConc(conc, isdir)
			return FolderBuild(in, out, compress, gran, align, conc)
		}
		return PartBuild(in, out, compress, gran, align)
	}

	err := build(in, out, compress, gran, align)
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}
}

func ParseArgsPredicate(from, to string) (pred Predicate, err error) {
	pred.Lower, err = ParseDateTime(from)
	if err != nil {
		return
	}
	pred.Upper, err = ParseDateTime(to)
	if err != nil {
		return
	}
	return
}

func ParseArgsEvents(s string) ([]EventId, error) {
	if len(s) == 0 {
		return nil, fmt.Errorf("events not specified")
	}
	events := make([]EventId, 0)
	for _, it := range strings.Split(s, ",") {
		ev, err := strconv.Atoi(it)
		if err != nil {
			return nil, fmt.Errorf("parsing event args: %s", err.Error())
		}
		events = append(events, EventId(ev))
	}
	return events, nil
}

func AutoDectectConc(conc int, isdir bool) int {
	if conc != 0 {
		return conc
	}
	if isdir {
		if conc <= 0 {
			conc = runtime.NumCPU() / 2 + 2
		}
	} else {
		if conc <= 0 {
			conc = 1
		}
	}
	return conc
}

func IsDir(path string) (bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return false, err
	}
	return info.IsDir(), nil
}

func ParseDateTime(s string) (TimestampBound, error) {
	if len(s) == 0 {
		return TimestampNoBound, nil
	}

	var bound TimestampBound
	bound.Included = true
	if s[0] == '-' {
		s = s[1: len(s) - 1]
		bound.Included = false
	}

	t, err := time.Parse("2006-01-02 15:04:05", s)
	if err != nil {
		return TimestampNoBound, err
	}
	bound.Ts = Timestamp(int64(t.UnixNano()) / int64(time.Second))
	return bound, nil
}
