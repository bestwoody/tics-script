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
	query := cmds.Sub("query", "execute query")
	query.Reg("cal", "calculate analysys OLAP reward", CmdQueryCal)
	query.Reg("count", "calculate rows count", CmdQueryCount)

	data := cmds.Sub("data", "data commands")
	data.Reg("dump", "dump data and verify", CmdDataDump)

	index := cmds.Sub("index", "index commands")
	index.Reg("build", "build index from origin data", CmdIndexBuild)
	index.Reg("dump", "dump index and verify", CmdIndexDump)

	cmds.Run(os.Args[1:])
}

func CmdQueryCal(args []string) {
	var path string
	var from string
	var to string
	var events string
	var window int
	var exp string
	var conc int
	var bulk bool
	var byblock bool

	flag := flag.NewFlagSet("", flag.ContinueOnError)
	flag.StringVar(&path, "path", "db", "file path")
	flag.StringVar(&from, "from", "", "data begin time, 'YYYY-MM-DD HH:MM:SS-', ends with '-' means not included")
	flag.StringVar(&to, "to", "", "data end time, 'YYYY-MM-DD HH:MM:SS-', ends with '-' means not included" )
	flag.StringVar(&events, "events", "", "query events, seperated by ','")
	flag.IntVar(&window, "window", 60 * 24, "window size in minutes")
	flag.StringVar(&exp, "exp", "", "query data where expression is true")
	flag.IntVar(&conc, "conc", 0, "conrrent threads, '0' means auto detect")
	flag.BoolVar(&bulk, "bulk", false, "use block bulk loading")
	flag.BoolVar(&byblock, "byblock", false, "Async calculate, block by block")

	tools.ParseFlagOrDie(flag, args, "path", "from", "to", "events", "window", "exp", "conc", "bulk", "byblock")

	pred, err := ParseArgsPredicate(from, to)
	CheckError(err)
	isdir, err := IsDir(path)
	CheckError(err)
	eseq, err := ParseArgsEvents(events)
	CheckError(err)
	conc = AutoDectectConc(conc, isdir)
	CheckError(err)

	tracer, err := NewTraceUsers(eseq, Timestamp(window * 60 * 1000))
	CheckError(err)

	var sink ScanSink
	if byblock {
		sink = tracer.ByBlock()
	} else {
		sink = tracer.ByRow()
	}

	if isdir {
		err = FolderScan(path, conc, bulk, pred, sink)
	} else {
		err = FilesScan([]string {path}, conc, bulk, pred, sink)
	}
	CheckError(err)

	result := tracer.Result()
	for i := 0; i <= len(eseq); i++ {
		score := result[i]
		event := "-"
		if i != 0 {
			event = fmt.Sprintf("%v", eseq[i - 1])
		}
		fmt.Printf("%v\t#%v\t%v\t%v\n", event, i, score.Val, score.Acc)
	}
}

func CmdQueryCount(args []string) {
	var path string
	var from string
	var to string
	var conc int
	var bulk bool

	flag := flag.NewFlagSet("", flag.ContinueOnError)
	flag.StringVar(&path, "path", "db", "file path")
	flag.StringVar(&from, "from", "", "data begin time, 'YYYY-MM-DD HH:MM:SS-', ends with '-' means not included")
	flag.StringVar(&to, "to", "", "data end time, 'YYYY-MM-DD HH:MM:SS-', ends with '-' means not included" )
	flag.IntVar(&conc, "conc", 0, "conrrent threads, '0' means auto detect")
	flag.BoolVar(&bulk, "bulk", false, "use block bulk loading")

	tools.ParseFlagOrDie(flag, args, "path", "from", "to", "conc", "bulk")

	pred, err := ParseArgsPredicate(from, to)
	CheckError(err)
	isdir, err := IsDir(path)
	CheckError(err)
	conc = AutoDectectConc(conc, isdir)

	counter := NewRowCounter()
	sink := counter.ByRow()
	if isdir {
		err = FolderScan(path, conc, bulk, pred, sink)
	} else {
		err = FilesScan([]string {path}, conc, bulk, pred, sink)
	}
	CheckError(err)
	fmt.Printf("rows\t%v\nusers\t%v\nevents\t%v\nfiles\t%v\nblocks\t%v\n",
		counter.Rows, counter.Users, counter.Events, counter.Files, counter.Blocks)
}

func CmdDataDump(args []string) {
	var path string
	var from string
	var to string
	var user int
	var event int
	var conc int
	var bulk bool
	var verify bool
	var dry bool

	flag := flag.NewFlagSet("", flag.ContinueOnError)
	flag.StringVar(&path, "path", "db", "file path")
	flag.StringVar(&from, "from", "", "data begin time, 'YYYY-MM-DD HH:MM:SS-', ends with '-' means not included")
	flag.StringVar(&to, "to", "", "data end time, 'YYYY-MM-DD HH:MM:SS-', ends with '-' means not included" )
	flag.IntVar(&user, "user", 0, "only rows of this user, '0' means all")
	flag.IntVar(&event, "event", 0, "only rows of this event, '0' means all")
	flag.IntVar(&conc, "conc", 0, "conrrent threads, '0' means auto detect")
	flag.BoolVar(&bulk, "bulk", false, "use block bulk loading")
	flag.BoolVar(&verify, "verify", true, "verify timestamp ascending")
	flag.BoolVar(&dry, "dry", false, "dry run, for correctness check and benchmark")

	tools.ParseFlagOrDie(flag, args, "path", "from", "to", "user", "event", "conc", "bulk", "verify", "dry")

	pred, err := ParseArgsPredicate(from, to)
	CheckError(err)
	isdir, err := IsDir(path)
	CheckError(err)
	conc = AutoDectectConc(conc, isdir)

	sink := RowPrinter{os.Stdout, Timestamp(0), UserId(user), EventId(event), verify, dry}.ByRow()
	if isdir {
		err = FolderScan(path, conc, bulk, pred, sink)
	} else {
		err = FilesScan([]string {path}, conc, bulk, pred, sink)
	}
	CheckError(err)
}

func CmdIndexDump(args []string) {
	var path string
	flag := flag.NewFlagSet("", flag.ContinueOnError)
	flag.StringVar(&path, "path", "db.idx", "file path")
	tools.ParseFlagOrDie(flag, args, "path")
	err := IndexDump(path, os.Stdout)
	CheckError(err)
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

	isdir, err := IsDir(in)
	CheckError(err)
	if isdir {
		err = os.MkdirAll(out, 0744)
		if os.IsExist(err) {
			err = nil
		}
		CheckError(err)
		conc = AutoDectectConc(conc, isdir)
		err = FolderBuild(in, out, compress, gran, align, conc)
	} else {
		err = PartBuild(in, out, compress, gran, align)
	}
	CheckError(err)
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
	if s[len(s) - 1] == '-' {
		s = s[0: len(s) - 1]
		bound.Included = false
	}

	ts, err := strconv.ParseUint(s, 10, 64)
	if err == nil {
		bound.Ts = Timestamp(ts)
		return bound, nil
	}

	t, err := time.Parse("2006-01-02 15:04:05", s)
	if err != nil {
		return TimestampNoBound, err
	}
	// Manually change timezone, for platform compatibility
	bound.Ts = Timestamp(int64(t.UnixNano()) / int64(time.Millisecond)) - Timestamp(time.Hour * 8 / time.Millisecond)
	return bound, nil
}

func CheckError(err error) {
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}
}
