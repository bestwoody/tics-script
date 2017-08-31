package analysys

import (
	"flag"
	"runtime"
	"os"
	"strings"
	"time"
	"github.com/pingcap/analysys/tools"
)

func Main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	cmds := tools.NewCmds()

	data := cmds.Sub("data", "data commands")
	data.Reg("dump", "dump data and verify", CmdDataDump)

	index := cmds.Sub("index", "index commands")
	index.Reg("build", "build index from origin data", CmdIndexBuild)
	index.Reg("dump", "dump index and verify", CmdIndexDump)

	cmds.Run(os.Args[1:])
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
		file, err := os.Open(in)
		if err != nil {
			return err
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil {
			return err
		}

		if info.IsDir() {
			err = os.MkdirAll(out, 0744)
			if err != nil && !os.IsNotExist(err) {
				return err
			}
			if conc <= 0 {
				conc = runtime.NumCPU() / 4 + 2
			}
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

	dump:= func(path string) error {
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil {
			return err
		}

		var pred Predicate
		pred.Lower, err = ParseDateTime(from)
		if err != nil {
			return err
		}
		pred.Upper, err = ParseDateTime(to)
		if err != nil {
			return err
		}

		if info.IsDir() {
			if conc <= 0 {
				conc = runtime.NumCPU() / 2 + 2
			}
			return FolderDump(path, conc, pred, os.Stdout, verify, dry)
		} else {
			if conc <= 0 {
				conc = 1
			}
			return PartDump(path, conc, pred, os.Stdout, verify, dry)
		}
	}

	err := dump(path)
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
