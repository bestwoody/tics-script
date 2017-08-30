package analysys

import (
	"flag"
	"os"
	"runtime"
	"strings"
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
				conc = runtime.NumCPU() / 4
			}
			if conc < 4 {
				conc = 3
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
	var verify bool
	var conc int
	var dry bool

	flag := flag.NewFlagSet("", flag.ContinueOnError)
	flag.StringVar(&path, "path", "db", "file path")
	flag.BoolVar(&verify, "verify", true, "verify timestamp ascending")
	flag.IntVar(&conc, "conc", 0, "conrrent threads, '0' means auto detect")
	flag.BoolVar(&dry, "dry", false, "dry run, for correctness check and benchmark")

	tools.ParseFlagOrDie(flag, args, "path", "verify", "conc", "dry")

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

		if info.IsDir() {
			if conc <= 0 {
				conc = runtime.NumCPU()
			}
			return FolderDump(path, conc, os.Stdout, verify, dry)
		} else {
			if conc <= 0 {
				conc = 1
			}
			return PartDump(path, conc, os.Stdout, verify, dry)
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
