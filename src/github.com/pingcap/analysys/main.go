package analysys

import (
	"flag"
	"os"
	"strings"
	"github.com/pingcap/analysys/tools"
)

func Main() {
	cmds := tools.NewCmds()

	data := cmds.Sub("data", "data commands")
	data.Reg("dump", "dump data and verify", CmdDataDump)

	index := cmds.Sub("index", "index commands")
	index.Reg("build", "build index from origin data", CmdIndexBuild)
	index.Reg("dump", "dump index and verify", CmdIndexDump)
	index.Reg("check", "verify index offsets", CmdIndexCheck)

	cmds.Run(os.Args[1:])
}

func CmdIndexBuild(args []string) {
	var out string
	var in string
	var gran int
	var align int
	var compress string

	flag := flag.NewFlagSet("", flag.ContinueOnError)
	flag.StringVar(&in, "in", "origin", "input file path")
	flag.StringVar(&out, "out", "db", "output path")
	flag.IntVar(&gran, "gran", 8192, "index granularity")
	flag.IntVar(&align, "align", 512, "block size/offset align")
	flag.StringVar(&compress, "compress", "", "compress method, '' means no compress")

	tools.ParseFlagOrDie(flag, args, "in", "out", "gran", "align")

	err := Build(in, out, strings.ToLower(compress), gran, align)
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}
}

func CmdIndexCheck(args []string) {
	var path string
	flag := flag.NewFlagSet("", flag.ContinueOnError)
	flag.StringVar(&path, "path", "db", "db file path")
	tools.ParseFlagOrDie(flag, args, "path")

	err := IndexToBlockCheck(path, os.Stdout)
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}
}

func CmdDataDump(args []string) {
	var path string
	flag := flag.NewFlagSet("", flag.ContinueOnError)
	flag.StringVar(&path, "path", "db", "file path")
	tools.ParseFlagOrDie(flag, args, "path")

	err := DataDump(path, os.Stdout)
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
