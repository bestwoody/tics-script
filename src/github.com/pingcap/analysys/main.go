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
	index.Reg("check", "verify index", CmdIndexCheck)

	cmds.Run(os.Args[1:])
}

func CmdIndexBuild(args []string) {
	var out string
	var in string
	var gran int
	var compress string

	flag := flag.NewFlagSet("", flag.ContinueOnError)
	flag.StringVar(&in, "in", "origin", "input file path")
	flag.StringVar(&out, "out", "db", "output path")
	flag.IntVar(&gran, "gran", 8192, "index granularity")
	flag.StringVar(&compress, "compress", "", "compress method, '' means no compress")

	tools.ParseFlagOrDie(flag, args, "in", "out", "gran")

	err := Build(in, out, strings.ToLower(compress), gran)
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}
}

func CmdIndexCheck(args []string) {
	println("TODO")
}

func CmdDataDump(args []string) {
	var path string
	flag := flag.NewFlagSet("", flag.ContinueOnError)
	flag.StringVar(&path, "path", "db", "file path")
	tools.ParseFlagOrDie(flag, args, "path")

	err := Dump(path, os.Stdout)
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}
}
