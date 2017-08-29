package analysys

import (
	"flag"
	"os"
	"strings"
	"github.com/pingcap/analysys/tools"
)

func Main() {
	cmds := tools.NewCmds()
	cmds.Reg("build", "build indexed data from origin data", CmdBuild)
	cmds.Run(os.Args[1:])
}

func CmdBuild(args []string) {
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
