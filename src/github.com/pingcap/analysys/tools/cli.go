package tools

import (
	"flag"
	"fmt"
	"os"
)

func AutoComplete(args []string, flags ...string) []string {
	if len(args) == 0 {
		return args
	}

	for _, it := range args {
		if len(it) > 0 && it[0] == '-' {
			for _, c := range it[1:] {
				if c < '0' || c > '9' {
					return args
				}
			}
		}
	}

	if len(args) > len(flags) {
		return args
	}

	ret := []string{}
	for i, arg := range args {
		flag := flags[i]
		ret = append(ret, "-" + flag + "=" + arg)
	}

	return ret
}

func ArgsCount(fs *flag.FlagSet) (count int) {
	fs.VisitAll(func(it *flag.Flag) {
		count += 1
	})
	return
}

func ParseFlagOrDie(flag *flag.FlagSet, args []string, flags ...string) {
	display := func() {
		if ArgsCount(flag) == 0 {
			fmt.Println("no args need")
			return
		}

		flag.PrintDefaults()

		fmt.Println()
		fmt.Print("shortcut:")
		for _, it := range flags {
			fmt.Print(" <", it, ">")
		}
		fmt.Println()
	}

	if len(args) > 0 && (args[len(args) - 1] == "help" || args[len(args) - 1] == "?") {
		display()
		os.Exit(1)
	}

	args = AutoComplete(args, flags...)
	err := flag.Parse(args)
	if err != nil {
		display()
		os.Exit(1)
	}
}
