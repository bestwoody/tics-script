# The dev guide for integrated tools

## This folder contains tools about TiFlash for:
* Testing
* Benchmark
* Online operating


## The special sub-folders
* `_base`: This folder contains common bash functions, just functions, if you `source` a script from here, it do actually nothing.
* The other folders: can be anything


## The `_env.sh` config file
* `<here>/_env.sh`: This file contains basic functions from `_base`. This file also contains a special arg `integrated` can be used in any scripts, it means `<here>`


## How to write new scripts here
* Use the header below, within this header you can use basic functions from `_base`:
```
#!/bin/bash
source `cd $(dirname ${BASH_SOURCE[0]}) && pwd`/../_env.sh
```
* Use abspath or paths start with `${integrated}` in scripts, this and the header make sure all path are right wherever you run a script.
* Call `auto_error_handle`(equal to `set -euo pipefail`) as soon as posible
    * It means you should:
        * Call it after header if your script doesn't have optional args.
        * Fetch the optional args of the script, then call this function. Once you call the function, any undefined vars/args are not allowed to refer.
    * This make sure nothing goes wrong even if your scripts are sloppy.
* If you want to check the return code of a command, you can call `set +e` to close auto error handling temporarily, then call the command and check `$?`, then call `restore_error_handl_flags` to re-enabled auto error handling.
* If you want to check an arg(eg: `var`) are defined or not, use `if [ ! -z ${var+x} ]; then {var is defined} fi`
* `${var}` is better than `$var`.
* When a function or script is called, embrace all args with '"', eg: `./myscript.sh "arg1" "arg2"`.
* Use bash array carefully, it's very tricky.
    * Use `"${array[@]}"` to pass an array to a function/script as args, notice it embrace by '"'
    * Use `"local args=`("${@}")`"` to store args as args, notice it embrace by '"'
* Wrap all codes in functions as many as possible, use local when defining a var in a function.
* Aways check the result of `\`...\`` or `$((...))` or other methods lauching sub-processes, it may(I don't know why) fail and `-e` can't catch the exception
* Be carefull with `grep`, it returns 1 when no line matchs
    * Use `ps -ef | grep bar | { grep -v grep || true; }` instead of `ps -ef | grep bar | grep -v grep`, this works when `-e` and `-o pipefail`
