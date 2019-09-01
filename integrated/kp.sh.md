# The ops/kp.sh tool
The ops/kp.sh (kp means keeper) is design for running bunchs of test cases in backgound processes. It's a bit like the `supervisor` tool,but more convenient:
* Using `supervisor`, you have to write a config file for each single task.
* In `supervisorctl`, processes are not related, you can't get a whole big piture of what all the things are going on easily.


## Quick start
Let's assume we have this scripts:
```
my/bar.sh
my/x/foo.sh
my/x/bar.sh
my/x/no.sh
```

Edit a file `my.kp` like this, you can put it anywhere you want:
```
# This is a sample of *.kp file
my/bar.sh
my/x
!my/x/no.sh
```

Then, you can use this commands:
* `ops/kp.sh my.kp run`: run `my/bar.sh`, `my/foo.sh`, `my/bar.sh`, if any of them finish, it will be auto re-runned
* `ops/kp.sh my.kp list`: list the scripts that will be runned
* `ops/kp.sh my.kp stop`: stop all. the unfinished scripts will take a while to finish in the background
* `ops/kp.sh my.kp status`: show status of all the scripts


## How to edit a `*.ti` file
```
my/bar.sh           -- keep this script file running
my/bar.sh.term      -- if this file exists, it will be runned when `my/bar.sh` is stopping, so it can do some clearing jobs
my/x                -- keep *.sh files in this specified dir running
!my/x/no.sh         -- ignore this script file, even it appear in the specified dirsh
```


## All things are automatic
If a `*.kp` file is running, edit the file will be appled automatically:
* If you add new dirs or script files, they will be runned in secondes.
* If you disable a script by prefix `!`, it will be stop automatically. (will take a while to finish, like the `stop` command)


## Status logs
```
my.kp               -- the kp file
my.kp.log           -- log: when the scripts in the pk file start or end
my/bar.sh           -- the task script
my/bar.sh.report    -- (may not exists) if a task generate the report file, it will shows in `status` command
my/bar.sh.log       -- the stdout of the task script
my/bar.sh.err.log   -- the stderr of the task script
my/bar.sh.term      -- (may not exists) will be runned when `my/bar.sh` is stopping if this file exists
my/bar.sh.term.log  -- (may not exists) only apear when `my/bar.sh.term` fail or have output lines during execution
```


## Processe module
A script is a task
```
keep_script_running my.kp        -- the monitor process           - will be killed when stop
  keep_script_running my/foo.sh  -- the looping process of task   - will be killed when stop
    bash my/foo.sh               -- the real process of task      - will be run to the end when stop
  keep_script_running my/bar.sh  -- the looping process of task   - will be killed when stop
    bash my/bar.sh               -- the real process of task      - will be run to the end when stop
  ...
```
