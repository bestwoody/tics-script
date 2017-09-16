db="data/db"
if [ ! -z "$1" ]; then
	db="$1"
fi

set -eu

# bin/analysys query dump ?
#
# -conc int
#   conrrent threads, '0' means auto detect
# -dry
#   dry run, for correctness check and benchmark
# -event int
#   only rows of this event, '0' means all
# -from string
#   data begin time, 'YYYY-MM-DD HH:MM:SS-', ends with '-' means not included
# -path string
#   file path (default "db")
# -to string
#   data end time, 'YYYY-MM-DD HH:MM:SS-', ends with '-' means not included
# -user int
#   only rows of this user, '0' means all
# -verify
#   verify timestamp ascending (default true)
#
#shortcut: <path> <from> <to> <user> <event> <conc> <verify> <dry>

bin/analysys query dump \
	-path="$db" \
	-from="" \
	-to="" \
	-user=0 \
	-event=0 \
	-conc=0 \
	-verify=true \
	-dry=false \
