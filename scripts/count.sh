db="data/db"
if [ ! -z "$1" ]; then
	db="$1"
fi

set -eu

# bin/analysys query count ?
#
# -bulk
#   use block bulk loading (default true)
# -conc int
#   conrrent threads, '0' means auto detect
# -fast
#   just count the rows
# -from string
#   data begin time, 'YYYY-MM-DD HH:MM:SS-', ends with '-' means not included
# -path string
#   file path (default "db")
# -to string
#   data end time, 'YYYY-MM-DD HH:MM:SS-', ends with '-' means not included
#
#shortcut: <path> <from> <to> <fast> <conc> <bulk>

bin/analysys query count \
	-path="$db" \
	-from="" \
	-to="" \
	-fast=false \
	-conc=0 \
	-bulk=true
