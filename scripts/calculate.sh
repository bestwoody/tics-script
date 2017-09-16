set -eu

# bin/analysys query cal ?
#
# -byblock
#   Async calculate, block by block (default true)
# -calc string
#   choose calculator from [base, startlink, paged] (default "paged")
# -conc int
#   conrrent threads, '0' means auto detect
# -events string
#   query events, seperated by ','
# -exp string
#   query data where expression is true
# -from string
#   data begin time, 'YYYY-MM-DD HH:MM:SS-', ends with '-' means not included
# -path string
#   file path (default "db")
# -to string
#   data end time, 'YYYY-MM-DD HH:MM:SS-', ends with '-' means not included
# -window int
#   window size in minutes (default 1440)
#
# shortcut: <path> <from> <to> <events> <window> <exp> <conc> <calc> <byblock>

#- Q1 -

#       #0      26      4000000
#10001  #1      4074    3999974
#10004  #2      386966  3995900
#10008  #3      3608934 3608934

bin/analysys query cal \
	-path="data/db" \
	-from="2017-01-01 00:00:00" \
	-to="2017-02-01 00:00:00" \
	-events="10001,10004,10008"\
	-window=10080 \
	-calc="paged" \
