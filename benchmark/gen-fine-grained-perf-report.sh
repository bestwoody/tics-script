#!/bin/bash

function print_help()
{
  echo "$0 usage: $0 log1 log2 [log3]"
}

if [ "$1" = "-help" ]; then
  print_help
  exit 0
fi

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
  print_help
  exit 1
fi

log1=$1
title1=$(head -n 1 "$log1")
log2=$2
title2=$(head -n 1 "$log2")
gen_log_3=false
if [ -n "$3" ]; then
  gen_log_3=true
  log3=$3
  title3=$(head -n 1 "$log3")
fi

set -eu

function gen_table_head()
{
  if [ "$gen_log_3" = true ]; then
    echo "| Query | $title1 | $title2 | $title3 |"
    echo "| ----- | ------- | ------- | ------- |"
  else
    echo "| Query | $title1 | $title2 |"
    echo "| ----- | ------- | ------- |"
  fi
}

function gen_table_row()
{
  id=${1%%query: *}
  q=${1#* query: }
  t1=$2
  t2=$(grep -A 1 "${id}" $log2 | grep -v "${id}")
  if [ "$gen_log_3" = true ]; then
    t3=$(grep -A 1 "${id}" $log3 | grep -v "${id}")
    echo "| ${q/|/\|} | $t1 | $t2 | $t3 |"
  else
    echo "| ${q/|/\|} | $t1 | $t2 |"
  fi
}

intable=false
inrow=false
qline=""
t1line=""
tail -n +2 $1 | while read line; do
  if [ "$line" = "<BEGIN_TABLE>" ]; then
    intable=true
    echo ""
    gen_table_head
  elif [ "$line" = "<END_TABLE>" ]; then
    echo ""
    intable=false
  elif [ "$intable" = true ]; then
    if [ "$inrow" = true ]; then
      t1line=$line
      inrow=false
      gen_table_row "$qline" "$t1line"
    else
      qline=$line
      inrow=true
    fi
  else
    echo "$line"
  fi
done
