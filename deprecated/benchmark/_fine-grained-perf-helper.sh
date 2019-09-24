#!/bin/bash

source ./_env.sh

function print_help()
{
  echo "$0 usage: $0 table [OPTION ...]"
  echo "Options:"
  echo "-help: Print this message."
  echo "-log=<LOGFILE>: Log file name."
  echo "-skip-single-column: Skip single column perfing."
  echo "-skip-multi-column: Skip multi column perfing."
  echo "-skip-gby: Skip group by perfing."
  echo "-gby-threshold=<THRESHOLD>: Distinct count threshold (less or equal) for doing group by."
}

if [ "$1" = "-help" ]; then
  print_help
  exit 0
fi

table=$1
report=${0/.sh/.log}
skip_single_column=false
skip_multi_column=false
skip_gby=false
gby_threshold=50

for arg in $@; do
  if [[ $arg = -log=* ]]; then
    report=${arg:5}
  elif [ $arg = "-skip-single-column" ]; then
    skip_single_column=true
  elif [ $arg = "-skip-multi-column" ]; then
    skip_multi_column=true
  elif [ $arg = "-skip-gby" ]; then
    skip_gby=true
  elif [[ $arg = -gby-threshold=* ]]; then
    gby_threshold=$arg
    gby_threshold=${arg:15}
  elif [ $arg != $table ]; then
    echo "Unknown argument $arg."
    print_help
    exit 1
  fi
done
