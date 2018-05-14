#!/bin/bash

source ./_fine-grained-perf-helper.sh

set -u

res=""
if [ $selraw == "true" ]; then
  select="selraw"
else
  select="select"
fi

function log()
{
  echo "$1" | head -n 1 | tee -a $report
  if [ $? != 0 ]; then
    echo "Logging \"$1\" error, exiting."
    exit 1
  fi
}

function time_ch_q()
{
  log "$1 query: $2"
  tm=$(./ch-q.sh "$2" --time 2>&1 1>/dev/null)
  if [ $? != 0 ]; then
    echo "Timing CH query \"$1\" error."
  fi
  log "$tm"
}

function result_ch_q()
{
  ./ch-q.sh "$1" --time 1>/tmp/result 2>/tmp/time
  if [ $? != 0 ]; then
    echo "Getting result for CH query \"$1\" error."
  fi
  res=$(cat /tmp/result)
}

cat /dev/null > $report
if [ $select = "selraw" ]; then
  log "CH(selraw)"
else
  log "CH"
fi
log "# Fine grained perf report for table $table"
log "* Time unit: sec."
col_types=($(./ch-q.sh "desc $table"))
if [ $? != 0 ]; then
  echo "Getting schema of table $table error, exiting."
  exit 1
fi

if [ "$skip_single_column" != true ]; then
  log "## Single column"
  for ((i=0; i<${#col_types[@]}; i += 2)) do
    id=$((i / 2))
    col=${col_types[$i]}
    type=${col_types[$i + 1]}

    log "### Column $col"
    log "* Type: $type"
    result_ch_q "$select count($col) from $table"
    col_count=$res
    log "* Count: $col_count"
    result_ch_q "$select min($col) from $table"
    col_min=$res
    log "* Min: $col_min"
    result_ch_q "$select max($col) from $table"
    col_max=$res
    log "* Max: $col_max"

    log "<BEGIN_TABLE>"

    time_ch_q "SC${id}1" "$select $col from $table"
    time_ch_q "SC${id}2" "$select count($col) from $table"
    time_ch_q "SC${id}3" "$select min($col) from $table"
    time_ch_q "SC${id}4" "$select max($col) from $table"
    if [[ $type == *Int* ]] || [[ $type == *Float* ]]; then
      time_ch_q "SC${id}5" "$select sum($col) from $table"
    fi

    if [[ $type == *String* ]] || [[ $type == Date ]]; then
      col_min="'$col_min'"
      col_max="'$col_max'"
    fi
    time_ch_q "SC${id}6" "$select $col from $table where $col >= $col_min and $col <= $col_max"
    time_ch_q "SC${id}7" "$select $col from $table where $col < $col_min or $col > $col_max"

    if [[ $type == *String* ]]; then
      time_ch_q "SC${id}8" "$select $col = 'abc' from $table"
    elif [[ $type == Date ]]; then
      time_ch_q "SC${id}9" "$select $col + 1 > '1990-01-01' from $table"
    else
      time_ch_q "SC${id}10" "$select $col + 1 > 10000 from $table"
    fi

    log "<END_TABLE>"
  done
fi

if [ "$skip_multi_column" != true ]; then
  log "## Multi columns"
  proj=""
  proj_count=""
  proj_min=""
  proj_max=""
  for ((i=0; i<${#col_types[@]}; i += 2)) do
    id=$((i / 2))
    log "<BEGIN_TABLE>"

    col=${col_types[$i]}
    type=${col_types[$i + 1]}
    if [ $i != 0 ]; then
      proj="$proj, $col"
      proj_count="$proj_count, count($col)"
      proj_min="$proj_min, min($col)"
      proj_max="$proj_max, max($col)"
    else
      proj="$col"
      proj_count="count($col)"
      proj_min="min($col)"
      proj_max="max($col)"
    fi

    time_ch_q "MC${id}1" "$select $proj from $table"
    time_ch_q "MC${id}2" "$select $proj_count from $table"
    time_ch_q "MC${id}3" "$select $proj_min from $table"
    time_ch_q "MC${id}4" "$select $proj_max from $table"

    log "<END_TABLE>"
  done
fi

if [ "$skip_gby" != true ]; then
  log "## Group by"
  for ((i=0; i<${#col_types[@]}; i += 2)) do
    id=$((i / 2))
    col=${col_types[$i]}
    type=${col_types[$i + 1]}
    result_ch_q "$select count(distinct $col) from $table"
    dc=$res
    if [ -n "$dc" ] && [ $dc -le $gby_threshold ]; then
      log "### Column $col"
      log "* Distinct count: $dc"
      log "<BEGIN_TABLE>"

      time_ch_q "GB${id}1" "$select count(*) from $table group by $col"

      log "<END_TABLE>"
    fi
  done
fi
