#!/bin/bash

here=`cd $(dirname ${BASH_SOURCE[0]}) && pwd`
source "${here}/_env.sh"
auto_error_handle

title='<cluster run/stop elapsed>'
data="${here}/ti_launch.sh.data"
report="${here}/ti_launch.sh.report"

to_table "${title}" 'cols:op; rows:mod,ver|notag; cell:limit(20)|avg|~|duration' 9999 "${data}" | tee "${report}"
