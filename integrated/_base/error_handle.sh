#!/bin/bash

function auto_error_handle()
{
	set -ueo pipefail
}
export -f auto_error_handle

# Example:
#   error_handle="$-"
#   set +e
#   do something
#   restore_error_handle_flags "${error_handle}"
function restore_error_handle_flags()
{
	local flags="$1"
	if [ ! -z "`echo ${flags} | { grep 'e' || test $? = 1; }`" ]; then
		set -e
	else
		set +e
	fi
	if [ ! -z "`echo ${flags} | { grep 'u' || test $? = 1; }`" ]; then
		set -u
	else
		set +u
	fi
}
export -f restore_error_handle_flags
