host="${1}"
shift
ssh "${host}" "${@}" | awk '{print "['${host}'] "$0}'
