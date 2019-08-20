host="${1}"
shift 1

args=("${@}")
args_str="[ "
for it in "${args[@]}"; do
	args_str="$args_str'$it' "
done
args_str="$args_str]"

echo "${host}" "${args_str}"
