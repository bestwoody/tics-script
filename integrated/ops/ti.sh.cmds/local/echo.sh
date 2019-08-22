index="${1}"
mod_name="${2}"
dir="${3}"
conf_rel_path="${4}"
host="${5}"

shift 5

args=("${@}")
args_str="extra:[ "
for it in "${args[@]}"; do
	args_str="$args_str'$it' "
done
args_str="$args_str]"

echo "${index}" "${mod_name}" "${dir}" "${conf_rel_path}" "${host}" "${args_str}"
