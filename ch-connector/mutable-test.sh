function run_file()
{
	local dbc="$1"
	local path="$2"

	python mutable-test.py "$dbc" "$path"
	if [ $? == 0 ]; then
		echo $path: OK
	else
		echo $path: Failed
		exit 1
	fi
}

function run_dir()
{
	local dbc="$1"
	local path="$2"

	find "$path" -name "*.test" -type f | while read file; do
		if [ -f "$file" ]; then
			run_file "$dbc" "$file"
		fi
	done
}

function run_path()
{
	local dbc="$1"
	local path="$2"

	if [ -f "$path" ]; then
		run_file "$dbc" "$path"
	else
		if [ -d "$path" ]; then
			run_dir "$dbc" "$path"
		else
			echo "error: $path not file nor dir." >&2
			exit 1
		fi
	fi
}

target="$1"
dbc="$2"
debug="$3"

if [ -z "$target" ]; then
	target="mutable-test"
fi

if [ -z "$debug" ]; then
	debug="false"
fi

if [ -z "$dbc" ]; then
	if [ "$debug" != "false" ] && [ "$debug" != "0" ]; then
		debug="--stacktrace"
	fi
	dbc="build/dbms/src/Server/clickhouse client $debug -f PrettyCompactNoEscapes --query"
fi

run_path "$dbc" "$target"
