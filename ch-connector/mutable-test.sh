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

	for file in $path/*; do
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

if [ -z "$target" ]; then
	target="mutable-test"
fi
if [ -z "$dbc" ]; then
	dbc="build/dbms/src/Server/clickhouse client -f PrettyCompactNoEscapes --query"
fi

run_path "$dbc" "$target"
