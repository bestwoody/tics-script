function run_file()
{
	local dbc="$1"
	local path="$2"
	local continue_on_error="$3"
	local fuzz="$4"

	python mutable-test.py "$dbc" "$path" "$fuzz"
	if [ $? == 0 ]; then
		echo $path: OK
	else
		echo $path: Failed
		if [ "$continue_on_error" != "true" ]; then
			exit 1
		fi
	fi
}

function run_dir()
{
	local dbc="$1"
	local path="$2"
	local continue_on_error="$3"
	local fuzz="$4"

	find "$path" -maxdepth 1 -name "*.visual" -type f | sort -V | while read file; do
		if [ -f "$file" ]; then
			python gen-test-from-visual.py "$file"
		fi
		if [ $? != 0 ]; then
			echo "Generate test files failed: $file" >&2
			exit
		fi
	done

	if [ $? != 0 ]; then
		echo "Generate test files failed" >&2
		exit 1
	fi

	find "$path" -maxdepth 1 -name "*.test" -type f | sort -V | while read file; do
		if [ -f "$file" ]; then
			run_file "$dbc" "$file" "$continue_on_error" "$fuzz"
		fi
	done

	if [ $? != 0 ]; then
		exit 1
	fi

	find "$path" -maxdepth 1 -type d | sort -Vr | while read dir; do
		if [ -d "$dir" ] && [ "$dir" != "$path" ]; then
			run_dir "$dbc" "$dir" "$continue_on_error" "$fuzz"
		fi
	done

	if [ $? != 0 ]; then
		exit 1
	fi
}

function run_path()
{
	local dbc="$1"
	local path="$2"
	local continue_on_error="$3"
	local fuzz="$4"

	if [ -f "$path" ]; then
		run_file "$dbc" "$path" "$continue_on_error" "$fuzz"
	else
		if [ -d "$path" ]; then
			run_dir "$dbc" "$path" "$continue_on_error" "$fuzz"
		else
			echo "error: $path not file nor dir." >&2
			exit 1
		fi
	fi
}

target="$1"
fuzz="$2"
debug="$3"
continue_on_error="$4"
dbc="$5"

if [ -z "$target" ]; then
	target="mutable-test"
fi

if [ -z "$debug" ]; then
	debug="false"
fi

if [ -z "$fuzz" ]; then
	fuzz="false"
fi

if [ -z "$dbc" ]; then
	if [ "$debug" != "false" ] && [ "$debug" != "0" ]; then
		debug="--stacktrace"
	fi
	dbc="build/dbms/src/Server/clickhouse client $debug -f PrettyCompactNoEscapes --query"
fi

if [ -z "$continue_on_error" ]; then
	continue_on_error="false"
fi
run_path "$dbc" "$target" "$continue_on_error" "$fuzz"
