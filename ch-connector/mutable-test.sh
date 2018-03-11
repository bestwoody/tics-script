function run_file()
{
	local dbc="$1"
	local path="$2"
	local continue_on_error="$3"
	local fuzz="$4"
	local skip_raw_test="$5"

	local ext=${path##*.}

	if [ "$ext" == "test" ]; then
		python mutable-test.py "$dbc" "$path" "$fuzz"
	else
		if [ "$ext" == "visual" ]; then
			python gen-test-from-visual.py "$path" "$skip_raw_test"
			if [ $? != 0 ]; then
				echo "Generate test files failed: $file" >&2
				exit 1
			fi
			run_dir "$dbc" "$path.test" "$continue_on_error" "$fuzz" "$skip_raw_test"
		fi
	fi

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
	local skip_raw_test="$5"

	find "$path" -maxdepth 1 -name "*.visual" -type f | sort -V | while read file; do
		if [ -f "$file" ]; then
			python gen-test-from-visual.py "$file" "$skip_raw_test"
		fi
		if [ $? != 0 ]; then
			echo "Generate test files failed: $file" >&2
			exit 1
		fi
	done

	if [ $? != 0 ]; then
		echo "Generate test files failed" >&2
		exit 1
	fi

	find "$path" -maxdepth 1 -name "*.test" -type f | sort -V | while read file; do
		if [ -f "$file" ]; then
			run_file "$dbc" "$file" "$continue_on_error" "$fuzz" "$skip_raw_test"
		fi
	done

	if [ $? != 0 ]; then
		exit 1
	fi

	find "$path" -maxdepth 1 -type d | sort -Vr | while read dir; do
		if [ -d "$dir" ] && [ "$dir" != "$path" ]; then
			run_dir "$dbc" "$dir" "$continue_on_error" "$fuzz" "$skip_raw_test"
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
	local skip_raw_test="$5"

	if [ -f "$path" ]; then
		run_file "$dbc" "$path" "$continue_on_error" "$fuzz" "$skip_raw_test"
	else
		if [ -d "$path" ]; then
			run_dir "$dbc" "$path" "$continue_on_error" "$fuzz" "$skip_raw_test"
		else
			echo "error: $path not file nor dir." >&2
			exit 1
		fi
	fi
}

target="$1"
fuzz="$2"
skip_raw_test="$3"
debug="$4"
continue_on_error="$5"
dbc="$6"

if [ -z "$target" ]; then
	target="mutable-test"
fi

if [ -z "$debug" ]; then
	debug="false"
fi

if [ -z "$fuzz" ]; then
	fuzz="false"
fi

if [ -z "$skip_raw_test" ]; then
	skip_raw_test="false"
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
run_path "$dbc" "$target" "$continue_on_error" "$fuzz" "$skip_raw_test"
