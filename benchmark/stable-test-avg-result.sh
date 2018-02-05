file="$1"

default="stable-test.log"

if [ -z "$file" ]; then
	if [ -f "$default" ]; then
		file="$default"
	else
		echo "usage: <bin> spark-log-file"
		exit 1
	fi
fi

cat "$file" | python stable-test-avg-result.py
