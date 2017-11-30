query="$1"

set -ef

if [ -z "$query" ]; then
	query="show tables"
fi

config=`pwd`
config=`dirname "$config"`
config="`dirname $config`/ch-connector/running/config/config.xml"

./_run.sh query "$config" "$query"
