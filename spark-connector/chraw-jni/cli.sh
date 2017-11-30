set -ef

config=`pwd`
config=`dirname "$config"`
config="`dirname $config`/ch-connector/running/config/config.xml"

./_run.sh cli "$config"
