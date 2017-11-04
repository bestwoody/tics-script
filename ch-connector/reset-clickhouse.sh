set -eu

cd clickhouse

git status | grep modified | awk -F 'modified:' '{print $2}' | xargs git checkout
