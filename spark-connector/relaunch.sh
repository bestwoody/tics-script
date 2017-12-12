set -eu

./build-chspark.sh
echo
./stop-all.sh
echo
./start-all.sh
echo
./check-running.sh
echo
./spark-shell.sh
