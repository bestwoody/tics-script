set -eu

./build-ch-sdk.sh
echo
./build-chspark.sh
echo
./stop-all.sh
echo
./start-all.sh
echo
./check-running.sh
echo
./spark-shell.sh
