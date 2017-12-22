build="$1"

set -eu

if [ "$build" == "build" ]; then
	./build-ch-sdk.sh
	echo
	./build-chspark.sh
	echo
fi

./stop-all.sh
echo
./start-all.sh
echo
./check-running.sh
echo
./spark-shell.sh
