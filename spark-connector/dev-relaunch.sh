set -eu

./dev-build-chspark.sh
echo
./dev-stop-all.sh
echo
./dev-start-all.sh
echo
./dev-check-running.sh
echo
./spark-shell.sh
