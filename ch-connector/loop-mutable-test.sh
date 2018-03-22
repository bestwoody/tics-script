set -eu
for ((i = 0; i < 1000; i++)); do
	./mutable-test.sh mutable-test
done
