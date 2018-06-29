set -eu

source ./_env.sh

for node in ${nodes[@]}; do
	echo "$node:"
	ssh "$node" "ps -ef | grep ceph | grep -v grep"
done
