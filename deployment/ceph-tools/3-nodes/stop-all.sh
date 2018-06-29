set -eu
source ./_env.sh

for node in ${nodes[@]}; do
	ssh "$node" "sudo systemctl stop ceph-mds@$node"
done

for node in ${nodes[@]}; do
	ssh "$node" "sudo systemctl stop ceph-mgr@$node"
done

for node in ${nodes[@]}; do
	ssh "$node" "sudo systemctl stop ceph-mon@$node"
done

for i in ${#nodes[@]}; do
	i=$(( $i - 1 ))
	ssh "${nodes[$i]}" "sudo systemctl stop ceph-osd@$i"
done

echo "status: (should be nothing)"
./status.sh
