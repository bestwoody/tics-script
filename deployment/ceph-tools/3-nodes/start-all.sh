set -eu

source ./_env.sh

for i in ${#nodes[@]}; do
	i=$(( $i - 1 ))
	ssh "${nodes[$i]}" "sudo systemctl start ceph-osd@$i"
done

for node in ${nodes[@]}; do
	ssh "$node" "sudo systemctl start ceph-mon@$node"
done

for node in ${nodes[@]}; do
	ssh "$node" "sudo systemctl start ceph-mgr@$node"
done

for node in ${nodes[@]}; do
	ssh "$node" "sudo systemctl start ceph-mds@$node"
done

sleep 1

sudo ceph -s
