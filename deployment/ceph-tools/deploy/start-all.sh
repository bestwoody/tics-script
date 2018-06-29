set -eu

source ./_env.sh

for i in ${!nodes[@]}; do
	ssh "${nodes[$i]}" "sudo systemctl start ceph-osd@$i"
done

for i in ${!nodes[@]}; do
	if [ $i -gt 2 ]; then
		continue
	fi
	node=${nodes[$i]}
	ssh "$node" "sudo systemctl start ceph-mon@$node"
done

for i in ${!nodes[@]}; do
	if [ $i -gt 2 ]; then
		continue
	fi
	node=${nodes[$i]}
	ssh "$node" "sudo systemctl start ceph-mgr@$node"
done

for i in ${!nodes[@]}; do
	if [ $i -gt 2 ]; then
		continue
	fi
	node=${nodes[$i]}
	ssh "$node" "sudo systemctl start ceph-mds@$node"
done

sleep 1

sudo ceph -s
