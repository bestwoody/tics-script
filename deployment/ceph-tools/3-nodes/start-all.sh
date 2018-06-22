set -eu

source ./_env.sh

ssh "$h0" "sudo systemctl start ceph-osd@0"
ssh "$h1" "sudo systemctl start ceph-osd@1"
ssh "$h2" "sudo systemctl start ceph-osd@2"

ssh "$h0" "sudo systemctl start ceph-mon@$h0"
ssh "$h1" "sudo systemctl start ceph-mon@$h1"
ssh "$h2" "sudo systemctl start ceph-mon@$h2"

ssh "$h0" "sudo systemctl start ceph-mgr@$h0"
ssh "$h1" "sudo systemctl start ceph-mgr@$h1"
ssh "$h2" "sudo systemctl start ceph-mgr@$h2"

ssh "$h0" "sudo systemctl start ceph-mds@$h0"
ssh "$h1" "sudo systemctl start ceph-mds@$h1"
ssh "$h2" "sudo systemctl start ceph-mds@$h2"

sleep 1

sudo ceph -s
