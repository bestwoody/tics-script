set -eu

ssh "$h0" "sudo systemctl stop ceph-mds@$h0"
ssh "$h1" "sudo systemctl stop ceph-mds@$h1"
ssh "$h2" "sudo systemctl stop ceph-mds@$h2"

ssh "$h0" "sudo systemctl stop ceph-mgr@$h0"
ssh "$h1" "sudo systemctl stop ceph-mgr@$h1"
ssh "$h2" "sudo systemctl stop ceph-mgr@$h2"

ssh "$h0" "sudo systemctl stop ceph-mon@$h0"
ssh "$h1" "sudo systemctl stop ceph-mon@$h1"
ssh "$h2" "sudo systemctl stop ceph-mon@$h2"

ssh "$h0" "sudo systemctl stop ceph-osd@0"
ssh "$h1" "sudo systemctl stop ceph-osd@1"
ssh "$h2" "sudo systemctl stop ceph-osd@2"

echo "status: (should be nothing)"
./status.sh
