set -eu

source ./_env.sh

echo "$h0:"
ssh "$h0" "ps -ef | grep ceph | grep -v grep"
echo "$h1:"
ssh "$h1" "ps -ef | grep ceph | grep -v grep"
echo "$h2:"
ssh "$h2" "ps -ef | grep ceph | grep -v grep"
