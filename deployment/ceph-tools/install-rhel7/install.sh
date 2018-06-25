set -eu

sudo rpm --import 'https://download.ceph.com/keys/release.asc'

sudo cp ./ceph.repo /etc/yum.repos.d

sudo yum update
sudo yum install ntp ntpdate ntp-doc ceph-deploy -y
