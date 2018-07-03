set -eu

sudo rpm --import 'https://download.ceph.com/keys/release.asc'

sudo cp ./ceph.repo /etc/yum.repos.d

sudo yum update -y
sudo yum install ntp ntpdate ntp-doc ceph-deploy -y
sudo yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
