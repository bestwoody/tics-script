set -eu

sudo rpm --import 'https://download.ceph.com/keys/release.asc'

sudo cp ./ceph.repo /etc/yum.repos.d

#su -c 'rpm -Uvh https://download.ceph.com/rpms/el7/x86_64/ceph-{release}.el7.noarch.rpm'
sudo yum install ceph-deploy -y
