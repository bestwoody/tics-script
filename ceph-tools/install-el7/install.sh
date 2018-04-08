set -eu

sudo rpm --import 'https://download.ceph.com/keys/release.asc'

sudo cp ./ceph.repo /etc/yum.repos.d

sudo yum install ntp ntpdate ntp-doc ceph-deploy -y

ceph-deploy repo --repo-url http://mirrors.ustc.edu.cn/ceph/rpm-luminous/el7/x86_64/ ceph-repo h0
