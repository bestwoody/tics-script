set -eu

if [ `whoami` != "root" ]
	echo "need 'sudo' to run, exiting" >&2
	exit 1
fi

rpm --import 'https://download.ceph.com/keys/release.asc'

cp ./ceph.repo /etc/yum.repos.d

yum update -y
yum install ntp ntpdate ntp-doc ceph-deploy -y
yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
