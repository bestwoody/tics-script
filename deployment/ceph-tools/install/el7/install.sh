set -eu

if [ `whoami` != "root" ]; then
	echo "need 'sudo' to run, exiting" >&2
	exit 1
fi

echo "=> rpm --import 'https://download.ceph.com/keys/release.asc'"
rpm --import 'https://download.ceph.com/keys/release.asc'

echo "=> yum --skip-broken update -y"
yum --skip-broken update -y

echo "=> cp ./ceph.repo /etc/yum.repos.d"
cp ./ceph.repo /etc/yum.repos.d

echo "=> yum --skip-broken install ntp ntpdate ntp-doc ceph-deploy -y"
yum --skip-broken install ntp ntpdate ntp-doc ceph-deploy -y

epel_installed=`rpm -qa | grep epel`
if [ -z "$epel_intalled" ]; then
	echo "=> yum --skip-broken install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm -y"
	yum --skip-broken install https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm -y
fi
