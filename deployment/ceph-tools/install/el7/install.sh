#!/bin/bash

repo_url="$1"

set -eu

if [ `whoami` != "root" ]; then
	echo "need 'sudo' to run, exiting" >&2
	exit 1
fi

if [ -z "$repo_url" ]; then
	echo "usage: <bin> ceph-repo-url" >&2
	exit 1
fi

echo "=> sudo rpm --import ../../download/downloaded/release.asc"
sudo rpm --import ../../download/downloaded/release.asc

echo "=> creating ceph.repo"
repo_gpg="`readlink -f ../../download/downloaded/release.asc`"
cp -f ceph.repo.tmpl ceph.repo
sed -i "s#<repo_gpg>#file://${repo_gpg}#g" ceph.repo
sed -i "s#<repo_url>#${repo_url}#g" ceph.repo

echo "=> cp ./ceph.repo /etc/yum.repos.d"
cp ./ceph.repo /etc/yum.repos.d

echo "=> yum --skip-broken -y install ntp ntpdate ntp-doc ceph-deploy"
yum --skip-broken -y install ntp ntpdate ntp-doc ceph-deploy

epel_installed=`rpm -qa | grep epel`
if [ -z "$epel_installed" ]; then
	old=`pwd`
	cd ../../download/downloaded
	file="epel-release-latest-7.noarch.rpm"
	echo "=> sudo rpm -i $file"
	sudo rpm -i "$file"
	cd "$old"
fi
