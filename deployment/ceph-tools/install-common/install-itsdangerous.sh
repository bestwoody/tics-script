set -eu
curl -Ov http://mirror.centos.org/centos/7/extras/x86_64/Packages/python-itsdangerous-0.23-2.el7.noarch.rpm
rpm -i python-itsdangerous-0.23-2.el7.noarch.rpm
rm python-itsdangerous-0.23-2.el7.noarch.rpm
