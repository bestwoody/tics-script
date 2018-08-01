set -eu

itsdangerous_installed=`rpm -qa | grep itsdangerous`

if [ ! -z "$itsdangerous_installed" ]; then
	echo "=> python-itsdangerous installed, skipped"
else
	file="python-itsdangerous-0.23-2.el7.noarch.rpm"
	rm -f "$file"

	echo "=> curl -Ov http://mirror.centos.org/centos/7/extras/x86_64/Packages/$file"
	curl -Ov "http://mirror.centos.org/centos/7/extras/x86_64/Packages/$file"

	echo "=> sudo rpm -i $file"
	sudo rpm -i "$file"

	rm -f "$file"
fi
