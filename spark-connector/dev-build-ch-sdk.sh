set -eu

old=`pwd`
cd ch-sdk
mvn clean install
cd "$old"

echo
echo "OK"
