set -eu

name="spark-2.1.1-bin-hadoop2.7"
file="$name.tgz"
url="http://download.pingcap.org/$file"

echo "=> downloading $file"
wget "$url"

echo "=> unpacking $file"
tar -xvzf "$file"

echo "=> remove $file"
rm -f "$file"

echo "=> mv $name to spark"
mv "$name" spark

echo "OK"
