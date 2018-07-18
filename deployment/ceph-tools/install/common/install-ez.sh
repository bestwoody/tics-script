set -eu

file="setuptools-33.1.1.zip"

rm -f "$file"
curl https://bootstrap.pypa.io/ez_setup.py | sudo python
rm -f "$file"
