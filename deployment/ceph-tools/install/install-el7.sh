set -eu

cd el7
./install.sh

cd ..
cd common
./install-ez.sh
./install-itsdangerous.sh
