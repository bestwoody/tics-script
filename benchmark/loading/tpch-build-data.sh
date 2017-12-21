if [ ! -n "$1" ]; then
        echo "usage: Please enter the generated N data size" >&2
	echo "Please enter the generated data size"
	echo "Usage: <bin> [ 1 | 10 | 100 | N  GB]"
        exit 1
fi

if [ -d "tpch-1-c$1" ]; then
        rm -rf tpch-1-c$1
fi
cd ../tpch-dbgen
make clean
make
./dbgen -s $1
if [ ! -d "../loading/tpch-1-c$1" ]; then
	mkdir -p ../loading/tpch-1-c$1
fi
mv ./*.tbl ../loading/tpch-1-c$1
ls -lh ../loading/tpch-1-c$1 |awk -F ' ' '{ print $9"\t"$5}'
