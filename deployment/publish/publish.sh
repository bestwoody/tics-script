build="$1"
name="$2"

set -eu

if [ `uname` == "Darwin" ]; then
	echo "this script should be running on compiling server, not a mac developing env, exiting" >&1
	exit 1
fi

if [ -z "$build" ]; then
    build="false"
fi

repo_git_hash=`git log HEAD -1 | head -n 1 | awk '{print $2}'`
if [ -z "$name" ]; then
    name="theflash-${repo_git_hash:0-6:6}"
fi

publish_dir="`pwd`"
benchmark_dir="$publish_dir/../../benchmark"
storage_dir="$publish_dir/../../ch-connector"
computing_dir="$publish_dir/../../spark-connector"

echo "=> creating package info of $name"
info_file="$publish_dir/$name/package-info"
mkdir -p "`dirname $info_file`"
echo "[source git hash] $repo_git_hash" > "$info_file"
echo "[packing server os] `uname -a`" >> "$info_file"

echo "=> packing publish package $name"
if [ "$build" == "true" ]; then
	echo "=> building theflash:"
	cd "$storage_dir" && ./build.sh && cd "$publish_dir"
fi

echo "=> copying theflash"
storage_pack="$publish_dir/$name/storage"
mkdir -p "$storage_pack"
cp -f "$storage_dir/build/dbms/src/Server/theflash" "$storage_pack"
cp -f "$storage_dir/running/config/config.xml" "$storage_pack"
cp -f "$storage_dir/running/config/users.xml" "$storage_pack"

echo "=> copying libs"
dylibs_pack="$storage_pack"
ldd "$storage_dir/build/dbms/src/Server/theflash" | grep '/' | grep '=>' | \
	awk -F '=>' '{print $2}' | awk '{print $1}' | while read libfile; do
	cp -f "$libfile" "$storage_pack"
done

if [ "$build" == "true" ]; then
	echo "=> building spark:"
	cd "$computing_dir/spark" && build/mvn -DskipTests package && cd "$publish_dir"
	echo "=> building chspark:"
	cd "$computing_dir" && ./build.sh && cd "$publish_dir"
fi

echo "=> copying spark (with chspark)"
spark_dir="$computing_dir/spark"
spark_pack="$publish_dir/$name/spark"
mkdir -p "$spark_pack"
cp -rf "$spark_dir/sbin" "$spark_pack"
cp -rf "$spark_dir/bin" "$spark_pack"
cp -rf "$spark_dir/conf" "$spark_pack"
cp -rf "$spark_dir/assembly" "$spark_pack"
cp -rf "$publish_dir/scripts/spark/spark-defaults.conf" "$spark_pack/conf/"

if [ "$build" == "true" ]; then
	echo "=> building tpch dbgen"
	cd "$benchmark_dir/tpch-dbgen" && make cd "$publish_dir"
fi

echo "=> copying tpch dbgen"
tpch_pack="$publish_dir/$name/tpch"
mkdir -p "$tpch_pack"
cp -f "$benchmark_dir/tpch-dbgen/dbgen" "$tpch_pack"
cp -f "$benchmark_dir/tpch-dbgen/dists.dss" "$tpch_pack"

echo "=> copying tpch data loader and env"
cp -rf "$benchmark_dir/tpch-sql" "$tpch_pack/sql"
cp -rf "$benchmark_dir/tpch-load" "$tpch_pack/load"

echo "=> copying scripts"
scripts_pack="$publish_dir/$name"
cp -f "$computing_dir/spark-check-running.sh" "$scripts_pack"
cp -f "$computing_dir/spark-stop-all.sh" "$scripts_pack"
cp -f "$computing_dir/spark-start-all.sh" "$scripts_pack"
cp -f "$benchmark_dir/storage-server.sh" "$scripts_pack"
cp -f "$benchmark_dir/storage-client.sh" "$scripts_pack"
cp -f "$benchmark_dir/storage-list-running-query.sh" "$scripts_pack"
cp -f "$benchmark_dir/spark-q.sh" "$scripts_pack"
cp -f "$benchmark_dir/tpch-spark-r.sh" "$scripts_pack"
cp -f "$benchmark_dir/tpch-gen-report.sh" "$scripts_pack"
cp -f "$benchmark_dir/tpch-gen-report.py" "$scripts_pack"
cp -f "$benchmark_dir/stable-test-ch-stable.sh" "$scripts_pack/stable-test.sh"
cp -f "$publish_dir/README.md" "$scripts_pack"
cp -f $publish_dir/scripts/*.sh "$scripts_pack"
cp -f $publish_dir/scripts/tpch/load/*.sh "$tpch_pack/load"

echo "=> packing to ./${name}.tar.gz (may take some time)"
tar -cvzf "./${name}.tar.gz" "$name"

echo "=> done"
