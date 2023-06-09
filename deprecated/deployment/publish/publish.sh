#!/bin/bash

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
    name="tiflash-${repo_git_hash:0:6}"
fi

publish_dir="`pwd`"
deployment_dir="$publish_dir/.."
benchmark_dir="$publish_dir/../../benchmark"
storage_dir="$publish_dir/../../storage"
computing_dir="$publish_dir/../../computing"

echo "=> creating package info of $name"
info_file="$publish_dir/$name/package-info"
mkdir -p "`dirname $info_file`"
echo "[source git hash] $repo_git_hash" > "$info_file"
echo "[packing server os] `uname -a`" >> "$info_file"
echo "[packing date] `date`" >> "$info_file"

echo "=> packing publish package $name"
if [ "$build" == "true" ]; then
	echo "=> building theflash:"
	cd "$storage_dir" && ./build.sh && cd "$publish_dir"
fi

echo "=> copying theflash"
storage_pack="$publish_dir/$name/storage"
mkdir -p "$storage_pack"
cp -f "$storage_dir/build/dbms/src/Server/theflash" "$storage_pack/tiflash"
cp -f "$storage_dir/running/config/config.xml" "$storage_pack"
cp -f "$storage_dir/running/config/users.xml" "$storage_pack"

echo "=> copying libs"
dylibs_pack="$storage_pack"
ldd "$storage_dir/build/dbms/src/Server/theflash" | grep '/' | grep '=>' | \
	awk -F '=>' '{print $2}' | awk '{print $1}' | while read libfile; do
	cp -f "$libfile" "$storage_pack"
done

if [ "$build" == "true" ]; then
	echo "=> building tiflashspark:"
	cd "$computing_dir" && ./build.sh && cd "$publish_dir"
fi

echo "=> copying tiflashspark"
cp "$computing_dir/chspark/target/chspark-0.1.0-SNAPSHOT-jar-with-dependencies.jar" "$computing_dir/spark/jars/tiflashspark-0.1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "=> copying spark"
spark_dir="$computing_dir/spark"
spark_pack="$publish_dir/$name/spark"
rm -rf "$spark_pack"
cp -rf "$spark_dir" "$spark_pack"
cp -rf "$computing_dir/conf/spark-defaults.conf" "$spark_pack/conf/"

if [ "$build" == "true" ]; then
	echo "=> building tpch dbgen"
	cd "$benchmark_dir/tpch-dbgen" && make && cd "$publish_dir"
fi

echo "=> copying tiflash_proxy"
tiflash_proxy_pack="$publish_dir/$name/tiflash-proxy"
mkdir -p "$tiflash_proxy_pack"
wget -O "$tiflash_proxy_pack/tiflash-proxy" 139.219.11.38:8000/15JRou/tiflash-proxy
cp -f "$publish_dir/scripts/tiflash-proxy.toml" "$tiflash_proxy_pack/tiflash-proxy.toml"
cp -f "$publish_dir/scripts/storage-proxy-start.sh" "$publish_dir/$name"
cp -f "$publish_dir/scripts/storage-proxy-stop.sh" "$publish_dir/$name"
cp -f "$publish_dir/scripts/storage-proxy-pid.sh" "$publish_dir/$name"
chmod 744 "$tiflash_proxy_pack/tiflash-proxy"

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
cp -rf "$deployment_dir/ceph-tools" "$scripts_pack/"
cp -f "$computing_dir/spark-check-running.sh" "$scripts_pack"
cp -f "$computing_dir/spark-stop-all.sh" "$scripts_pack"
cp -f "$computing_dir/spark-start-all.sh" "$scripts_pack"
cp -f "$computing_dir/spark-start-master.sh" "$scripts_pack"
cp -f "$computing_dir/spark-start-slave.sh" "$scripts_pack"
cp -f "$computing_dir/j2s-r.sh" "$scripts_pack"
cp -f "$computing_dir/j2s-w.sh" "$scripts_pack"
cp -f "$computing_dir/j2s-w-baseline.sh" "$scripts_pack"
cp -rf "$computing_dir/j2s-cases" "$scripts_pack"
cp -f "$benchmark_dir/_helper.sh" "$scripts_pack"
cp -f "$benchmark_dir/_vars.sh" "$scripts_pack"
cp -f "$benchmark_dir/io-report.sh" "$scripts_pack"
cp -f "$benchmark_dir/hw-report.sh" "$scripts_pack"
cp -f "$benchmark_dir/clear-page-cache.sh" "$scripts_pack"
cp -f "$benchmark_dir/trace-table-compaction.sh" "$scripts_pack"
cp -f "$benchmark_dir/storage-server-start.sh" "$scripts_pack"
cp -f "$benchmark_dir/storage-server-stop.sh" "$scripts_pack"
cp -f "$benchmark_dir/storage-client.sh" "$scripts_pack"
cp -f "$benchmark_dir/storage-list-running-query.sh" "$scripts_pack"
cp -f "$benchmark_dir/storage-pid.sh" "$scripts_pack"
cp -f "$benchmark_dir/storage-cache-status.sh" "$scripts_pack"
cp -f "$benchmark_dir/storages-client.sh" "$scripts_pack"
cp -f "$benchmark_dir/storages-pid.sh" "$scripts_pack"
cp -f "$benchmark_dir/storages-dsh.sh" "$scripts_pack"
cp -f "$benchmark_dir/storages-spread-file.sh" "$scripts_pack"
cp -f "$benchmark_dir/storages-server-start.sh" "$scripts_pack"
cp -f "$benchmark_dir/storages-server-stop.sh" "$scripts_pack"
cp -f "$benchmark_dir/storages-cache-status.sh" "$scripts_pack"
cp -f "$benchmark_dir/storages-clear-page-cache.sh" "$scripts_pack"
cp -f "$benchmark_dir/refresh-schema.sh" "$scripts_pack"
cp -f "$benchmark_dir/spark-q.sh" "$scripts_pack"
cp -f "$benchmark_dir/tpch-spark-r.sh" "$scripts_pack"
cp -f "$benchmark_dir/tpch-gen-report.sh" "$scripts_pack"
cp -f "$benchmark_dir/tpch-gen-report.py" "$scripts_pack"
cp -f "$benchmark_dir/stable-test-ch-stable.sh" "$scripts_pack/tpch-stable-test.sh"
cp -f "$benchmark_dir/analyze-table-compaction.sh" "$scripts_pack/analyze-table-compaction.sh"
cp -f "$benchmark_dir/analyze-table-compaction.py" "$scripts_pack/analyze-table-compaction.py"
cp -f "$publish_dir/scripts/README.md" "$scripts_pack"
cp -f "$publish_dir/scripts/HOWTO.md" "$scripts_pack"
cp -f "$publish_dir/scripts/upgrade-cp-conf-from.unified" "$scripts_pack"
cp -f "$publish_dir/scripts/upgrade-cp-conf-from.pernode" "$scripts_pack"
cp -f $publish_dir/scripts/*.sh "$scripts_pack"
cp -f $publish_dir/scripts/tpch/load/*.sh "$tpch_pack/load"
cp -f "$storage_dir/dbg-invoke.sh" "$scripts_pack"

echo "=> packing to ./${name}.tar.gz (may take some time)"
tar -cvzf "./${name}.tar.gz" "$name"

echo "=> done"
