source _env.sh

cp "$repo_dir/spark-connector/conf/spark-defaults.conf" "$repo_dir/spark-connector/spark/conf/"
if [ "$?" != "0" ]; then
	echo "Copy config file to spark failed." >&2
	exit 1
fi

$repo_dir/spark-connector/spark/bin/spark-shell $@
