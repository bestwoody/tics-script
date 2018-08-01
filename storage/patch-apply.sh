#!/bin/bash

print_target_commit_hash()
{
	local target="$1"

	local old=`pwd`
	cd "$target"
	local hash=`git log HEAD -1 | grep commit | awk '{print $2}'`
	cd "$old"
	echo "$hash"
}

reset_target_file()
{
	local target="$1"
	local file="$2"

	local old=`pwd`
	cd "$target"
	git checkout "$file"
	cd "$old"
}

update_target_unpatched_cache()
{
	local target="$1"

	mkdir -p "$target-cache"
	local hash_file="$target-cache/_git_hash"
	if [ ! -f "$hash_file" ] || [ "`print_target_commit_hash $target`" != "`cat $hash_file`" ]; then
		echo "target repo updated, removing all cache files in $target-path" >&2
		rm -rf "$target-cache/*"
	fi

	find "$target-patch" -type f | while read patch_file; do
		local raw_file="${patch_file#*patch}"
		local patch_ext="${patch_file##*.}"
		if [ "$patch_ext" == "patch" ]; then
			raw_file="${raw_file%.patch}"
		fi
		local origin_file="$target""$raw_file"

		local cache_file="$target-cache""$raw_file"
		local cache_path=`dirname "$cache_file"`
		mkdir -p "$cache_path"

		if [ -f "$cache_file" ]; then
			continue
		fi

		if [ "$patch_ext" == "patch" ]; then
			if [ ! -f "$origin_file" ]; then
				echo "origin file $origin_file missed, aborted" >&2
			fi
			reset_target_file "$target" ".${raw_file}"
			echo cp "$origin_file" "$cache_file"
			cp "$origin_file" "$cache_file"
		else
			echo cp "$patch_file" "$cache_file"
			cp "$patch_file" "$cache_file"
		fi
	done

	print_target_commit_hash $target > $hash_file
}

patch_apply()
{
	local target="$1"

	update_target_unpatched_cache "$target"
	echo "cache updated"

	find "$target-patch" -type f | while read patch_file; do
		local raw_file="${patch_file#*patch}"
		local patch_ext="${patch_file##*.}"
		if [ "$patch_ext" == "patch" ]; then
			raw_file="${raw_file%.patch}"
		fi

		local origin_file="$target""$raw_file"
		mkdir -p `dirname "$origin_file"`

		local cache_file="$target-cache""$raw_file"
		local patching_file="$cache_file.patching"

		if [ "$patch_ext" == "patch" ]; then
			cp "$cache_file" "$patching_file"
			patch -p0 "$patching_file" < "$patch_file"
			if [ -z "`diff $patching_file $origin_file`" ]; then
				echo "ignore patching '$origin_file'"
			else
				echo "patching '$origin_file' with '$patch_file'"
				mv "$patching_file" "$origin_file"
			fi
		else
			if [ -f "$origin_file" ] && [ -z "`diff $patch_file $origin_file`" ]; then
				echo "ignore cp '$origin_file'"
			else
				echo "cp '$origin_file'"
				cp "$patch_file" "$origin_file"
			fi
		fi
		echo "OK"
	done
}

patch_apply_contrib()
{
	local contrib="$1"

	rm -rf "$this_dir/ch/contrib/${contrib}-patch"
	rm -rf "$this_dir/ch/contrib/${contrib}-cache"

	cd "$this_dir/ch/contrib/"

	cp -r "$this_dir/${contrib}-patch" "$this_dir/ch/contrib/"
	if [ -d "$this_dir/${contrib}-cache" ]; then
		cp -r "$this_dir/${contrib}-cache" "$this_dir/ch/contrib/"
	fi

	patch_apply "${contrib}"

	rm -rf "$this_dir/${contrib}-cache"
	mv "$this_dir/ch/contrib/${contrib}-cache" "$this_dir/"

	rm -rf "$this_dir/ch/contrib/${contrib}-patch"
	rm -rf "$this_dir/ch/contrib/${contrib}-cache"

	cd "$this_dir"
}

this_dir=$(cd $(dirname $0); echo $PWD)

set -eu

patch_apply "ch"
patch_apply_contrib "poco"
patch_apply_contrib "capnproto"


