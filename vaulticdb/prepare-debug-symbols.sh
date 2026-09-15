#!/bin/sh

set -eu

if [ "$#" -ne 3 ]; then
	echo "usage: $0 <linux|macos> <binary-directory> <debug-directory>" >&2
	exit 2
fi

os=$1
binary_dir=$2
debug_dir=$3
binaries="vaulticdb vaultic-key-broker vaultic-key-custodian"

rm -rf "$debug_dir"
mkdir -p "$debug_dir"
binary_dir=$(cd "$binary_dir" && pwd)
debug_dir=$(cd "$debug_dir" && pwd)

case "$os" in
	linux)
		objcopy=${OBJCOPY:-objcopy}
		command -v "$objcopy" >/dev/null 2>&1 || {
			echo "debug symbols: $objcopy is required" >&2
			exit 1
		}
		for binary in $binaries; do
			debug_file="$debug_dir/$binary.debug"
			"$objcopy" --only-keep-debug "$binary_dir/$binary" "$debug_file"
			test -s "$debug_file"
			"$objcopy" --strip-all "$binary_dir/$binary"
			(
				cd "$binary_dir"
				"$objcopy" --add-gnu-debuglink="$debug_file" "$binary"
			)
		done
		;;
	macos)
		command -v dsymutil >/dev/null 2>&1 || {
			echo "debug symbols: dsymutil is required" >&2
			exit 1
		}
		for binary in $binaries; do
			dsymutil "$binary_dir/$binary" -o "$debug_dir/$binary.dSYM"
			test -s "$debug_dir/$binary.dSYM/Contents/Resources/DWARF/$binary"
			strip -S "$binary_dir/$binary"
		done
		;;
	*)
		echo "debug symbols: unsupported operating system '$os'" >&2
		exit 2
		;;
esac

cat >"$debug_dir/README.txt" <<EOF
Debug symbols for VaulticDB Rust executables.

These symbols match the executables in the corresponding Vaultic release archive.
EOF

case "$os" in
	linux)
		cat >>"$debug_dir/README.txt" <<EOF
Place each .debug file beside its matching executable or in a .debug subdirectory.
EOF
		;;
	macos)
		cat >>"$debug_dir/README.txt" <<EOF
Load the matching .dSYM bundle in LLDB or your profiler. Its UUID matches the executable.
EOF
		;;
esac