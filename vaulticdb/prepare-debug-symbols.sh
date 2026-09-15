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
		readelf=${READELF:-readelf}
		command -v "$objcopy" >/dev/null 2>&1 || {
			echo "debug symbols: $objcopy is required" >&2
			exit 1
		}
		command -v "$readelf" >/dev/null 2>&1 || {
			echo "debug symbols: $readelf is required" >&2
			exit 1
		}
		for binary in $binaries; do
			debug_file="$debug_dir/$binary.debug"
			build_id=$(
				"$readelf" --notes "$binary_dir/$binary" |
					awk '/Build ID:/ { print $3; exit }'
			)
			test -n "$build_id" || {
				echo "debug symbols: $binary has no ELF Build ID" >&2
				exit 1
			}
			"$objcopy" --only-keep-debug "$binary_dir/$binary" "$debug_file"
			test -s "$debug_file"
			"$objcopy" --strip-all "$binary_dir/$binary"
			(
				cd "$binary_dir"
				"$objcopy" --add-gnu-debuglink="$debug_file" "$binary"
			)
			stripped_build_id=$(
				"$readelf" --notes "$binary_dir/$binary" |
					awk '/Build ID:/ { print $3; exit }'
			)
			debug_build_id=$(
				"$readelf" --notes "$debug_file" |
					awk '/Build ID:/ { print $3; exit }'
			)
			test "$build_id" = "$stripped_build_id" && test "$build_id" = "$debug_build_id" || {
				echo "debug symbols: Build ID mismatch for $binary" >&2
				exit 1
			}
			"$readelf" --string-dump=.gnu_debuglink "$binary_dir/$binary" |
				grep -Fq "$binary.debug" || {
				echo "debug symbols: missing .gnu_debuglink for $binary" >&2
				exit 1
			}
			printf '%s  %s\n' "$build_id" "$binary" >>"$debug_dir/BUILDIDS"
		done
		;;
	macos)
		command -v dsymutil >/dev/null 2>&1 || {
			echo "debug symbols: dsymutil is required" >&2
			exit 1
		}
		command -v dwarfdump >/dev/null 2>&1 || {
			echo "debug symbols: dwarfdump is required" >&2
			exit 1
		}
		for binary in $binaries; do
			uuid=$(dwarfdump --uuid "$binary_dir/$binary" | awk 'NR == 1 { print $2 }')
			test -n "$uuid" || {
				echo "debug symbols: $binary has no Mach-O UUID" >&2
				exit 1
			}
			dsymutil "$binary_dir/$binary" -o "$debug_dir/$binary.dSYM"
			test -s "$debug_dir/$binary.dSYM/Contents/Resources/DWARF/$binary"
			strip -S "$binary_dir/$binary"
			stripped_uuid=$(dwarfdump --uuid "$binary_dir/$binary" | awk 'NR == 1 { print $2 }')
			debug_uuid=$(dwarfdump --uuid "$debug_dir/$binary.dSYM" | awk 'NR == 1 { print $2 }')
			test "$uuid" = "$stripped_uuid" && test "$uuid" = "$debug_uuid" || {
				echo "debug symbols: UUID mismatch for $binary" >&2
				exit 1
			}
			printf '%s  %s\n' "$uuid" "$binary" >>"$debug_dir/UUIDS"
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

Build IDs are listed in BUILDIDS. Verify a pair with:
	readelf --notes ./vaulticdb | grep 'Build ID'
	readelf --notes ./vaulticdb.debug | grep 'Build ID'

Keep vaulticdb.debug beside the exact vaulticdb executable used for recording.
GNU debuglink lets perf load it automatically:
	perf record -g -- ./vaulticdb [arguments]
	perf report --input perf.data
EOF
		;;
	macos)
		cat >>"$debug_dir/README.txt" <<EOF
Load the matching .dSYM bundle in LLDB or your profiler. Its UUID matches the executable.
Matching executable and dSYM UUIDs are listed in UUIDS.
EOF
		;;
esac