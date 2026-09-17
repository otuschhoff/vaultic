#!/bin/sh

set -eu

if [ "$#" -lt 2 ] || [ "$#" -gt 5 ]; then
	echo "usage: $0 <pid> <output-directory> [seconds] [fp|dwarf] [debug-file]" >&2
	exit 2
fi

pid=$1
output_dir=$2
seconds=${3:-30}
call_graph=${4:-dwarf}
debug_file=${5:-}

case "$pid" in
	*[!0-9]* | "")
		echo "profile: pid must be a positive integer" >&2
		exit 2
		;;
esac
case "$seconds" in
	*[!0-9]* | "" | 0)
		echo "profile: seconds must be a positive integer" >&2
		exit 2
		;;
esac
case "$call_graph" in
	fp) record_call_graph=fp ;;
	dwarf) record_call_graph=dwarf,8192 ;;
	*)
		echo "profile: call graph must be 'fp' or 'dwarf'" >&2
		exit 2
		;;
esac

for command_name in perf readelf timeout; do
	command -v "$command_name" >/dev/null 2>&1 || {
		echo "profile: $command_name is required" >&2
		exit 1
	}
done

test -r "/proc/$pid/exe" || {
	echo "profile: cannot read /proc/$pid/exe" >&2
	exit 1
}

mkdir -p "$output_dir"
output_dir=$(cd "$output_dir" && pwd)
for artifact in perf.data perf.raw.data flat.txt callgraph.txt BUILDID; do
	test ! -e "$output_dir/$artifact" || {
		echo "profile: output directory already contains $artifact" >&2
		exit 1
	}
done

executable_path=$(readlink "/proc/$pid/exe")
case "$executable_path" in
	/*) ;;
	*)
		echo "profile: process executable path is not absolute: $executable_path" >&2
		exit 1
		;;
esac
snapshot="$output_dir/symfs$executable_path"
mkdir -p "$(dirname "$snapshot")"
cp "/proc/$pid/exe" "$snapshot"

build_id=$(
	readelf --notes "$snapshot" |
		awk '/Build ID:/ { print $3; exit }'
)
test -n "$build_id" || {
	echo "profile: process executable has no ELF Build ID" >&2
	exit 1
}
printf '%s  %s\n' "$build_id" "$executable_path" >"$output_dir/BUILDID"

if ! readelf --sections "$snapshot" | grep -Fq '.debug_info'; then
	debug_name=$(
		readelf --string-dump=.gnu_debuglink "$snapshot" 2>/dev/null |
			awk '/\[[[:space:]]*[0-9a-f]+\]/{ print $NF; exit }'
	)
	if [ -z "$debug_file" ] && [ -n "$debug_name" ]; then
		for candidate in \
			"$(dirname "$executable_path")/$debug_name" \
			"$(dirname "$executable_path")/.debug/$debug_name"; do
			if [ -f "$candidate" ]; then
				debug_file=$candidate
				break
			fi
		done
	fi
	test -n "$debug_file" && test -f "$debug_file" || {
		echo "profile: exact DWARF file is required for stripped executable $executable_path" >&2
		echo "profile: pass it as the fifth argument" >&2
		exit 1
	}
	debug_build_id=$(
		readelf --notes "$debug_file" |
			awk '/Build ID:/ { print $3; exit }'
	)
	test "$build_id" = "$debug_build_id" || {
		echo "profile: executable/debug Build ID mismatch" >&2
		exit 1
	}
	readelf --sections "$debug_file" | grep -Fq '.debug_info' || {
		echo "profile: $debug_file contains no DWARF debug information" >&2
		exit 1
	}
	mkdir -p "$(dirname "$snapshot")/.debug"
	cp "$debug_file" "$(dirname "$snapshot")/.debug/${debug_name:-$(basename "$debug_file")}"
	build_id_dir="$output_dir/symfs/usr/lib/debug/.build-id/$(printf '%.2s' "$build_id")"
	mkdir -p "$build_id_dir"
	cp "$debug_file" "$build_id_dir/${build_id#??}.debug"
fi

perf record -q -F 99 --call-graph "$record_call_graph" -p "$pid" \
	-o "$output_dir/perf.raw.data" -- sleep "$seconds"
perf inject --build-ids --known-build-ids="$build_id,$executable_path" \
	-i "$output_dir/perf.raw.data" \
	-o "$output_dir/perf.data"

perf report --stdio -i "$output_dir/perf.data" --symfs "$output_dir/symfs" \
	--no-children --no-inline --call-graph none --sort comm,dso,symbol \
	>"$output_dir/flat.txt"

report_timeout=${VAULTICDB_PERF_REPORT_TIMEOUT:-120}
if ! timeout "$report_timeout" perf report --stdio \
	-i "$output_dir/perf.data" --symfs "$output_dir/symfs" --no-inline \
	--call-graph graph,0.5,caller,function --sort comm,dso,symbol \
	>"$output_dir/callgraph.txt"; then
	echo "profile: call-graph report exceeded ${report_timeout}s; flat.txt is complete" >&2
	rm -f "$output_dir/callgraph.txt"
fi

echo "profile: build ID $build_id"
echo "profile: flat report $output_dir/flat.txt"
test ! -f "$output_dir/callgraph.txt" || \
	echo "profile: call-graph report $output_dir/callgraph.txt"