#!/bin/sh
set -eu

if [ "$#" -eq 0 ]; then
    echo "usage: $0 BINARY..." >&2
    exit 2
fi

for binary in "$@"; do
    description=$(file -b "$binary")
    case "$description" in
        *ELF*statically\ linked*) ;;
        *)
            echo "$binary is not a statically linked ELF executable: $description" >&2
            exit 1
            ;;
    esac

    if command -v readelf >/dev/null 2>&1 && readelf -d "$binary" 2>/dev/null | grep -q '(NEEDED)'; then
        echo "$binary has dynamic library dependencies:" >&2
        readelf -d "$binary" | grep '(NEEDED)' >&2
        exit 1
    fi

    echo "$binary: statically linked"
done