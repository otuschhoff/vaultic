#!/bin/sh
set -eu

case "${1:-}" in
    vaultic|vaulticdb|vaultic-key-broker|vaultic-key-custodian)
        executable="/usr/bin/$1"
        shift
        ;;
    *)
        executable=/usr/bin/vaultic
        ;;
esac

if [ -n "${IONICE_CLASS:-}" ]; then
    set -- ionice -c "$IONICE_CLASS" -n "${IONICE_PRIORITY:-4}" "$executable" "$@"
else
    set -- "$executable" "$@"
fi

exec nice -n "${NICE:-0}" "$@"