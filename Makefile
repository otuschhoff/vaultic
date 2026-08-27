.PHONY: all clean test vaultic vaulticdb vaulticdb-proto vaulticdb-musl vaulticdb-smoke

all: vaultic

vaultic:
	go run build.go

clean:
	rm -f vaultic

test:
	go test ./cmd/... ./internal/...

vaulticdb:
	rustup run $${VAULTICDB_RUST_TOOLCHAIN:-stable} cargo build --manifest-path vaulticdb/Cargo.toml --release

vaulticdb-proto:
	./vaulticdb/generate-proto.sh

vaulticdb-musl:
	./vaulticdb/build-musl.sh

vaulticdb-smoke:
	VAULTICDB_NATIVE_SMOKE=1 rustup run $${VAULTICDB_RUST_TOOLCHAIN:-stable} cargo run --manifest-path vaulticdb/Cargo.toml --quiet

