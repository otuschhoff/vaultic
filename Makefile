.PHONY: all clean test vaultic vaulticd vaulticd-proto vaulticd-musl vaulticd-smoke

all: vaultic

vaultic:
	go run build.go

clean:
	rm -f vaultic

test:
	go test ./cmd/... ./internal/...

vaulticd:
	rustup run $${VAULTICD_RUST_TOOLCHAIN:-stable} cargo build --manifest-path vaulticd/Cargo.toml --release

vaulticd-proto:
	./vaulticd/generate-proto.sh

vaulticd-musl:
	./vaulticd/build-musl.sh

vaulticd-smoke:
	VAULTICD_NATIVE_SMOKE=1 rustup run $${VAULTICD_RUST_TOOLCHAIN:-stable} cargo run --manifest-path vaulticd/Cargo.toml --quiet

