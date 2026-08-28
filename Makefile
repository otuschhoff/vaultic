.PHONY: all build clean test \
	vaultic vaultic-arm64 vaultic-amd64 \
	vaulticdb vaulticdb-arm64 vaulticdb-amd64 \
	vaulticdb-proto vaulticdb-musl vaulticdb-smoke

BIN_DIR := bin
VAULTICDB_RUST_TOOLCHAIN ?= stable

all: build

# Alias to build both deliverables for the current arch into bin/.
build: vaultic vaulticdb

clean:
	rm -rf $(BIN_DIR)

test:
	go test ./cmd/... ./internal/...

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

# --- vaultic (Go CLI) ---

vaultic: | $(BIN_DIR)
	go run build.go -o $(BIN_DIR)/vaultic

vaultic-arm64: | $(BIN_DIR)
	go run build.go --goos darwin --goarch arm64 -o $(BIN_DIR)/vaultic-darwin-arm64

vaultic-amd64: | $(BIN_DIR)
	go run build.go --goos darwin --goarch amd64 -o $(BIN_DIR)/vaultic-darwin-amd64

# --- vaulticdb (Rust daemon) ---

vaulticdb: | $(BIN_DIR)
	rustup run $(VAULTICDB_RUST_TOOLCHAIN) cargo build --manifest-path vaulticdb/Cargo.toml --release
	cp vaulticdb/target/release/vaulticdb $(BIN_DIR)/vaulticdb

vaulticdb-arm64: | $(BIN_DIR)
	./vaulticdb/check-rust-target.sh aarch64-apple-darwin "$(VAULTICDB_RUST_TOOLCHAIN)"
	rustup run $(VAULTICDB_RUST_TOOLCHAIN) cargo build --manifest-path vaulticdb/Cargo.toml --release --target aarch64-apple-darwin
	cp vaulticdb/target/aarch64-apple-darwin/release/vaulticdb $(BIN_DIR)/vaulticdb-darwin-arm64

vaulticdb-amd64: | $(BIN_DIR)
	./vaulticdb/check-rust-target.sh x86_64-apple-darwin "$(VAULTICDB_RUST_TOOLCHAIN)"
	rustup run $(VAULTICDB_RUST_TOOLCHAIN) cargo build --manifest-path vaulticdb/Cargo.toml --release --target x86_64-apple-darwin
	cp vaulticdb/target/x86_64-apple-darwin/release/vaulticdb $(BIN_DIR)/vaulticdb-darwin-amd64

vaulticdb-proto:
	./vaulticdb/generate-proto.sh

vaulticdb-musl:
	./vaulticdb/build-musl.sh

vaulticdb-smoke:
	VAULTICDB_NATIVE_SMOKE=1 rustup run $(VAULTICDB_RUST_TOOLCHAIN) cargo run --manifest-path vaulticdb/Cargo.toml --quiet


