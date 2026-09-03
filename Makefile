PLATFORMS := macos-arm64 linux-amd64 linux-arm64

.PHONY: all build clean test vaultic vaulticdb \
	vaulticdb-proto vaulticdb-musl vaulticdb-smoke

BIN_DIR := bin
VAULTICDB_RUST_TOOLCHAIN ?= stable

# Map uname -s/-m to one of $(PLATFORMS). Empty if the host isn't supported
# (e.g. Intel Macs, which are not one of the supported build targets).
HOST_OS := $(shell uname -s)
HOST_ARCH := $(shell uname -m)
ifeq ($(HOST_OS),Darwin)
	ifeq ($(HOST_ARCH),arm64)
		HOST_PLATFORM := macos-arm64
	endif
else ifeq ($(HOST_OS),Linux)
	ifeq ($(HOST_ARCH),x86_64)
		HOST_PLATFORM := linux-amd64
	else ifeq ($(HOST_ARCH),aarch64)
		HOST_PLATFORM := linux-arm64
	endif
endif

all: build

# Alias to build the CLI, metadata daemon, and key broker for the host platform.
build: vaultic vaulticdb

clean:
	rm -rf $(BIN_DIR)

test:
	go test ./cmd/... ./internal/...

# --- vaultic (Go CLI) ---
# Always built with CGO disabled (build.go defaults to CGO_ENABLED=0 unless
# --enable-cgo is passed, which these recipes never do), which also yields a
# statically linked binary on Linux. macOS binaries can never be fully static
# (Mach-O always dynamically links libSystem); CGO-disabled is the closest
# equivalent there.

vaultic: vaultic-$(HOST_PLATFORM)

vaultic-%:
	@case "$*" in \
		macos-arm64) goos=darwin; goarch=arm64 ;; \
		linux-amd64) goos=linux; goarch=amd64 ;; \
		linux-arm64) goos=linux; goarch=arm64 ;; \
		"") echo "vaultic: unsupported host platform ($(HOST_OS)/$(HOST_ARCH)); use one of: $(PLATFORMS)" >&2; exit 1 ;; \
		*) echo "vaultic: unsupported platform '$*'; supported: $(PLATFORMS)" >&2; exit 1 ;; \
	esac; \
	mkdir -p $(BIN_DIR)/$*; \
	go run build.go --goos "$$goos" --goarch "$$goarch" -o $(BIN_DIR)/$*/vaultic

# --- vaulticdb (Rust daemon and key broker) ---
# Linux targets use the *-musl target triple (statically linked by default)
# built via cargo-zigbuild for cross-linking from macOS. The macOS target
# builds natively via cargo; true static linking isn't possible on Darwin.

vaulticdb: vaulticdb-$(HOST_PLATFORM)

vaulticdb-%:
	@case "$*" in \
		macos-arm64) target=aarch64-apple-darwin ;; \
		linux-amd64) target=x86_64-unknown-linux-musl ;; \
		linux-arm64) target=aarch64-unknown-linux-musl ;; \
		"") echo "vaulticdb: unsupported host platform ($(HOST_OS)/$(HOST_ARCH)); use one of: $(PLATFORMS)" >&2; exit 1 ;; \
		*) echo "vaulticdb: unsupported platform '$*'; supported: $(PLATFORMS)" >&2; exit 1 ;; \
	esac; \
	./vaulticdb/check-rust-target.sh "$$target" "$(VAULTICDB_RUST_TOOLCHAIN)"; \
	mkdir -p $(BIN_DIR)/$*; \
	case "$$target" in \
		*-musl) \
			command -v cargo-zigbuild >/dev/null 2>&1 || { echo "vaulticdb: cargo-zigbuild is required for $$target (install with: cargo install cargo-zigbuild)" >&2; exit 1; }; \
			RUSTUP_TOOLCHAIN=$(VAULTICDB_RUST_TOOLCHAIN) cargo-zigbuild zigbuild --manifest-path vaulticdb/Cargo.toml --target "$$target" --release; \
			helper_target=$${target%-musl}-gnu; \
			./vaulticdb/check-rust-target.sh "$$helper_target" "$(VAULTICDB_RUST_TOOLCHAIN)"; \
			RUSTUP_TOOLCHAIN=$(VAULTICDB_RUST_TOOLCHAIN) cargo-zigbuild zigbuild --manifest-path vaulticdb/Cargo.toml --target "$$helper_target" --release --bin vaultic-key-custodian ;; \
		*) \
			rustup run $(VAULTICDB_RUST_TOOLCHAIN) cargo build --manifest-path vaulticdb/Cargo.toml --release --target "$$target" ;; \
	esac; \
	cp vaulticdb/target/$$target/release/vaulticdb $(BIN_DIR)/$*/vaulticdb; \
	cp vaulticdb/target/$$target/release/vaultic-key-broker $(BIN_DIR)/$*/vaultic-key-broker; \
	helper_target=$$target; case "$$target" in *-musl) helper_target=$${target%-musl}-gnu ;; esac; \
	cp vaulticdb/target/$$helper_target/release/vaultic-key-custodian $(BIN_DIR)/$*/vaultic-key-custodian

vaulticdb-proto:
	./vaulticdb/generate-proto.sh

vaulticdb-musl:
	./vaulticdb/build-musl.sh

vaulticdb-smoke:
	VAULTICDB_NATIVE_SMOKE=1 rustup run $(VAULTICDB_RUST_TOOLCHAIN) cargo run --manifest-path vaulticdb/Cargo.toml --quiet


