PLATFORMS := macos-arm64 linux-amd64 linux-arm64

.PHONY: all build profile clean test metrics vaultic vaulticdb \
	vaulticdb-proto vaulticdb-musl vaulticdb-smoke \
	vaultic-rados-linux-amd64 vaultic-rados-linux-arm64

BIN_DIR := bin
VAULTICDB_RUST_TOOLCHAIN ?= stable
VAULTICDB_PREPARE_DEBUG ?= 0
VAULTIC_BUILD_TAGS ?=
VAULTICDB_FEATURES ?=
VAULTICDB_TARGET_DIR ?= vaulticdb/target
MACOS_CODESIGN_IDENTITY ?= -
MACOS_CUSTODIAN_ENTITLEMENTS ?= contrib/macos/vaultic-key-custodian.entitlements

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

profile:
	RUSTFLAGS="$${RUSTFLAGS:+$$RUSTFLAGS }-C force-frame-pointers=yes" \
		$(MAKE) BIN_DIR=$(BIN_DIR)/profile VAULTIC_BUILD_TAGS=profile VAULTICDB_PREPARE_DEBUG=0 build

clean:
	rm -rf $(BIN_DIR)

test:
	go test ./cmd/... ./internal/...

metrics:
	go run ./helpers/codemetrics -root .

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
		linux-amd64-rados) goos=linux; goarch=amd64 ;; \
		linux-arm64-rados) goos=linux; goarch=arm64 ;; \
		"") echo "vaultic: unsupported host platform ($(HOST_OS)/$(HOST_ARCH)); use one of: $(PLATFORMS)" >&2; exit 1 ;; \
		*) echo "vaultic: unsupported platform '$*'; supported: $(PLATFORMS)" >&2; exit 1 ;; \
	esac; \
	mkdir -p $(BIN_DIR)/$*; \
	go run build.go --goos "$$goos" --goarch "$$goarch" $(if $(VAULTIC_BUILD_TAGS),--tags "$(VAULTIC_BUILD_TAGS)") -o $(BIN_DIR)/$*/vaultic

# --- vaulticdb (Rust daemon and key broker) ---
# Linux targets use the *-musl target triple (statically linked by default)
# built via cargo-zigbuild for cross-linking from macOS. The macOS target
# builds natively via cargo; true static linking isn't possible on Darwin.

vaulticdb: vaulticdb-$(HOST_PLATFORM)

vaulticdb-%:
	@set -e; \
	case "$*" in \
		macos-arm64) target=aarch64-apple-darwin ;; \
		linux-amd64) target=x86_64-unknown-linux-musl ;; \
		linux-arm64) target=aarch64-unknown-linux-musl ;; \
		linux-amd64-rados) target=x86_64-unknown-linux-musl ;; \
		linux-arm64-rados) target=aarch64-unknown-linux-musl ;; \
		"") echo "vaulticdb: unsupported host platform ($(HOST_OS)/$(HOST_ARCH)); use one of: $(PLATFORMS)" >&2; exit 1 ;; \
		*) echo "vaulticdb: unsupported platform '$*'; supported: $(PLATFORMS)" >&2; exit 1 ;; \
	esac; \
	./vaulticdb/check-rust-target.sh "$$target" "$(VAULTICDB_RUST_TOOLCHAIN)"; \
	mkdir -p $(BIN_DIR)/$*; \
	case "$$target" in \
		*-musl) \
			command -v cargo-zigbuild >/dev/null 2>&1 || { echo "vaulticdb: cargo-zigbuild is required for $$target (install with: cargo install cargo-zigbuild)" >&2; exit 1; }; \
			RUSTFLAGS="$${RUSTFLAGS:+$$RUSTFLAGS }-D warnings -A linker_messages -C link-arg=-Wl,--build-id=sha1" RUSTUP_TOOLCHAIN=$(VAULTICDB_RUST_TOOLCHAIN) \
				CARGO_TARGET_DIR=$(VAULTICDB_TARGET_DIR) cargo-zigbuild zigbuild --manifest-path vaulticdb/Cargo.toml --locked --target "$$target" --release $(if $(VAULTICDB_FEATURES),--features "$(VAULTICDB_FEATURES)") ;; \
		*) \
			CARGO_TARGET_DIR=$(VAULTICDB_TARGET_DIR) rustup run $(VAULTICDB_RUST_TOOLCHAIN) cargo build --manifest-path vaulticdb/Cargo.toml --locked --release --target "$$target" $(if $(VAULTICDB_FEATURES),--features "$(VAULTICDB_FEATURES)") ;; \
	esac; \
	cp $(VAULTICDB_TARGET_DIR)/$$target/release/vaulticdb $(BIN_DIR)/$*/vaulticdb; \
	cp $(VAULTICDB_TARGET_DIR)/$$target/release/vaultic-key-broker $(BIN_DIR)/$*/vaultic-key-broker; \
	cp $(VAULTICDB_TARGET_DIR)/$$target/release/vaultic-key-custodian $(BIN_DIR)/$*/vaultic-key-custodian; \
	if [ "$(VAULTICDB_PREPARE_DEBUG)" = "1" ]; then \
		case "$$target" in *-linux-*) os=linux ;; *-apple-*) os=macos ;; esac; \
		./vaulticdb/prepare-debug-symbols.sh "$$os" $(BIN_DIR)/$* $(BIN_DIR)/$*-debug; \
	fi; \
	case "$$target" in *-musl) ./vaulticdb/verify-static-linux.sh $(BIN_DIR)/$*/vaulticdb $(BIN_DIR)/$*/vaultic-key-broker $(BIN_DIR)/$*/vaultic-key-custodian ;; esac; \
	case "$*" in macos-*) \
		codesign --force --sign "$(MACOS_CODESIGN_IDENTITY)" --identifier com.vaultic.key-custodian --entitlements "$(MACOS_CUSTODIAN_ENTITLEMENTS)" $(BIN_DIR)/$*/vaultic-key-custodian; \
		codesign --verify --strict $(BIN_DIR)/$*/vaultic-key-custodian ;; \
	esac

vaulticdb-proto:
	./vaulticdb/generate-proto.sh

vaulticdb-musl:
	./vaulticdb/build-musl.sh

vaulticdb-smoke:
	VAULTICDB_NATIVE_SMOKE=1 rustup run $(VAULTICDB_RUST_TOOLCHAIN) cargo run --manifest-path vaulticdb/Cargo.toml --quiet

# Produce non-container Linux amd64 binaries with native RADOS support. Vaultic
# uses the pure-Go RADOS client and VaulticDB uses the pure-Rust RADOS client.
vaultic-rados-linux-amd64:
	$(MAKE) vaultic-linux-amd64-rados vaulticdb-linux-amd64-rados VAULTIC_BUILD_TAGS="$(VAULTIC_BUILD_TAGS) rados" VAULTICDB_FEATURES=rados

vaultic-rados-linux-arm64:
	$(MAKE) vaultic-linux-arm64-rados vaulticdb-linux-arm64-rados VAULTIC_BUILD_TAGS="$(VAULTIC_BUILD_TAGS) rados" VAULTICDB_FEATURES=rados


