# Vaultic patch

This is `ctap-hid-fido2` 3.5.13, vendored because the upstream crate forces
HIDAPI's `linux-static-hidraw` backend, which still links to `libudev`.

The local manifest exposes a `linux-native-basic-udev` feature and delegates it
to HIDAPI. No Rust source is changed. Remove this copy once upstream supports
selecting the HIDAPI Linux backend.
