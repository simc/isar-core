rustup target add aarch64-apple-darwin x86_64-apple-darwin
cargo +nightly build -Z build-std=std,panic_abort -Z build-std-features=panic_immediate_abort --target aarch64-apple-darwin --release
cargo +nightly build -Z build-std=std,panic_abort -Z build-std-features=panic_immediate_abort --target x86_64-apple-darwin --release

lipo "target/aarch64-apple-darwin/release/libisar.dylib" "target/x86_64-apple-darwin/release/libisar.dylib" -output "libisar_macos.dylib" -create