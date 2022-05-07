rustup target add aarch64-apple-ios x86_64-apple-ios
cargo +nightly build -C panic=abort -Z build-std=std,panic_abort -Z build-std-features=panic_immediate_abort --target aarch64-apple-ios --release
cargo +nightly build -C panic=abort -Z build-std=std,panic_abort -Z build-std-features=panic_immediate_abort --target x86_64-apple-ios --release

lipo "target/aarch64-apple-ios/release/libisar.a" "target/x86_64-apple-ios/release/libisar.a" -output "libisar_ios.a" -create