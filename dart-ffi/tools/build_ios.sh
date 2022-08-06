rustup target add aarch64-apple-ios x86_64-apple-ios
cargo build -Zbuild-std --target aarch64-apple-ios --release
cargo build -Zbuild-std --target x86_64-apple-ios --release

lipo "target/aarch64-apple-ios/release/libisar.a" "target/x86_64-apple-ios/release/libisar.a" -output "libisar_ios.a" -create