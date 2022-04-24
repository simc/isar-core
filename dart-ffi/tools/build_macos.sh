if [ "$1" = "x64" ]; then
  rustup target add x86_64-apple-darwin
  cargo build --target x86_64-apple-darwin --release
  mv "target/x86_64-apple-darwin/release/libisar.dylib" "libisar_macos_x64.dylib"
else
  rustup target add aarch64-apple-darwin
  cargo build --target aarch64-apple-darwin --release
  mv "target/aarch64-apple-darwin/release/libisar.dylib" "libisar_macos_arm64.dylib"
fi