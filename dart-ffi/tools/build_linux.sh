if [ "$1" = "x64" ]; then
  rustup target add target x86_64-unknown-linux-gnu
  cargo build -Z build-std=panic_abort,std --target x86_64-unknown-linux-gnu --release
  mv "target/x86_64-unknown-linux-gnu/release/libisar.so" "libisar_linux_x64.so"
else
  rustup target add aarch64-unknown-linux-gnu
  cargo build -Z build-std=panic_abort,std --target aarch64-unknown-linux-gnu --release
  mv "target/aarch64-unknown-linux-gnu/release/libisar.so" "libisar_linux_arm64.so"
fi