if [ "$1" = "x64" ]; then
  rustup target add target x86_64-unknown-linux-gnu
  cargo +nightly build -C panic=abort -Z build-std=std,panic_abort -Z build-std-features=panic_immediate_abort --target x86_64-unknown-linux-gnu --release
  mv "target/x86_64-unknown-linux-gnu/release/libisar.so" "libisar_linux_x64.so"
else
  rustup target add aarch64-unknown-linux-gnu
  cargo +nightly build -C panic=abort -Z build-std=std,panic_abort -Z build-std-features=panic_immediate_abort --target aarch64-unknown-linux-gnu --release
  mv "target/aarch64-unknown-linux-gnu/release/libisar.so" "libisar_linux_arm64.so"
fi