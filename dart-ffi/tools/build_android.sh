#!/bin/bash

if [[ "$(uname -s)" == "Darwin" ]]; then
    export NDK_HOST_TAG="darwin-x86_64"
elif [[ "$(uname -s)" == "Linux" ]]; then
    export NDK_HOST_TAG="linux-x86_64"
else
    echo "Unsupported OS."
    exit
fi

NDK=${ANDROID_NDK_HOME:-${ANDROID_NDK_ROOT:-"$ANDROID_SDK_ROOT/ndk"}}
COMPILER_DIR="$NDK/toolchains/llvm/prebuilt/$NDK_HOST_TAG/bin"
export PATH="$COMPILER_DIR:$PATH"

echo "$COMPILER_DIR"

export CC_i686_linux_android=$COMPILER_DIR/i686-linux-android21-clang
export AR_i686_linux_android=$COMPILER_DIR/llvm-ar
export CARGO_TARGET_I686_LINUX_ANDROID_LINKER=$COMPILER_DIR/i686-linux-android21-clang
export CARGO_TARGET_I686_LINUX_ANDROID_AR=$COMPILER_DIR/llvm-ar

export CC_x86_64_linux_android=$COMPILER_DIR/x86_64-linux-android21-clang
export AR_x86_64_linux_android=$COMPILER_DIR/llvm-ar
export CARGO_TARGET_X86_64_LINUX_ANDROID_LINKER=$COMPILER_DIR/x86_64-linux-android21-clang
export CARGO_TARGET_X86_64_LINUX_ANDROID_AR=$COMPILER_DIR/llvm-ar

export CC_armv7_linux_androideabi=$COMPILER_DIR/armv7a-linux-androideabi21-clang
export AR_armv7_linux_androideabi=$COMPILER_DIR/llvm-ar
export CARGO_TARGET_ARMV7_LINUX_ANDROIDEABI_LINKER=$COMPILER_DIR/armv7a-linux-androideabi21-clang
export CARGO_TARGET_ARMV7_LINUX_ANDROIDEABI_AR=$COMPILER_DIR/llvm-ar

export CC_aarch64_linux_android=$COMPILER_DIR/aarch64-linux-android21-clang
export AR_aarch64_linux_android=$COMPILER_DIR/llvm-ar
export CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER=$COMPILER_DIR/aarch64-linux-android21-clang
export CARGO_TARGET_AARCH64_LINUX_ANDROID_AR=$COMPILER_DIR/llvm-ar

if [ "$1" = "x86" ]; then
  rustup target add i686-linux-android
  cargo build -Zbuild-std --target i686-linux-android --release
  mv "target/i686-linux-android/release/libisar.so" "libisar_android_x86.so"
elif [ "$1" = "x64" ]; then
  rustup target add x86_64-linux-android
  cargo build -Zbuild-std --target x86_64-linux-android --release
  mv "target/x86_64-linux-android/release/libisar.so" "libisar_android_x64.so"
elif [ "$1" = "armv7" ]; then
  rustup target add armv7-linux-androideabi
  cargo build -Zbuild-std --target armv7-linux-androideabi --release
  mv "target/armv7-linux-androideabi/release/libisar.so" "libisar_android_armv7.so"
else
  rustup target add aarch64-linux-android
  cargo build -Zbuild-std --target aarch64-linux-android --release
  mv "target/aarch64-linux-android/release/libisar.so" "libisar_android_arm64.so"
fi






