fn main() {
    cc::Build::new()
        .file("dart-sdk/dart_api_dl.c")
        .compile("dart");
}