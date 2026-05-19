# Vendored Native DAVE Dependencies

This directory contains the native dependency stack needed to build Discord DAVE without CMake:

- `libdave`: https://github.com/discord/libdave at `52cd56dc550f447fb354b3a06c9e2d2e2a4309c6`
- `mlspp`: https://github.com/cisco/mlspp at `92aaa4134fa45ec39957a7c81a342401fba7feb2`
- `nlohmann_json`: https://github.com/nlohmann/json at `4e5fa3bdd21248b0f0ab5683d4df0daa5300a39e` (`include/` only)
- `openssl`: full OpenSSL 3.0.13 source tree from https://github.com/openssl/openssl at `85cf92f55d9e2ac5aacf92bedd33fb890b9f8b4c`

`build.rs` builds OpenSSL from the vendored source into Cargo `OUT_DIR` with `perl Configure linux-x86_64 no-shared no-tests` and `make build_libs`, then links the generated `libcrypto.a` statically into the Rust build. The DAVE C++ stack does not depend on host OpenSSL headers or `/lib`/`/usr/lib` `libcrypto` artifacts.
