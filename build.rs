use std::fs;
use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    build_dave_native();
    build_protos()?;
    Ok(())
}

fn build_protos() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "proto/discordvoice/v1/control.proto",
                "proto/ytmusic/v1/public.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}

fn build_dave_native() {
    let root = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").expect("manifest dir"));
    let vendor = root.join("vendor");
    let libdave = vendor.join("libdave/cpp");
    let mlspp = vendor.join("mlspp");

    println!("cargo:rerun-if-changed=vendor/libdave");
    println!("cargo:rerun-if-changed=vendor/mlspp");
    println!("cargo:rerun-if-changed=vendor/nlohmann_json");
    println!("cargo:rerun-if-changed=vendor/openssl");

    let mut build = cc::Build::new();
    build
        .cpp(true)
        .std("c++17")
        .warnings(false)
        .flag_if_supported("-Wno-deprecated-declarations")
        .define("WITH_OPENSSL3", None)
        .define("DISABLE_GREASE", None)
        .include(mlspp.join("include"))
        .include(mlspp.join("generated/include"))
        .include(mlspp.join("lib/bytes/include"))
        .include(mlspp.join("lib/tls_syntax/include"))
        .include(mlspp.join("lib/hpke/include"))
        .include(mlspp.join("third_party"))
        .include(libdave.join("includes"))
        .include(libdave.join("src"))
        .include(vendor.join("nlohmann_json/include"))
        .include(vendor.join("openssl/include"));

    add_cpp_files(&mut build, &mlspp.join("src"), |_| true);
    add_cpp_files(&mut build, &mlspp.join("lib/bytes/src"), |_| true);
    add_cpp_files(&mut build, &mlspp.join("lib/tls_syntax/src"), |_| true);
    add_cpp_files(&mut build, &mlspp.join("lib/hpke/src"), |_| true);

    add_cpp_files(&mut build, &libdave.join("src"), |path| {
        let path = path.to_string_lossy();
        !path.contains("bindings_wasm")
            && !path.contains("boringssl_cryptor")
            && !path.contains("_apple")
            && !path.contains("_win")
            && !path.contains("persisted_key_pair.cpp")
            && !path.contains("detail/persisted_key_pair")
    });
    build.file(libdave.join("src/mls/persisted_key_pair_null.cpp"));

    // Upstream's C API tests use this helper to generate valid MLS external-sender proposals.
    build
        .file(libdave.join("test/external_sender.cpp"))
        .file(libdave.join("test/capi/external_sender_wrapper.cpp"));

    build.compile("dave_native");

    link_system_libcrypto();
    println!("cargo:rustc-link-lib=dylib=crypto");
}

fn add_cpp_files<F>(build: &mut cc::Build, dir: &Path, include: F)
where
    F: Fn(&Path) -> bool + Copy,
{
    for entry in fs::read_dir(dir).unwrap_or_else(|err| panic!("read {}: {err}", dir.display())) {
        let path = entry.expect("directory entry").path();
        if path.is_dir() {
            add_cpp_files(build, &path, include);
        } else if path.extension().is_some_and(|ext| ext == "cpp") && include(&path) {
            build.file(path);
        }
    }
}

fn link_system_libcrypto() {
    let out_dir = PathBuf::from(std::env::var_os("OUT_DIR").expect("out dir"));
    let libcrypto = [
        "/lib/x86_64-linux-gnu/libcrypto.so.3",
        "/usr/lib/x86_64-linux-gnu/libcrypto.so.3",
    ]
    .into_iter()
    .map(PathBuf::from)
    .find(|path| path.exists())
    .expect("libcrypto.so.3");
    let link = out_dir.join("libcrypto.so");
    if !link.exists() {
        #[cfg(unix)]
        std::os::unix::fs::symlink(&libcrypto, &link)
            .unwrap_or_else(|err| panic!("symlink {}: {err}", link.display()));
    }
    println!("cargo:rustc-link-search=native={}", out_dir.display());
}
