use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }

    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("discordvoice_control_descriptor.bin"))
        .compile_protos(&["proto/discordvoice/v1/control.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto");
    Ok(())
}
