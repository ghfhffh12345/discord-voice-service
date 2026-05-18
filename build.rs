fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let ytmusic_proto_root = "/home/ghfhffh12345/ytmusic-service/proto";
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "proto/discordvoice/v1/control.proto",
                "/home/ghfhffh12345/ytmusic-service/proto/ytmusic/v1/public.proto",
            ],
            &["proto", ytmusic_proto_root],
        )?;
    Ok(())
}
