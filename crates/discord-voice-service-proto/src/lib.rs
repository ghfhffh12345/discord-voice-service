pub mod discordvoice {
    pub mod v1 {
        tonic::include_proto!("discordvoice.v1");
        pub const CONTROL_FILE_DESCRIPTOR_SET: &[u8] =
            tonic::include_file_descriptor_set!("discordvoice_control_descriptor");
    }
}
