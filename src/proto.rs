pub mod discordvoice {
    pub mod v1 {
        tonic::include_proto!("discordvoice.v1");
    }

    pub use v1::*;
}
