pub const REQUIRED_MODE: &str = "aead_xchacha20_poly1305_rtpsize";
pub const PREFERRED_MODE: &str = "aead_aes256_gcm_rtpsize";

pub fn pick_mode(modes: &[String]) -> Option<&'static str> {
    if modes.iter().any(|mode| mode == PREFERRED_MODE) {
        Some(PREFERRED_MODE)
    } else if modes.iter().any(|mode| mode == REQUIRED_MODE) {
        Some(REQUIRED_MODE)
    } else {
        None
    }
}
