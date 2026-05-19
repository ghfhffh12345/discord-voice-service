pub const REQUIRED_MODE: &str = "aead_xchacha20_poly1305_rtpsize";
pub const PREFERRED_MODE: &str = "aead_aes256_gcm_rtpsize";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncryptionMode {
    AeadAes256GcmRtpsize,
    AeadXChaCha20Poly1305Rtpsize,
}

impl EncryptionMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AeadAes256GcmRtpsize => PREFERRED_MODE,
            Self::AeadXChaCha20Poly1305Rtpsize => REQUIRED_MODE,
        }
    }
}

impl std::str::FromStr for EncryptionMode {
    type Err = crate::error::AppError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            PREFERRED_MODE => Ok(Self::AeadAes256GcmRtpsize),
            REQUIRED_MODE => Ok(Self::AeadXChaCha20Poly1305Rtpsize),
            _ => Err(crate::error::AppError::UnsupportedEncryptionMode),
        }
    }
}

pub fn choose_mode(modes: &[String]) -> Result<EncryptionMode, crate::error::AppError> {
    if modes.iter().any(|mode| mode == PREFERRED_MODE) {
        Ok(EncryptionMode::AeadAes256GcmRtpsize)
    } else if modes.iter().any(|mode| mode == REQUIRED_MODE) {
        Ok(EncryptionMode::AeadXChaCha20Poly1305Rtpsize)
    } else {
        Err(crate::error::AppError::UnsupportedEncryptionMode)
    }
}

pub fn pick_mode(modes: &[String]) -> Option<&'static str> {
    choose_mode(modes).ok().map(EncryptionMode::as_str)
}
