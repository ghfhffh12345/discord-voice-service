use std::sync::atomic::{AtomicU32, Ordering};

use aes_gcm::Aes256Gcm;
use aes_gcm::aead::{AeadInPlace, KeyInit};
use chacha20poly1305::XChaCha20Poly1305;

use crate::discord_voice::crypto::EncryptionMode;
use crate::discord_voice::protocol::SessionDescription;
use crate::error::AppError;

pub struct ProtectionContext {
    mode: EncryptionMode,
    secret_key: Vec<u8>,
    next_nonce: AtomicU32,
}

impl ProtectionContext {
    pub fn new(mode: EncryptionMode, secret_key: Vec<u8>) -> Result<Self, AppError> {
        if secret_key.len() != 32 {
            return Err(AppError::InvalidState(
                "voice packet protection key invalid",
            ));
        }

        Ok(Self {
            mode,
            secret_key,
            next_nonce: AtomicU32::new(0),
        })
    }

    pub fn from_session(session: &SessionDescription) -> Result<Self, AppError> {
        Self::new(session.mode.parse()?, session.secret_key.clone())
    }

    pub fn test_xchacha() -> Self {
        Self::new(EncryptionMode::AeadXChaCha20Poly1305Rtpsize, vec![7u8; 32]).unwrap()
    }

    pub fn protect_packet(&self, rtp_header: &[u8], payload: &[u8]) -> Result<Vec<u8>, AppError> {
        let nonce_suffix = self
            .next_nonce
            .fetch_add(1, Ordering::Relaxed)
            .to_be_bytes();
        let mut protected = Vec::with_capacity(rtp_header.len() + payload.len() + 20);
        protected.extend_from_slice(rtp_header);

        let payload_start = protected.len();
        protected.extend_from_slice(payload);
        let payload_end = protected.len();

        let tag = match self.mode {
            EncryptionMode::AeadAes256GcmRtpsize => {
                let cipher = Aes256Gcm::new_from_slice(&self.secret_key)
                    .map_err(|_| AppError::InvalidState("voice packet protection key invalid"))?;
                let mut nonce = aes_gcm::Nonce::default();
                nonce[..nonce_suffix.len()].copy_from_slice(&nonce_suffix);
                cipher
                    .encrypt_in_place_detached(
                        &nonce,
                        rtp_header,
                        &mut protected[payload_start..payload_end],
                    )
                    .map_err(|_| AppError::InvalidState("voice packet protection failed"))?
                    .to_vec()
            }
            EncryptionMode::AeadXChaCha20Poly1305Rtpsize => {
                let cipher = XChaCha20Poly1305::new_from_slice(&self.secret_key)
                    .map_err(|_| AppError::InvalidState("voice packet protection key invalid"))?;
                let mut nonce = chacha20poly1305::XNonce::default();
                nonce[..nonce_suffix.len()].copy_from_slice(&nonce_suffix);
                cipher
                    .encrypt_in_place_detached(
                        &nonce,
                        rtp_header,
                        &mut protected[payload_start..payload_end],
                    )
                    .map_err(|_| AppError::InvalidState("voice packet protection failed"))?
                    .to_vec()
            }
        };

        protected.extend_from_slice(&tag);
        protected.extend_from_slice(&nonce_suffix);
        Ok(protected)
    }
}
