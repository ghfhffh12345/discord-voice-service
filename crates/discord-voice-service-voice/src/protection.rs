use std::sync::atomic::{AtomicU32, Ordering};

use aes_gcm::Aes256Gcm;
use aes_gcm::aead::{AeadInPlace, KeyInit};
use bytes::Bytes;
use chacha20poly1305::XChaCha20Poly1305;

use crate::crypto::EncryptionMode;
use crate::error::VoiceError;
use crate::protocol::SessionDescription;
use crate::rtp::{RtpHeader, parse_rtp_header};

const PROTECTION_TAG_LEN: usize = 16;
const PROTECTION_NONCE_SUFFIX_LEN: usize = 4;

pub struct ProtectionContext {
    mode: EncryptionMode,
    secret_key: Vec<u8>,
    next_nonce: AtomicU32,
}

impl ProtectionContext {
    pub fn new(mode: EncryptionMode, secret_key: Vec<u8>) -> Result<Self, VoiceError> {
        if secret_key.len() != 32 {
            return Err(VoiceError::InvalidState(
                "voice packet protection key invalid",
            ));
        }

        Ok(Self {
            mode,
            secret_key,
            next_nonce: AtomicU32::new(0),
        })
    }

    pub fn from_session(session: &SessionDescription) -> Result<Self, VoiceError> {
        Self::new(session.mode.parse()?, session.secret_key.clone())
    }

    pub fn protect_packet(&self, rtp_header: &[u8], payload: &[u8]) -> Result<Vec<u8>, VoiceError> {
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
                    .map_err(|_| VoiceError::InvalidState("voice packet protection key invalid"))?;
                let mut nonce = aes_gcm::Nonce::default();
                nonce[..nonce_suffix.len()].copy_from_slice(&nonce_suffix);
                cipher
                    .encrypt_in_place_detached(
                        &nonce,
                        rtp_header,
                        &mut protected[payload_start..payload_end],
                    )
                    .map_err(|_| VoiceError::InvalidState("voice packet protection failed"))?
                    .to_vec()
            }
            EncryptionMode::AeadXChaCha20Poly1305Rtpsize => {
                let cipher = XChaCha20Poly1305::new_from_slice(&self.secret_key)
                    .map_err(|_| VoiceError::InvalidState("voice packet protection key invalid"))?;
                let mut nonce = chacha20poly1305::XNonce::default();
                nonce[..nonce_suffix.len()].copy_from_slice(&nonce_suffix);
                cipher
                    .encrypt_in_place_detached(
                        &nonce,
                        rtp_header,
                        &mut protected[payload_start..payload_end],
                    )
                    .map_err(|_| VoiceError::InvalidState("voice packet protection failed"))?
                    .to_vec()
            }
        };

        protected.extend_from_slice(&tag);
        protected.extend_from_slice(&nonce_suffix);
        Ok(protected)
    }

    pub fn unprotect_packet(&self, packet: &[u8]) -> Result<(RtpHeader, Bytes), VoiceError> {
        let header = parse_rtp_header(packet)?;
        let protected_body = packet
            .get(header.header_len..)
            .ok_or(VoiceError::InvalidState("voice protected packet truncated"))?;
        if protected_body.len() < PROTECTION_TAG_LEN + PROTECTION_NONCE_SUFFIX_LEN {
            return Err(VoiceError::InvalidState(
                "voice protected packet body too short",
            ));
        }

        let (ciphertext_and_tag, nonce_suffix) =
            protected_body.split_at(protected_body.len() - PROTECTION_NONCE_SUFFIX_LEN);
        let (ciphertext, tag) =
            ciphertext_and_tag.split_at(ciphertext_and_tag.len() - PROTECTION_TAG_LEN);
        let header_bytes = &packet[..header.header_len];
        let mut plaintext = ciphertext.to_vec();

        match self.mode {
            EncryptionMode::AeadAes256GcmRtpsize => {
                let cipher = Aes256Gcm::new_from_slice(&self.secret_key)
                    .map_err(|_| VoiceError::InvalidState("voice packet protection key invalid"))?;
                let mut nonce = aes_gcm::Nonce::default();
                nonce[..nonce_suffix.len()].copy_from_slice(nonce_suffix);
                cipher
                    .decrypt_in_place_detached(
                        &nonce,
                        header_bytes,
                        &mut plaintext,
                        aes_gcm::Tag::from_slice(tag),
                    )
                    .map_err(|_| VoiceError::InvalidState("voice packet unprotect failed"))?;
            }
            EncryptionMode::AeadXChaCha20Poly1305Rtpsize => {
                let cipher = XChaCha20Poly1305::new_from_slice(&self.secret_key)
                    .map_err(|_| VoiceError::InvalidState("voice packet protection key invalid"))?;
                let mut nonce = chacha20poly1305::XNonce::default();
                nonce[..nonce_suffix.len()].copy_from_slice(nonce_suffix);
                cipher
                    .decrypt_in_place_detached(
                        &nonce,
                        header_bytes,
                        &mut plaintext,
                        chacha20poly1305::Tag::from_slice(tag),
                    )
                    .map_err(|_| VoiceError::InvalidState("voice packet unprotect failed"))?;
            }
        }

        if header.padding {
            let padding_len = plaintext
                .last()
                .copied()
                .map(usize::from)
                .ok_or(VoiceError::InvalidState("voice rtp padding invalid"))?;
            if padding_len == 0 || padding_len > plaintext.len() {
                return Err(VoiceError::InvalidState("voice rtp padding invalid"));
            }
            plaintext.truncate(plaintext.len() - padding_len);
        }

        Ok((header, Bytes::from(plaintext)))
    }
}
