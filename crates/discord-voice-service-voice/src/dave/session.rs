use std::num::NonZeroU16;
use std::panic::{AssertUnwindSafe, catch_unwind};

use davey::{Codec, ProposalsOperationType};
use openmls::prelude::{ExternalSender, tls_codec::DeserializeBytes};

use super::DaveError;
use super::wire::pack_commit_welcome;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DaveMlsProposalsOperation {
    Append,
    Revoke,
}

impl From<DaveMlsProposalsOperation> for ProposalsOperationType {
    fn from(value: DaveMlsProposalsOperation) -> Self {
        match value {
            DaveMlsProposalsOperation::Append => Self::APPEND,
            DaveMlsProposalsOperation::Revoke => Self::REVOKE,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DaveMediaType {
    Audio,
    Video,
}

impl From<DaveMediaType> for davey::MediaType {
    fn from(value: DaveMediaType) -> Self {
        match value {
            DaveMediaType::Audio => Self::AUDIO,
            DaveMediaType::Video => Self::VIDEO,
        }
    }
}

pub struct DaveSession {
    inner: Option<davey::DaveSession>,
    pending_external_sender: Option<Vec<u8>>,
}

unsafe impl Send for DaveSession {}

impl DaveSession {
    pub fn new(_auth_session_id: Option<&str>) -> Result<Self, DaveError> {
        Ok(Self {
            inner: None,
            pending_external_sender: None,
        })
    }

    pub fn max_supported_protocol_version() -> u16 {
        davey::DAVE_PROTOCOL_VERSION
    }

    pub fn init(
        &mut self,
        protocol_version: u16,
        group_id: u64,
        self_user_id: &str,
    ) -> Result<(), DaveError> {
        let protocol_version = NonZeroU16::new(protocol_version)
            .ok_or_else(|| DaveError::operation("init", "protocol version must be non-zero"))?;
        let self_user_id = parse_user_id(self_user_id)?;
        let mut inner = davey::DaveSession::new(protocol_version, self_user_id, group_id, None)
            .map_err(|err| DaveError::operation("init", err))?;
        if let Some(external_sender) = &self.pending_external_sender {
            inner.set_external_sender(external_sender).map_err(|err| {
                DaveError::mls_failure("set external sender", "SetExternalSender", err)
            })?;
        }
        self.inner = Some(inner);
        Ok(())
    }

    pub fn set_external_sender(&mut self, external_sender: &[u8]) -> Result<(), DaveError> {
        validate_external_sender(external_sender)?;
        self.pending_external_sender = Some(external_sender.to_vec());
        if let Some(inner) = &mut self.inner {
            catch_unwind(AssertUnwindSafe(|| {
                inner.set_external_sender(external_sender)
            }))
            .map_err(|_| {
                DaveError::mls_failure(
                    "set external sender",
                    "SetExternalSender",
                    "external sender deserialization panicked",
                )
            })?
            .map_err(|err| {
                DaveError::mls_failure("set external sender", "SetExternalSender", err)
            })?;
        }
        Ok(())
    }

    pub fn protocol_version(&self) -> u16 {
        self.inner
            .as_ref()
            .map(|inner| inner.protocol_version().get())
            .unwrap_or(0)
    }

    #[doc(hidden)]
    pub fn epoch(&self) -> Option<u64> {
        self.inner
            .as_ref()
            .and_then(|inner| inner.epoch().map(|epoch| epoch.as_u64()))
    }

    pub fn key_package(&mut self) -> Result<Vec<u8>, DaveError> {
        self.inner_mut()?
            .create_key_package()
            .map_err(|err| DaveError::operation("key package", err))
    }

    pub fn process_proposals(
        &mut self,
        proposals: &[u8],
        recognized_user_ids: &[&str],
    ) -> Result<Vec<u8>, DaveError> {
        self.process_proposals_with_operation(
            DaveMlsProposalsOperation::Append,
            proposals,
            recognized_user_ids,
        )
        .and_then(|commit_welcome| commit_welcome.ok_or(DaveError::EmptyOutput("commit welcome")))
    }

    pub(crate) fn process_proposals_with_operation(
        &mut self,
        operation: DaveMlsProposalsOperation,
        proposals: &[u8],
        recognized_user_ids: &[&str],
    ) -> Result<Option<Vec<u8>>, DaveError> {
        let expected_user_ids = if recognized_user_ids.len() <= 1 {
            None
        } else {
            Some(parse_user_ids(recognized_user_ids)?)
        };
        let commit_welcome = self
            .inner_mut()?
            .process_proposals(operation.into(), proposals, expected_user_ids.as_deref())
            .map_err(|err| DaveError::operation("process proposals", err))?;
        match (operation, commit_welcome) {
            (DaveMlsProposalsOperation::Revoke, None) => Ok(None),
            (_, Some(commit_welcome)) => Ok(Some(pack_commit_welcome(
                &commit_welcome.commit,
                commit_welcome.welcome.as_deref(),
            ))),
            (DaveMlsProposalsOperation::Append, None) => Err(DaveError::operation(
                "process proposals",
                "append proposals returned no commit/welcome",
            )),
        }
    }

    pub fn process_commit(&mut self, commit: &[u8]) -> Result<DaveCommitResult, DaveError> {
        self.inner_mut()?
            .process_commit(commit)
            .map_err(|err| DaveError::operation("process commit", err))?;
        let roster_member_ids = self.roster_member_ids();
        Ok(DaveCommitResult {
            failed: false,
            ignored: false,
            roster_member_ids,
        })
    }

    pub fn process_welcome(
        &mut self,
        welcome: &[u8],
        _recognized_user_ids: &[&str],
    ) -> Result<DaveWelcomeResult, DaveError> {
        self.inner_mut()?
            .process_welcome(welcome)
            .map_err(|err| DaveError::operation("process welcome", err))?;
        let roster_member_ids = self.roster_member_ids();
        Ok(DaveWelcomeResult { roster_member_ids })
    }

    pub fn last_epoch_authenticator(&self) -> Result<Vec<u8>, DaveError> {
        self.inner()?
            .get_epoch_authenticator()
            .map(|authenticator| authenticator.as_slice().to_vec())
            .ok_or(DaveError::EmptyOutput("last epoch authenticator"))
    }

    pub fn pairwise_fingerprint(
        &self,
        _protocol_version: u16,
        user_id: &str,
    ) -> Result<Vec<u8>, DaveError> {
        self.inner()?
            .get_pairwise_fingerprint(0, parse_user_id(user_id)?)
            .map_err(|err| DaveError::operation("pairwise fingerprint", err))
    }

    pub fn encrypt_audio_frame(&mut self, frame: &[u8]) -> Result<Vec<u8>, DaveError> {
        self.inner_mut()?
            .encrypt_opus(frame)
            .map(|frame| frame.into_owned())
            .map_err(|err| DaveError::operation("encrypt audio frame", err))
    }

    pub fn decrypt_audio_frame_from(
        &mut self,
        user_id: &str,
        media_type: DaveMediaType,
        encrypted_frame: &[u8],
    ) -> Result<Vec<u8>, DaveError> {
        self.inner_mut()?
            .decrypt(parse_user_id(user_id)?, media_type.into(), encrypted_frame)
            .map_err(|err| DaveError::operation("decrypt audio frame", err))
    }

    fn into_inner(mut self) -> Result<davey::DaveSession, DaveError> {
        self.inner.take().ok_or(DaveError::NotInitialized)
    }

    fn inner(&self) -> Result<&davey::DaveSession, DaveError> {
        self.inner.as_ref().ok_or(DaveError::NotInitialized)
    }

    fn inner_mut(&mut self) -> Result<&mut davey::DaveSession, DaveError> {
        self.inner.as_mut().ok_or(DaveError::NotInitialized)
    }

    fn roster_member_ids(&self) -> Vec<u64> {
        self.inner
            .as_ref()
            .and_then(davey::DaveSession::get_user_ids)
            .unwrap_or_default()
    }
}

pub struct DaveCommitResult {
    failed: bool,
    ignored: bool,
    roster_member_ids: Vec<u64>,
}

impl DaveCommitResult {
    pub fn is_failed(&self) -> bool {
        self.failed
    }

    pub fn is_ignored(&self) -> bool {
        self.ignored
    }

    pub fn roster_member_ids(&self) -> Vec<u64> {
        self.roster_member_ids.clone()
    }
}

pub struct DaveWelcomeResult {
    roster_member_ids: Vec<u64>,
}

impl DaveWelcomeResult {
    pub fn roster_member_ids(&self) -> Vec<u64> {
        self.roster_member_ids.clone()
    }
}

pub struct DaveRuntimeContext {
    pub protocol_version: u16,
    session: davey::DaveSession,
}

impl DaveRuntimeContext {
    pub fn from_session(session: DaveSession) -> Result<Self, DaveError> {
        let protocol_version = session.protocol_version();
        Ok(Self {
            protocol_version,
            session: session.into_inner()?,
        })
    }

    pub fn encrypt_audio_frame(&mut self, frame: &[u8]) -> Result<Vec<u8>, DaveError> {
        self.encrypt(DaveMediaType::Audio, frame)
    }

    pub fn decrypt_audio_frame_from(
        &mut self,
        user_id: &str,
        media_type: DaveMediaType,
        encrypted_frame: &[u8],
    ) -> Result<Vec<u8>, DaveError> {
        self.session
            .decrypt(parse_user_id(user_id)?, media_type.into(), encrypted_frame)
            .map_err(|err| DaveError::operation("decrypt audio frame", err))
    }

    pub(crate) fn process_commit(&mut self, commit: &[u8]) -> Result<DaveCommitResult, DaveError> {
        self.session
            .process_commit(commit)
            .map_err(|err| DaveError::operation("process commit", err))?;
        let roster_member_ids = self.session.get_user_ids().unwrap_or_default();
        Ok(DaveCommitResult {
            failed: false,
            ignored: false,
            roster_member_ids,
        })
    }

    pub(crate) fn process_welcome(
        &mut self,
        welcome: &[u8],
        _recognized_user_ids: &[&str],
    ) -> Result<DaveWelcomeResult, DaveError> {
        self.session
            .process_welcome(welcome)
            .map_err(|err| DaveError::operation("process welcome", err))?;
        let roster_member_ids = self.session.get_user_ids().unwrap_or_default();
        Ok(DaveWelcomeResult { roster_member_ids })
    }

    pub(crate) fn process_proposals_with_operation(
        &mut self,
        operation: DaveMlsProposalsOperation,
        proposals: &[u8],
        recognized_user_ids: &[&str],
    ) -> Result<Option<Vec<u8>>, DaveError> {
        let expected_user_ids = if recognized_user_ids.len() <= 1 {
            None
        } else {
            Some(parse_user_ids(recognized_user_ids)?)
        };
        let commit_welcome = self
            .session
            .process_proposals(operation.into(), proposals, expected_user_ids.as_deref())
            .map_err(|err| DaveError::operation("process proposals", err))?;
        match (operation, commit_welcome) {
            (DaveMlsProposalsOperation::Revoke, None) => Ok(None),
            (_, Some(commit_welcome)) => Ok(Some(pack_commit_welcome(
                &commit_welcome.commit,
                commit_welcome.welcome.as_deref(),
            ))),
            (DaveMlsProposalsOperation::Append, None) => Err(DaveError::operation(
                "process proposals",
                "append proposals returned no commit/welcome",
            )),
        }
    }

    fn encrypt(&mut self, media_type: DaveMediaType, frame: &[u8]) -> Result<Vec<u8>, DaveError> {
        let codec = match media_type {
            DaveMediaType::Audio => Codec::OPUS,
            DaveMediaType::Video => Codec::UNKNOWN,
        };
        self.session
            .encrypt(media_type.into(), codec, frame)
            .map(|frame| frame.into_owned())
            .map_err(|err| DaveError::operation("encrypt frame", err))
    }
}

fn parse_user_ids(user_ids: &[&str]) -> Result<Vec<u64>, DaveError> {
    user_ids
        .iter()
        .map(|user_id| parse_user_id(user_id))
        .collect()
}

fn parse_user_id(user_id: &str) -> Result<u64, DaveError> {
    Ok(user_id.parse()?)
}

fn validate_external_sender(external_sender: &[u8]) -> Result<(), DaveError> {
    catch_unwind(|| ExternalSender::tls_deserialize_exact_bytes(external_sender))
        .map_err(|_| {
            DaveError::mls_failure(
                "set external sender",
                "SetExternalSender",
                "external sender deserialization panicked",
            )
        })?
        .map(|_| ())
        .map_err(|err| DaveError::mls_failure("set external sender", "SetExternalSender", err))
}
