use std::ffi::{CStr, CString, NulError, c_char, c_void};
use std::ptr;
use std::slice;
use std::sync::Mutex;
use std::sync::mpsc::{self, Sender};
use std::time::Duration;

use thiserror::Error;

use super::dave_ffi;
use crate::error::AppError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaveContext {
    pub protocol_version: u32,
    pub transition_id: Option<String>,
}

#[derive(Debug, Error)]
pub enum DaveError {
    #[error("DAVE returned a null handle for {0}")]
    NullHandle(&'static str),
    #[error("DAVE returned no bytes for {0}")]
    EmptyOutput(&'static str),
    #[error("DAVE string contains an interior null byte")]
    InteriorNull(#[from] NulError),
    #[error("DAVE MLS failure during {context}: {mls_source}: {reason}")]
    MlsFailure {
        context: &'static str,
        mls_source: String,
        reason: String,
    },
    #[error("DAVE pairwise fingerprint callback timed out")]
    FingerprintTimeout,
    #[error("DAVE encryption failed with result code {0}")]
    EncryptFailed(u32),
    #[error("DAVE decryption failed with result code {0}")]
    DecryptFailed(u32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DaveMediaType {
    Audio,
    Video,
}

impl DaveMediaType {
    fn as_ffi(self) -> u32 {
        match self {
            Self::Audio => dave_ffi::DAVE_MEDIA_TYPE_AUDIO,
            Self::Video => dave_ffi::DAVE_MEDIA_TYPE_VIDEO,
        }
    }
}

pub struct DaveSession {
    handle: dave_ffi::DaveSessionHandle,
    failure_state: Box<SessionFailureState>,
}

unsafe impl Send for DaveSession {}

impl DaveSession {
    pub fn new(auth_session_id: Option<&str>) -> Result<Self, DaveError> {
        let auth_session_id = auth_session_id.map(CString::new).transpose()?;
        let auth_session_id_ptr = auth_session_id
            .as_ref()
            .map_or(ptr::null(), |value| value.as_ptr());
        let failure_state = Box::new(SessionFailureState::default());
        let failure_state_ptr = (&*failure_state as *const SessionFailureState)
            .cast_mut()
            .cast::<c_void>();
        let handle = unsafe {
            dave_ffi::daveSessionCreate(
                ptr::null_mut(),
                auth_session_id_ptr,
                Some(session_failure_callback),
                failure_state_ptr,
            )
        };
        if handle.is_null() {
            return Err(DaveError::NullHandle("session"));
        }
        Ok(Self {
            handle,
            failure_state,
        })
    }

    pub fn max_supported_protocol_version() -> u16 {
        unsafe { dave_ffi::daveMaxSupportedProtocolVersion() }
    }

    pub fn init(
        &mut self,
        protocol_version: u16,
        group_id: u64,
        self_user_id: &str,
    ) -> Result<(), DaveError> {
        let self_user_id = CString::new(self_user_id)?;
        unsafe {
            dave_ffi::daveSessionInit(
                self.handle,
                protocol_version,
                group_id,
                self_user_id.as_ptr(),
            );
        }
        if let Some(err) = self.take_failure("init") {
            return Err(err);
        }
        Ok(())
    }

    pub fn set_external_sender(&mut self, external_sender: &[u8]) -> Result<(), DaveError> {
        unsafe {
            dave_ffi::daveSessionSetExternalSender(
                self.handle,
                external_sender.as_ptr(),
                external_sender.len(),
            );
        }
        if let Some(err) = self.take_failure("set external sender") {
            return Err(err);
        }
        Ok(())
    }

    pub fn protocol_version(&self) -> u16 {
        unsafe { dave_ffi::daveSessionGetProtocolVersion(self.handle) }
    }

    pub fn key_package(&mut self) -> Result<Vec<u8>, DaveError> {
        let mut data = ptr::null_mut();
        let mut len = 0;
        unsafe {
            dave_ffi::daveSessionGetMarshalledKeyPackage(self.handle, &mut data, &mut len);
            self.take_dave_bytes(data, len, "key package")
        }
    }

    pub fn process_proposals(
        &mut self,
        proposals: &[u8],
        recognized_user_ids: &[&str],
    ) -> Result<Vec<u8>, DaveError> {
        let recognized_user_ids = c_string_list(recognized_user_ids)?;
        let recognized_user_id_ptrs = c_string_ptrs(&recognized_user_ids);
        let mut data = ptr::null_mut();
        let mut len = 0;
        unsafe {
            dave_ffi::daveSessionProcessProposals(
                self.handle,
                proposals.as_ptr(),
                proposals.len(),
                recognized_user_id_ptrs.as_ptr(),
                recognized_user_id_ptrs.len(),
                &mut data,
                &mut len,
            );
            self.take_dave_bytes(data, len, "commit welcome")
        }
    }

    pub fn process_commit(&mut self, commit: &[u8]) -> Result<DaveCommitResult, DaveError> {
        let handle = unsafe {
            dave_ffi::daveSessionProcessCommit(self.handle, commit.as_ptr(), commit.len())
        };
        if handle.is_null() {
            return Err(self
                .take_failure("process commit")
                .unwrap_or(DaveError::NullHandle("commit result")));
        }
        if let Some(err) = self.take_failure("process commit") {
            unsafe {
                dave_ffi::daveCommitResultDestroy(handle);
            }
            return Err(err);
        }
        Ok(DaveCommitResult { handle })
    }

    pub fn process_welcome(
        &mut self,
        welcome: &[u8],
        recognized_user_ids: &[&str],
    ) -> Result<DaveWelcomeResult, DaveError> {
        let recognized_user_ids = c_string_list(recognized_user_ids)?;
        let recognized_user_id_ptrs = c_string_ptrs(&recognized_user_ids);
        let handle = unsafe {
            dave_ffi::daveSessionProcessWelcome(
                self.handle,
                welcome.as_ptr(),
                welcome.len(),
                recognized_user_id_ptrs.as_ptr(),
                recognized_user_id_ptrs.len(),
            )
        };
        if handle.is_null() {
            return Err(self
                .take_failure("process welcome")
                .unwrap_or(DaveError::NullHandle("welcome result")));
        }
        if let Some(err) = self.take_failure("process welcome") {
            unsafe {
                dave_ffi::daveWelcomeResultDestroy(handle);
            }
            return Err(err);
        }
        Ok(DaveWelcomeResult { handle })
    }

    pub fn last_epoch_authenticator(&self) -> Result<Vec<u8>, DaveError> {
        let mut data = ptr::null_mut();
        let mut len = 0;
        unsafe {
            dave_ffi::daveSessionGetLastEpochAuthenticator(self.handle, &mut data, &mut len);
            self.take_dave_bytes(data, len, "last epoch authenticator")
        }
    }

    pub fn pairwise_fingerprint(
        &self,
        protocol_version: u16,
        user_id: &str,
    ) -> Result<Vec<u8>, DaveError> {
        let user_id = CString::new(user_id)?;
        let (tx, rx) = mpsc::channel::<Vec<u8>>();
        let user_data = Box::into_raw(Box::new(tx)).cast::<c_void>();
        unsafe {
            dave_ffi::daveSessionGetPairwiseFingerprint(
                self.handle,
                protocol_version,
                user_id.as_ptr(),
                Some(pairwise_fingerprint_callback),
                user_data,
            );
        }
        rx.recv_timeout(Duration::from_secs(30))
            .map_err(|_| DaveError::FingerprintTimeout)
            .and_then(|fingerprint| {
                if fingerprint.is_empty() {
                    Err(DaveError::EmptyOutput("pairwise fingerprint"))
                } else {
                    Ok(fingerprint)
                }
            })
    }

    pub fn key_ratchet_for(&self, user_id: &str) -> Result<DaveKeyRatchet, DaveError> {
        let user_id = CString::new(user_id)?;
        let handle = unsafe { dave_ffi::daveSessionGetKeyRatchet(self.handle, user_id.as_ptr()) };
        if handle.is_null() {
            return Err(self
                .take_failure("key ratchet")
                .unwrap_or(DaveError::NullHandle("key ratchet")));
        }
        if let Some(err) = self.take_failure("key ratchet") {
            unsafe {
                dave_ffi::daveKeyRatchetDestroy(handle);
            }
            return Err(err);
        }
        Ok(DaveKeyRatchet { handle })
    }

    fn take_failure(&self, context: &'static str) -> Option<DaveError> {
        self.failure_state
            .take()
            .map(|failure| DaveError::MlsFailure {
                context,
                mls_source: failure.source,
                reason: failure.reason,
            })
    }

    unsafe fn take_dave_bytes(
        &self,
        data: *mut u8,
        len: usize,
        context: &'static str,
    ) -> Result<Vec<u8>, DaveError> {
        if data.is_null() || len == 0 {
            return Err(self
                .take_failure(context)
                .unwrap_or(DaveError::EmptyOutput(context)));
        }
        let bytes = unsafe { slice::from_raw_parts(data, len).to_vec() };
        unsafe {
            dave_ffi::daveFree(data.cast::<c_void>());
        }
        if let Some(err) = self.take_failure(context) {
            return Err(err);
        }
        Ok(bytes)
    }
}

impl Drop for DaveSession {
    fn drop(&mut self) {
        unsafe {
            dave_ffi::daveSessionDestroy(self.handle);
        }
    }
}

pub struct DaveCommitResult {
    handle: dave_ffi::DaveCommitResultHandle,
}

impl DaveCommitResult {
    pub fn is_failed(&self) -> bool {
        unsafe { dave_ffi::daveCommitResultIsFailed(self.handle) }
    }

    pub fn is_ignored(&self) -> bool {
        unsafe { dave_ffi::daveCommitResultIsIgnored(self.handle) }
    }

    pub fn roster_member_ids(&self) -> Vec<u64> {
        let mut data = ptr::null_mut();
        let mut len = 0;
        unsafe {
            dave_ffi::daveCommitResultGetRosterMemberIds(self.handle, &mut data, &mut len);
            take_dave_u64s(data, len)
        }
    }
}

impl Drop for DaveCommitResult {
    fn drop(&mut self) {
        unsafe {
            dave_ffi::daveCommitResultDestroy(self.handle);
        }
    }
}

pub struct DaveWelcomeResult {
    handle: dave_ffi::DaveWelcomeResultHandle,
}

impl DaveWelcomeResult {
    pub fn roster_member_ids(&self) -> Vec<u64> {
        let mut data = ptr::null_mut();
        let mut len = 0;
        unsafe {
            dave_ffi::daveWelcomeResultGetRosterMemberIds(self.handle, &mut data, &mut len);
            take_dave_u64s(data, len)
        }
    }
}

impl Drop for DaveWelcomeResult {
    fn drop(&mut self) {
        unsafe {
            dave_ffi::daveWelcomeResultDestroy(self.handle);
        }
    }
}

pub struct DaveKeyRatchet {
    handle: dave_ffi::DaveKeyRatchetHandle,
}

impl Drop for DaveKeyRatchet {
    fn drop(&mut self) {
        unsafe {
            dave_ffi::daveKeyRatchetDestroy(self.handle);
        }
    }
}

pub struct DaveEncryptor {
    handle: dave_ffi::DaveEncryptorHandle,
}

unsafe impl Send for DaveEncryptor {}

impl DaveEncryptor {
    pub fn new() -> Result<Self, DaveError> {
        let handle = unsafe { dave_ffi::daveEncryptorCreate() };
        if handle.is_null() {
            return Err(DaveError::NullHandle("encryptor"));
        }
        Ok(Self { handle })
    }

    pub fn assign_opus_ssrc(&mut self, ssrc: u32) {
        unsafe {
            dave_ffi::daveEncryptorAssignSsrcToCodec(self.handle, ssrc, dave_ffi::DAVE_CODEC_OPUS);
        }
    }

    pub fn set_key_ratchet(&mut self, key_ratchet: &DaveKeyRatchet) -> Result<(), DaveError> {
        unsafe {
            dave_ffi::daveEncryptorSetKeyRatchet(self.handle, key_ratchet.handle);
        }
        Ok(())
    }

    pub fn encrypt(
        &mut self,
        media_type: DaveMediaType,
        ssrc: u32,
        frame: &[u8],
    ) -> Result<Vec<u8>, DaveError> {
        let capacity = unsafe {
            dave_ffi::daveEncryptorGetMaxCiphertextByteSize(
                self.handle,
                media_type.as_ffi(),
                frame.len(),
            )
        };
        let mut out = vec![0; capacity];
        let mut bytes_written = 0;
        let result = unsafe {
            dave_ffi::daveEncryptorEncrypt(
                self.handle,
                media_type.as_ffi(),
                ssrc,
                frame.as_ptr(),
                frame.len(),
                out.as_mut_ptr(),
                out.len(),
                &mut bytes_written,
            )
        };
        if result != dave_ffi::DAVE_ENCRYPTOR_RESULT_CODE_SUCCESS {
            return Err(DaveError::EncryptFailed(result));
        }
        out.truncate(bytes_written);
        Ok(out)
    }
}

impl Drop for DaveEncryptor {
    fn drop(&mut self) {
        unsafe {
            dave_ffi::daveEncryptorDestroy(self.handle);
        }
    }
}

pub struct DaveDecryptor {
    handle: dave_ffi::DaveDecryptorHandle,
}

unsafe impl Send for DaveDecryptor {}

impl DaveDecryptor {
    pub fn new() -> Result<Self, DaveError> {
        let handle = unsafe { dave_ffi::daveDecryptorCreate() };
        if handle.is_null() {
            return Err(DaveError::NullHandle("decryptor"));
        }
        Ok(Self { handle })
    }

    pub fn set_key_ratchet(&mut self, key_ratchet: &DaveKeyRatchet) -> Result<(), DaveError> {
        unsafe {
            dave_ffi::daveDecryptorTransitionToKeyRatchet(self.handle, key_ratchet.handle);
        }
        Ok(())
    }

    pub fn decrypt(
        &mut self,
        media_type: DaveMediaType,
        encrypted_frame: &[u8],
    ) -> Result<Vec<u8>, DaveError> {
        let capacity = unsafe {
            dave_ffi::daveDecryptorGetMaxPlaintextByteSize(
                self.handle,
                media_type.as_ffi(),
                encrypted_frame.len(),
            )
        };
        let mut out = vec![0; capacity];
        let mut bytes_written = 0;
        let result = unsafe {
            dave_ffi::daveDecryptorDecrypt(
                self.handle,
                media_type.as_ffi(),
                encrypted_frame.as_ptr(),
                encrypted_frame.len(),
                out.as_mut_ptr(),
                out.len(),
                &mut bytes_written,
            )
        };
        if result != dave_ffi::DAVE_DECRYPTOR_RESULT_CODE_SUCCESS {
            return Err(DaveError::DecryptFailed(result));
        }
        out.truncate(bytes_written);
        Ok(out)
    }
}

impl Drop for DaveDecryptor {
    fn drop(&mut self) {
        unsafe {
            dave_ffi::daveDecryptorDestroy(self.handle);
        }
    }
}

pub struct DaveRuntimeContext {
    pub protocol_version: u16,
    pub encryptor: DaveEncryptor,
    pub decryptor: DaveDecryptor,
}

impl DaveRuntimeContext {
    pub fn from_session(
        session: &DaveSession,
        protocol_version: u16,
        self_user_id: &str,
        peer_user_id: &str,
        ssrc: u32,
    ) -> Result<Self, AppError> {
        let encrypt_ratchet = session
            .key_ratchet_for(self_user_id)
            .map_err(|_| AppError::InvalidState("voice dave self ratchet missing"))?;
        let decrypt_ratchet = session
            .key_ratchet_for(peer_user_id)
            .map_err(|_| AppError::InvalidState("voice dave peer ratchet missing"))?;

        let mut encryptor = DaveEncryptor::new()
            .map_err(|_| AppError::InvalidState("voice dave encryptor create failed"))?;
        encryptor.assign_opus_ssrc(ssrc);
        encryptor
            .set_key_ratchet(&encrypt_ratchet)
            .map_err(|_| AppError::InvalidState("voice dave encryptor setup failed"))?;

        let mut decryptor = DaveDecryptor::new()
            .map_err(|_| AppError::InvalidState("voice dave decryptor create failed"))?;
        decryptor
            .set_key_ratchet(&decrypt_ratchet)
            .map_err(|_| AppError::InvalidState("voice dave decryptor setup failed"))?;

        Ok(Self {
            protocol_version,
            encryptor,
            decryptor,
        })
    }
}

#[doc(hidden)]
pub struct DaveExternalSender {
    handle: dave_ffi::DaveExternalSenderHandle,
}

unsafe impl Send for DaveExternalSender {}

impl DaveExternalSender {
    pub fn new(group_id: u64) -> Result<Self, DaveError> {
        let handle = unsafe { dave_ffi::daveExternalSenderCreate(group_id) };
        if handle.is_null() {
            return Err(DaveError::NullHandle("external sender"));
        }
        Ok(Self { handle })
    }

    pub fn marshalled_external_sender(&self) -> Result<Vec<u8>, DaveError> {
        let mut data = ptr::null_mut();
        let mut len = 0;
        unsafe {
            dave_ffi::daveExternalSenderGetMarshalledExternalSender(
                self.handle,
                &mut data,
                &mut len,
            );
            take_dave_bytes(data, len, "external sender")
        }
    }

    pub fn propose_add(&self, epoch: u32, key_package: &[u8]) -> Result<Vec<u8>, DaveError> {
        let mut key_package = key_package.to_vec();
        let mut data = ptr::null_mut();
        let mut len = 0;
        unsafe {
            dave_ffi::daveExternalSenderProposeAdd(
                self.handle,
                epoch,
                key_package.as_mut_ptr(),
                key_package.len(),
                &mut data,
                &mut len,
            );
            take_dave_bytes(data, len, "add proposal")
        }
    }

    pub fn split_commit_welcome(
        &self,
        commit_welcome: &[u8],
    ) -> Result<(Vec<u8>, Vec<u8>), DaveError> {
        let mut commit_welcome = commit_welcome.to_vec();
        let mut commit = ptr::null_mut();
        let mut commit_len = 0;
        let mut welcome = ptr::null_mut();
        let mut welcome_len = 0;
        unsafe {
            dave_ffi::daveExternalSenderSplitCommitWelcome(
                self.handle,
                commit_welcome.as_mut_ptr(),
                commit_welcome.len(),
                &mut commit,
                &mut commit_len,
                &mut welcome,
                &mut welcome_len,
            );
            Ok((
                take_dave_bytes(commit, commit_len, "commit")?,
                take_dave_bytes(welcome, welcome_len, "welcome")?,
            ))
        }
    }
}

impl Drop for DaveExternalSender {
    fn drop(&mut self) {
        unsafe {
            dave_ffi::daveExternalSenderDestroy(self.handle);
        }
    }
}

#[derive(Debug, Clone)]
struct MlsFailure {
    source: String,
    reason: String,
}

#[derive(Debug, Default)]
struct SessionFailureState {
    latest: Mutex<Option<MlsFailure>>,
}

impl SessionFailureState {
    fn record(&self, source: String, reason: String) {
        *self.latest.lock().expect("session failure mutex poisoned") =
            Some(MlsFailure { source, reason });
    }

    fn take(&self) -> Option<MlsFailure> {
        self.latest
            .lock()
            .expect("session failure mutex poisoned")
            .take()
    }
}

unsafe extern "C" fn session_failure_callback(
    source: *const c_char,
    reason: *const c_char,
    user_data: *mut c_void,
) {
    if user_data.is_null() {
        return;
    }
    let failure_state = unsafe { &*user_data.cast::<SessionFailureState>() };
    failure_state.record(unsafe { c_string_lossy(source) }, unsafe {
        c_string_lossy(reason)
    });
}

unsafe extern "C" fn pairwise_fingerprint_callback(
    fingerprint: *const u8,
    len: usize,
    user_data: *mut c_void,
) {
    if user_data.is_null() {
        return;
    }
    let tx = unsafe { Box::from_raw(user_data.cast::<Sender<Vec<u8>>>()) };
    let fingerprint = if fingerprint.is_null() || len == 0 {
        Vec::new()
    } else {
        unsafe { slice::from_raw_parts(fingerprint, len).to_vec() }
    };
    let _ = tx.send(fingerprint);
}

unsafe fn take_dave_bytes(
    data: *mut u8,
    len: usize,
    context: &'static str,
) -> Result<Vec<u8>, DaveError> {
    if data.is_null() || len == 0 {
        return Err(DaveError::EmptyOutput(context));
    }
    let bytes = unsafe { slice::from_raw_parts(data, len).to_vec() };
    unsafe {
        dave_ffi::daveFree(data.cast::<c_void>());
    }
    Ok(bytes)
}

unsafe fn take_dave_u64s(data: *mut u64, len: usize) -> Vec<u64> {
    if data.is_null() || len == 0 {
        return Vec::new();
    }
    let values = unsafe { slice::from_raw_parts(data, len).to_vec() };
    unsafe {
        dave_ffi::daveFree(data.cast::<c_void>());
    }
    values
}

fn c_string_list(values: &[&str]) -> Result<Vec<CString>, DaveError> {
    values
        .iter()
        .map(|value| CString::new(*value).map_err(DaveError::from))
        .collect()
}

fn c_string_ptrs(values: &[CString]) -> Vec<*const c_char> {
    values.iter().map(|value| value.as_ptr()).collect()
}

unsafe fn c_string_lossy(value: *const c_char) -> String {
    if value.is_null() {
        "<null>".to_string()
    } else {
        unsafe { CStr::from_ptr(value) }
            .to_string_lossy()
            .into_owned()
    }
}
