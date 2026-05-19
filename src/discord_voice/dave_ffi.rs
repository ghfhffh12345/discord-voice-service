#![allow(non_camel_case_types)]

use std::ffi::{c_char, c_uint, c_void};

pub type DaveSessionHandle = *mut c_void;
pub type DaveCommitResultHandle = *mut c_void;
pub type DaveWelcomeResultHandle = *mut c_void;
pub type DaveKeyRatchetHandle = *mut c_void;
pub type DaveEncryptorHandle = *mut c_void;
pub type DaveDecryptorHandle = *mut c_void;
pub type DaveExternalSenderHandle = *mut c_void;

pub type DaveMlsFailureCallback =
    Option<unsafe extern "C" fn(*const c_char, *const c_char, *mut c_void)>;
pub type DavePairwiseFingerprintCallback =
    Option<unsafe extern "C" fn(*const u8, usize, *mut c_void)>;

pub const DAVE_CODEC_OPUS: c_uint = 1;
pub const DAVE_MEDIA_TYPE_AUDIO: c_uint = 0;
pub const DAVE_MEDIA_TYPE_VIDEO: c_uint = 1;
pub const DAVE_ENCRYPTOR_RESULT_CODE_SUCCESS: c_uint = 0;
pub const DAVE_DECRYPTOR_RESULT_CODE_SUCCESS: c_uint = 0;

unsafe extern "C" {
    pub fn daveMaxSupportedProtocolVersion() -> u16;
    pub fn daveFree(ptr: *mut c_void);

    pub fn daveSessionCreate(
        context: *mut c_void,
        auth_session_id: *const c_char,
        callback: DaveMlsFailureCallback,
        user_data: *mut c_void,
    ) -> DaveSessionHandle;
    pub fn daveSessionDestroy(session: DaveSessionHandle);
    pub fn daveSessionInit(
        session: DaveSessionHandle,
        version: u16,
        group_id: u64,
        self_user_id: *const c_char,
    );
    pub fn daveSessionSetExternalSender(
        session: DaveSessionHandle,
        external_sender: *const u8,
        length: usize,
    );
    pub fn daveSessionGetProtocolVersion(session: DaveSessionHandle) -> u16;
    pub fn daveSessionGetMarshalledKeyPackage(
        session: DaveSessionHandle,
        key_package: *mut *mut u8,
        length: *mut usize,
    );
    pub fn daveSessionProcessProposals(
        session: DaveSessionHandle,
        proposals: *const u8,
        length: usize,
        recognized_user_ids: *const *const c_char,
        recognized_user_ids_length: usize,
        commit_welcome_bytes: *mut *mut u8,
        commit_welcome_bytes_length: *mut usize,
    );
    pub fn daveSessionProcessCommit(
        session: DaveSessionHandle,
        commit: *const u8,
        length: usize,
    ) -> DaveCommitResultHandle;
    pub fn daveSessionProcessWelcome(
        session: DaveSessionHandle,
        welcome: *const u8,
        length: usize,
        recognized_user_ids: *const *const c_char,
        recognized_user_ids_length: usize,
    ) -> DaveWelcomeResultHandle;
    pub fn daveSessionGetLastEpochAuthenticator(
        session: DaveSessionHandle,
        authenticator: *mut *mut u8,
        length: *mut usize,
    );
    pub fn daveSessionGetPairwiseFingerprint(
        session: DaveSessionHandle,
        version: u16,
        user_id: *const c_char,
        callback: DavePairwiseFingerprintCallback,
        user_data: *mut c_void,
    );
    pub fn daveSessionGetKeyRatchet(
        session: DaveSessionHandle,
        user_id: *const c_char,
    ) -> DaveKeyRatchetHandle;

    pub fn daveKeyRatchetDestroy(key_ratchet: DaveKeyRatchetHandle);

    pub fn daveCommitResultIsFailed(commit_result: DaveCommitResultHandle) -> bool;
    pub fn daveCommitResultIsIgnored(commit_result: DaveCommitResultHandle) -> bool;
    pub fn daveCommitResultGetRosterMemberIds(
        commit_result: DaveCommitResultHandle,
        roster_ids: *mut *mut u64,
        roster_ids_length: *mut usize,
    );
    pub fn daveCommitResultDestroy(commit_result: DaveCommitResultHandle);

    pub fn daveWelcomeResultGetRosterMemberIds(
        welcome_result: DaveWelcomeResultHandle,
        roster_ids: *mut *mut u64,
        roster_ids_length: *mut usize,
    );
    pub fn daveWelcomeResultDestroy(welcome_result: DaveWelcomeResultHandle);

    pub fn daveEncryptorCreate() -> DaveEncryptorHandle;
    pub fn daveEncryptorDestroy(encryptor: DaveEncryptorHandle);
    pub fn daveEncryptorSetKeyRatchet(
        encryptor: DaveEncryptorHandle,
        key_ratchet: DaveKeyRatchetHandle,
    );
    pub fn daveEncryptorAssignSsrcToCodec(encryptor: DaveEncryptorHandle, ssrc: u32, codec: c_uint);
    pub fn daveEncryptorGetMaxCiphertextByteSize(
        encryptor: DaveEncryptorHandle,
        media_type: c_uint,
        frame_size: usize,
    ) -> usize;
    pub fn daveEncryptorEncrypt(
        encryptor: DaveEncryptorHandle,
        media_type: c_uint,
        ssrc: u32,
        frame: *const u8,
        frame_length: usize,
        encrypted_frame: *mut u8,
        encrypted_frame_capacity: usize,
        bytes_written: *mut usize,
    ) -> c_uint;

    pub fn daveDecryptorCreate() -> DaveDecryptorHandle;
    pub fn daveDecryptorDestroy(decryptor: DaveDecryptorHandle);
    pub fn daveDecryptorTransitionToKeyRatchet(
        decryptor: DaveDecryptorHandle,
        key_ratchet: DaveKeyRatchetHandle,
    );
    pub fn daveDecryptorGetMaxPlaintextByteSize(
        decryptor: DaveDecryptorHandle,
        media_type: c_uint,
        encrypted_frame_size: usize,
    ) -> usize;
    pub fn daveDecryptorDecrypt(
        decryptor: DaveDecryptorHandle,
        media_type: c_uint,
        encrypted_frame: *const u8,
        encrypted_frame_length: usize,
        frame: *mut u8,
        frame_capacity: usize,
        bytes_written: *mut usize,
    ) -> c_uint;

    pub fn daveExternalSenderCreate(group_id: u64) -> DaveExternalSenderHandle;
    pub fn daveExternalSenderDestroy(external_sender: DaveExternalSenderHandle);
    pub fn daveExternalSenderGetMarshalledExternalSender(
        external_sender: DaveExternalSenderHandle,
        marshalled_external_sender: *mut *mut u8,
        length: *mut usize,
    );
    pub fn daveExternalSenderProposeAdd(
        external_sender: DaveExternalSenderHandle,
        epoch: u32,
        key_package: *mut u8,
        key_package_length: usize,
        proposal: *mut *mut u8,
        proposal_length: *mut usize,
    );
    pub fn daveExternalSenderSplitCommitWelcome(
        external_sender: DaveExternalSenderHandle,
        commit_welcome: *mut u8,
        commit_welcome_length: usize,
        commit: *mut *mut u8,
        commit_length: *mut usize,
        welcome: *mut *mut u8,
        welcome_length: *mut usize,
    );
}
