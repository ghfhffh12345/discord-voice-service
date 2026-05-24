use std::num::NonZeroU16;

use openmls::prelude::{
    BasicCredential, Ciphersuite, ExternalProposal, ExternalSender, GroupEpoch, GroupId,
    KeyPackageIn, OpenMlsProvider, ProtocolVersion, SenderExtensionIndex,
    tls_codec::{DeserializeBytes, Serialize, VLBytes},
};
use openmls_basic_credential::SignatureKeyPair;
use openmls_rust_crypto::OpenMlsRustCrypto;

use super::DaveError;
use super::wire::unpack_commit_welcome;

#[doc(hidden)]
pub struct DaveExternalSender {
    group_id: GroupId,
    signer: SignatureKeyPair,
    external_sender: ExternalSender,
    provider: OpenMlsRustCrypto,
}

impl DaveExternalSender {
    pub fn new(group_id: u64) -> Result<Self, DaveError> {
        let ciphersuite = dave_ciphersuite();
        let signer = SignatureKeyPair::new(ciphersuite.signature_algorithm())
            .map_err(|err| DaveError::operation("external sender key pair", err))?;
        let credential = BasicCredential::new(group_id.to_be_bytes().into());
        let external_sender = ExternalSender::new(signer.public().into(), credential.into());

        Ok(Self {
            group_id: GroupId::from_slice(&group_id.to_be_bytes()),
            signer,
            external_sender,
            provider: OpenMlsRustCrypto::default(),
        })
    }

    pub fn marshalled_external_sender(&self) -> Result<Vec<u8>, DaveError> {
        self.external_sender
            .tls_serialize_detached()
            .map_err(|err| DaveError::operation("external sender", err))
    }

    pub fn propose_add(&self, epoch: u32, key_package: &[u8]) -> Result<Vec<u8>, DaveError> {
        let key_package = KeyPackageIn::tls_deserialize_exact_bytes(key_package)
            .map_err(|err| DaveError::operation("add proposal key package deserialize", err))?
            .validate(self.provider.crypto(), ProtocolVersion::Mls10)
            .map_err(|err| DaveError::operation("add proposal key package validate", err))?;
        let proposal = ExternalProposal::new_add::<OpenMlsRustCrypto>(
            key_package,
            self.group_id.clone(),
            GroupEpoch::from(u64::from(epoch)),
            &self.signer,
            SenderExtensionIndex::new(0),
        )
        .map_err(|err| DaveError::operation("add proposal", err))?;
        let proposal = proposal
            .tls_serialize_detached()
            .map_err(|err| DaveError::operation("add proposal serialize", err))?;

        VLBytes::new(proposal)
            .tls_serialize_detached()
            .map_err(|err| DaveError::operation("add proposal payload", err))
    }

    pub fn split_commit_welcome(
        &self,
        commit_welcome: &[u8],
    ) -> Result<(Vec<u8>, Vec<u8>), DaveError> {
        unpack_commit_welcome(commit_welcome)
    }
}

fn dave_ciphersuite() -> Ciphersuite {
    let protocol_version = NonZeroU16::new(davey::DAVE_PROTOCOL_VERSION)
        .expect("davey exposes a non-zero protocol version");
    match protocol_version.get() {
        1 => Ciphersuite::MLS_128_DHKEMP256_AES128GCM_SHA256_P256,
        _ => unreachable!("unsupported davey protocol version"),
    }
}
