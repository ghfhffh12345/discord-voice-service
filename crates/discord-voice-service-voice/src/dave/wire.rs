use super::DaveError;

const LEN_BYTES: usize = size_of::<u32>();

pub(crate) fn pack_commit_welcome(commit: &[u8], welcome: Option<&[u8]>) -> Vec<u8> {
    let welcome = welcome.unwrap_or_default();
    let mut out = Vec::with_capacity(LEN_BYTES + commit.len() + LEN_BYTES + welcome.len());
    out.extend_from_slice(&(commit.len() as u32).to_be_bytes());
    out.extend_from_slice(commit);
    out.extend_from_slice(&(welcome.len() as u32).to_be_bytes());
    out.extend_from_slice(welcome);
    out
}

pub(crate) fn unpack_commit_welcome(
    commit_welcome: &[u8],
) -> Result<(Vec<u8>, Vec<u8>), DaveError> {
    let (commit_len, rest) = read_len_prefixed(commit_welcome)?;
    let (commit, rest) = take_exact(rest, commit_len)?;
    let (welcome_len, rest) = read_len_prefixed(rest)?;
    let (welcome, rest) = take_exact(rest, welcome_len)?;
    if !rest.is_empty() {
        return Err(DaveError::MalformedCommitWelcome);
    }
    Ok((commit.to_vec(), welcome.to_vec()))
}

fn read_len_prefixed(input: &[u8]) -> Result<(usize, &[u8]), DaveError> {
    let (len, rest) = take_exact(input, LEN_BYTES)?;
    Ok((
        u32::from_be_bytes(len.try_into().expect("u32 length prefix")) as usize,
        rest,
    ))
}

fn take_exact(input: &[u8], len: usize) -> Result<(&[u8], &[u8]), DaveError> {
    if input.len() < len {
        return Err(DaveError::MalformedCommitWelcome);
    }
    Ok(input.split_at(len))
}
