use hmac::{Hmac, Mac};
use sha2::Sha256;

pub fn sign_policy_revision_hmac_sha256(revision: &str, key: &str) -> Option<String> {
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(key.as_bytes()).ok()?;
    mac.update(revision.as_bytes());
    let bytes = mac.finalize().into_bytes();
    Some(hex_encode(&bytes))
}

pub fn verify_policy_revision_hmac_sha256(
    revision: &str,
    signature_hex: &str,
    key: &str,
) -> Option<bool> {
    let expected_signature = sign_policy_revision_hmac_sha256(revision, key)?;
    let received_bytes = decode_hex(signature_hex)?;
    let expected_bytes = decode_hex(&expected_signature)?;
    Some(constant_time_eq(&received_bytes, &expected_bytes))
}

pub fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{:02x}", byte));
    }
    out
}

pub fn decode_hex(value: &str) -> Option<Vec<u8>> {
    let raw = value.trim();
    if raw.is_empty() || raw.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(raw.len() / 2);
    let bytes = raw.as_bytes();
    for pair in bytes.chunks_exact(2) {
        let high = hex_nibble(pair[0])?;
        let low = hex_nibble(pair[1])?;
        out.push((high << 4) | low);
    }
    Some(out)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::{
        constant_time_eq, decode_hex, sign_policy_revision_hmac_sha256,
        verify_policy_revision_hmac_sha256,
    };

    #[test]
    fn sign_is_stable_and_keyed() {
        let first = sign_policy_revision_hmac_sha256("rev-1", "secret-a").expect("signature");
        let same = sign_policy_revision_hmac_sha256("rev-1", "secret-a").expect("signature");
        let different = sign_policy_revision_hmac_sha256("rev-1", "secret-b").expect("signature");
        assert_eq!(first, same);
        assert_ne!(first, different);
        assert_eq!(first.len(), 64);
    }

    #[test]
    fn decode_hex_and_constant_time_eq_work_for_mixed_case_input() {
        let mixed = decode_hex("Aa10").expect("mixed case hex");
        let lower = decode_hex("aa10").expect("lower hex");
        assert!(constant_time_eq(&mixed, &lower));
        assert!(decode_hex("xyz").is_none());
    }

    #[test]
    fn verify_hmac_signature_returns_boolean_result() {
        let signature = sign_policy_revision_hmac_sha256("rev-1", "secret-a").expect("signature");
        let ok = verify_policy_revision_hmac_sha256("rev-1", &signature, "secret-a")
            .expect("verification should parse");
        assert!(ok);
        let not_ok = verify_policy_revision_hmac_sha256("rev-1", &signature, "secret-b")
            .expect("verification should parse");
        assert!(!not_ok);
        assert!(verify_policy_revision_hmac_sha256("rev-1", "zz", "secret-a").is_none());
    }
}
