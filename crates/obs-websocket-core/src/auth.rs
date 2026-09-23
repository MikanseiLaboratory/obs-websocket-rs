//! OBS WebSocket v5 authentication.
//!
//! The secret is `base64(sha256(base64(sha256(password + salt)) + challenge))`.

use alloc::string::String;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use sha2::{Digest, Sha256};

/// Builds the `authentication` string sent in `Identify` (op 1).
pub fn authentication_string(password: &str, salt: &str, challenge: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    hasher.update(salt.as_bytes());
    let secret = STANDARD.encode(hasher.finalize_reset());
    hasher.update(secret.as_bytes());
    hasher.update(challenge.as_bytes());
    STANDARD.encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::authentication_string;

    #[test]
    fn matches_known_vector() {
        // Salt and challenge are the samples from the obs-websocket v5 protocol docs.
        let secret = authentication_string(
            "supersecretpassword",
            "lM1GncleQOaCu9lT1yeUZhFYnqhsLLP1G5lAGo3ixaI=",
            "+IxH4CnCiqpX1rM9scsNynZzbOe4KhDeYcTNS3PDaeY=",
        );
        assert_eq!(secret, "1Ct943GAT+6YQUUX47Ia/ncufilbe6+oD6lY+5kaCu4=");
    }

    #[test]
    fn is_deterministic() {
        let once = authentication_string("pw", "salt", "challenge");
        let twice = authentication_string("pw", "salt", "challenge");
        assert_eq!(once, twice);
    }
}
