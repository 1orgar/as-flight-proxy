use arrow_flight::BasicAuth;
use base64::Engine;
use base64::engine::general_purpose::{STANDARD as BASE64_STD, URL_SAFE_NO_PAD as BASE64_URL};
use hmac::{Hmac, Mac};
use prost::Message;
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};
use subtle::ConstantTimeEq;
use tonic::metadata::MetadataMap;

use crate::config::AuthConfig;
use crate::error::{ProxyError, Result};

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone)]
pub struct Authenticator {
    config: AuthConfig,
}

impl Authenticator {
    pub fn new(config: AuthConfig) -> Self {
        Self { config }
    }

    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Verify username and password in constant time.
    pub fn verify_credentials(&self, username: &str, password: &str) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let user_ok = constant_time_eq_str(username.as_bytes(), self.config.username.as_bytes());
        let pass_ok = constant_time_eq_str(password.as_bytes(), self.config.password.as_bytes());

        if user_ok & pass_ok {
            Ok(())
        } else {
            Err(ProxyError::Unauthenticated(
                "Invalid username or password".to_string(),
            ))
        }
    }

    /// Issue a signed Bearer token for an authenticated user.
    pub fn issue_token(&self, username: &str) -> Result<String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| ProxyError::Internal(format!("System clock error: {e}")))?
            .as_secs();
        let exp = now.saturating_add(self.config.token_ttl_secs);
        let payload = format!("{exp}:{username}");
        let payload_b64 = BASE64_URL.encode(payload.as_bytes());
        let sig_b64 = self.sign_payload(&payload_b64)?;
        Ok(format!("{payload_b64}.{sig_b64}"))
    }

    /// Verify a signed Bearer token.
    pub fn verify_token(&self, token: &str) -> Result<String> {
        if !self.config.enabled {
            return Ok(self.config.username.clone());
        }

        let (payload_b64, sig_b64) = token
            .split_once('.')
            .ok_or_else(|| ProxyError::Unauthenticated("Malformed Bearer token".to_string()))?;

        let expected_sig = self.sign_payload(payload_b64)?;
        if !constant_time_eq_str(sig_b64.as_bytes(), expected_sig.as_bytes()) {
            return Err(ProxyError::Unauthenticated(
                "Invalid Bearer token signature".to_string(),
            ));
        }

        let decoded_bytes = BASE64_URL.decode(payload_b64).map_err(|_| {
            ProxyError::Unauthenticated("Invalid Bearer token payload encoding".to_string())
        })?;
        let payload_str = std::str::from_utf8(&decoded_bytes).map_err(|_| {
            ProxyError::Unauthenticated("Invalid UTF-8 in Bearer token payload".to_string())
        })?;

        let (exp_str, username) = payload_str.split_once(':').ok_or_else(|| {
            ProxyError::Unauthenticated("Invalid Bearer token payload structure".to_string())
        })?;
        let exp: u64 = exp_str.parse().map_err(|_| {
            ProxyError::Unauthenticated("Invalid expiration in Bearer token".to_string())
        })?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| ProxyError::Internal(format!("System clock error: {e}")))?
            .as_secs();

        if now > exp {
            return Err(ProxyError::Unauthenticated(
                "Bearer token has expired".to_string(),
            ));
        }

        if !constant_time_eq_str(username.as_bytes(), self.config.username.as_bytes()) {
            return Err(ProxyError::Unauthenticated(
                "Unknown subject in Bearer token".to_string(),
            ));
        }

        Ok(username.to_string())
    }

    /// Authenticate an incoming gRPC request via `authorization` metadata header
    /// (supports both `Bearer <token>` and `Basic <base64(user:pass)>`).
    pub fn verify_metadata(&self, metadata: &MetadataMap) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let auth_val = metadata
            .get("authorization")
            .or_else(|| metadata.get("Authorization"))
            .ok_or_else(|| {
                ProxyError::Unauthenticated("Missing 'authorization' header".to_string())
            })?;

        let auth_str = auth_val.to_str().map_err(|_| {
            ProxyError::Unauthenticated("Invalid ASCII in 'authorization' header".to_string())
        })?;

        if let Some(bearer) = strip_prefix_case_insensitive(auth_str.trim(), "Bearer ") {
            self.verify_token(bearer.trim())?;
            Ok(())
        } else if let Some(basic) = strip_prefix_case_insensitive(auth_str.trim(), "Basic ") {
            let (user, pass) = decode_basic_header(basic.trim())?;
            self.verify_credentials(&user, &pass)
        } else {
            // Also allow raw token if passed directly
            self.verify_token(auth_str.trim())?;
            Ok(())
        }
    }

    /// Authenticate a `Handshake` request using either the `authorization` header
    /// or `HandshakeRequest.payload` (Arrow Flight `BasicAuth` protobuf or `user:pass`).
    /// Returns a freshly issued `Bearer` token.
    pub fn authenticate_handshake(&self, metadata: &MetadataMap, payload: &[u8]) -> Result<String> {
        if !self.config.enabled {
            return self.issue_token(&self.config.username);
        }

        // 1. Check if credentials are in the `authorization` header (e.g. Basic auth)
        if let Some(auth_val) = metadata
            .get("authorization")
            .or_else(|| metadata.get("Authorization"))
        {
            if let Ok(auth_str) = auth_val.to_str() {
                if let Some(basic) = strip_prefix_case_insensitive(auth_str.trim(), "Basic ") {
                    let (user, pass) = decode_basic_header(basic.trim())?;
                    self.verify_credentials(&user, &pass)?;
                    return self.issue_token(&user);
                } else if let Some(bearer) =
                    strip_prefix_case_insensitive(auth_str.trim(), "Bearer ")
                {
                    let user = self.verify_token(bearer.trim())?;
                    return self.issue_token(&user);
                }
            }
        }

        // 2. Check HandshakeRequest.payload (Arrow Flight BasicAuth protobuf or UTF-8 user:pass)
        if !payload.is_empty() {
            if let Ok(basic_proto) = BasicAuth::decode(payload) {
                if !basic_proto.username.is_empty() || !basic_proto.password.is_empty() {
                    self.verify_credentials(&basic_proto.username, &basic_proto.password)?;
                    return self.issue_token(&basic_proto.username);
                }
            }

            if let Ok(text) = std::str::from_utf8(payload) {
                if let Some((user, pass)) = text.split_once(':') {
                    self.verify_credentials(user, pass)?;
                    return self.issue_token(user);
                }
            }
        }

        Err(ProxyError::Unauthenticated(
            "Handshake requires BasicAuth protobuf payload or Basic authorization header"
                .to_string(),
        ))
    }

    fn sign_payload(&self, payload_b64: &str) -> Result<String> {
        let mut mac = HmacSha256::new_from_slice(self.config.token_secret.as_bytes())
            .map_err(|e| ProxyError::Internal(format!("HMAC init error: {e}")))?;
        mac.update(payload_b64.as_bytes());
        let bytes = mac.finalize().into_bytes();
        Ok(BASE64_URL.encode(bytes))
    }
}

fn decode_basic_header(b64: &str) -> Result<(String, String)> {
    let decoded = BASE64_STD.decode(b64).map_err(|_| {
        ProxyError::Unauthenticated("Invalid base64 in Basic authorization header".to_string())
    })?;
    let creds = std::str::from_utf8(&decoded).map_err(|_| {
        ProxyError::Unauthenticated("Invalid UTF-8 in Basic authorization header".to_string())
    })?;
    let (user, pass) = creds.split_once(':').ok_or_else(|| {
        ProxyError::Unauthenticated(
            "Basic authorization must contain 'username:password'".to_string(),
        )
    })?;
    Ok((user.to_string(), pass.to_string()))
}

fn strip_prefix_case_insensitive<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    if s.len() >= prefix.len() && s[..prefix.len()].eq_ignore_ascii_case(prefix) {
        Some(&s[prefix.len()..])
    } else {
        None
    }
}

/// Constant-time byte comparison that conceals both content and slice length by comparing
/// fixed-size SHA-256 digests alongside constant-time length equality.
fn constant_time_eq_str(a: &[u8], b: &[u8]) -> bool {
    let digest_a = Sha256::digest(a);
    let digest_b = Sha256::digest(b);
    let digest_eq = digest_a.ct_eq(&digest_b);
    let len_eq = (a.len() as u64).ct_eq(&(b.len() as u64));
    bool::from(digest_eq & len_eq)
}
