//! TLS for the native PostgreSQL engine (rustls), plus helpers shared with
//! the MySQL engine.
//!
//! `sslmode` follows libpq/asyncpg (default `prefer`):
//!
//! * `disable` – plaintext.
//! * `allow` / `prefer` – TLS without certificate verification; plaintext
//!   only if the server refuses TLS.
//! * `require` – TLS is mandatory; the chain is verified only when a root
//!   certificate is available, as in libpq.
//! * `verify-ca` – TLS, and the chain must verify against the root.
//! * `verify-full` – as `verify-ca`, and the host name must match.
//!
//! Roots come from `sslrootcert` (a PEM file, or `system` for the OS trust
//! store), `PGSSLROOTCERT`, or `~/.postgresql/root.crt`. Other TLS settings
//! (client certificates, CRLs, protocol bounds) are not supported natively;
//! the Python driver selects asyncpg for them.

/**
 * Copyright (C) 2026 Axis Communications AB, Lund, Sweden
 *
 * Use of this source code is governed by an MIT-style
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{WebPkiSupportedAlgorithms, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio_postgres::config::SslMode;
use tokio_postgres_rustls::MakeRustlsConnect;

pub type BoxErr = Box<dyn std::error::Error + Sync + Send>;

/// Install the process-wide rustls provider; mongodb and mysql_async link
/// both ring and aws-lc-rs, so `ClientConfig::builder()` needs one picked.
pub fn install_default_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// The operating system's trusted root certificates.
pub fn system_roots() -> Result<Vec<CertificateDer<'static>>, BoxErr> {
    let result = rustls_native_certs::load_native_certs();
    match result.errors.first() {
        Some(e) if result.certs.is_empty() => {
            Err(format!("could not load the system root certificates: {e}").into())
        }
        _ => Ok(result.certs),
    }
}

/// Accepts any certificate (`roots: None`) or verifies only the chain
/// (sslmode verify-ca), always checking the handshake signatures.
#[derive(Debug)]
struct Lenient {
    chain: Option<Arc<WebPkiServerVerifier>>,
    algorithms: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for Lenient {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let Some(chain) = &self.chain else {
            return Ok(ServerCertVerified::assertion());
        };
        // WebPkiServerVerifier checks the chain before the name, so a name
        // error means the chain itself verified.
        match chain.verify_server_cert(end_entity, intermediates, server_name, ocsp, now) {
            Err(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

fn root_certificates(sslrootcert: Option<String>) -> Result<Option<RootCertStore>, BoxErr> {
    let path = match sslrootcert.or_else(|| env("PGSSLROOTCERT")) {
        Some(value) if value == "system" => {
            let mut store = RootCertStore::empty();
            store.add_parsable_certificates(system_roots()?);
            return Ok(Some(store));
        }
        Some(path) => PathBuf::from(path),
        None => match env("HOME") {
            Some(home) if Path::new(&home).join(".postgresql/root.crt").is_file() => {
                Path::new(&home).join(".postgresql/root.crt")
            }
            _ => return Ok(None),
        },
    };
    let mut store = RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(&path)
        .map_err(|e| format!("could not read root certificate file {path:?}: {e}"))?
    {
        store.add(cert.map_err(|e| format!("invalid certificate in {path:?}: {e}"))?)?;
    }
    Ok(Some(store))
}

/// Resolve `sslmode` (falling back to `PGSSLMODE`, then `prefer`) and
/// `sslrootcert` into tokio-postgres' mode and a TLS connector (`None` for
/// `disable`).
pub fn pg_tls(
    sslmode: Option<String>,
    sslrootcert: Option<String>,
) -> Result<Option<(SslMode, MakeRustlsConnect)>, BoxErr> {
    let mode = sslmode
        .or_else(|| env("PGSSLMODE"))
        .unwrap_or_else(|| "prefer".into())
        .to_ascii_lowercase()
        .replace('_', "-");
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let algorithms = provider.signature_verification_algorithms;
    let lenient =
        |chain| -> Arc<dyn ServerCertVerifier> { Arc::new(Lenient { chain, algorithms }) };
    let webpki = |roots: RootCertStore| {
        WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider.clone()).build()
    };
    let roots = match mode.as_str() {
        "disable" => return Ok(None),
        "allow" | "prefer" => None,
        "require" | "verify-ca" | "verify-full" => root_certificates(sslrootcert)?,
        other => {
            return Err(format!(
                "invalid sslmode {other:?}: expected disable, allow, prefer, require, \
                 verify-ca or verify-full"
            )
            .into());
        }
    };
    let verifier = match (mode.as_str(), roots) {
        ("verify-full", Some(roots)) => webpki(roots)? as Arc<dyn ServerCertVerifier>,
        ("verify-ca" | "require", Some(roots)) => lenient(Some(webpki(roots)?)),
        ("verify-ca" | "verify-full", None) => {
            return Err("no root certificate for sslmode verify-ca/verify-full: \
                        ~/.postgresql/root.crt does not exist; set sslrootcert=<path> \
                        (or sslrootcert=system for the OS trust store)"
                .into());
        }
        _ => lenient(None),
    };
    let config = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    let ssl_mode = if matches!(mode.as_str(), "allow" | "prefer") {
        SslMode::Prefer
    } else {
        SslMode::Require
    };
    Ok(Some((ssl_mode, MakeRustlsConnect::new(config))))
}

/// Remove `sslmode` and `sslrootcert` from a URL's query, returning the URL
/// and their (percent-decoded) values.
pub fn split_ssl_params(url: &str) -> (String, Option<String>, Option<String>) {
    let Some((base, query)) = url.split_once('?') else {
        return (url.to_owned(), None, None);
    };
    let (mut sslmode, mut sslrootcert, mut kept) = (None, None, Vec::new());
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let value = || {
            percent_encoding::percent_decode_str(value)
                .decode_utf8_lossy()
                .into_owned()
        };
        match key {
            "sslmode" => sslmode = Some(value()),
            "sslrootcert" => sslrootcert = Some(value()),
            _ => kept.push(pair),
        }
    }
    let url = if kept.is_empty() {
        base.to_owned()
    } else {
        format!("{base}?{}", kept.join("&"))
    };
    (url, sslmode, sslrootcert)
}
