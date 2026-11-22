//! Azure Blob Storage backend: byte-range reads against a blob holding the tar archive.

use std::sync::Arc;

use azure_core::credentials::TokenCredential;
use azure_core::http::Etag;
use azure_core::http::Url;
use azure_core::http::headers::HeaderName;
use azure_identity::{
    ClientSecretCredential, ManagedIdentityCredential, ManagedIdentityCredentialOptions,
    UserAssignedId, WorkloadIdentityCredential,
};
use azure_storage_blob::BlobClient;
use azure_storage_blob::models::{BlobClientDownloadOptions, HttpRange};
use bytes::Bytes;

use super::{ArchiveSource, Storage, redact};
use crate::archive::Error;

/// Bytes read to probe the archive: one tar header block.
const PROBE_LEN: u64 = 512;

/// The SDK keeps its own `ContentRange` type private, so read the header directly.
const CONTENT_RANGE: HeaderName = HeaderName::from_static("content-range");

/// Host suffixes that identify an Azure Blob endpoint, across public and sovereign clouds.
const BLOB_HOST_SUFFIXES: [&str; 3] = [
    ".blob.core.windows.net",
    ".blob.core.usgovcloudapi.net",
    ".blob.core.chinacloudapi.cn",
];

/// True when `host` is a real Azure Blob endpoint in one of the known clouds.
///
/// `Url` lowercases the host during parsing; hostnames are case-insensitive.
fn has_blob_host_suffix(host: &str) -> bool {
    BLOB_HOST_SUFFIXES
        .iter()
        .any(|suffix| host.len() > suffix.len() && host.ends_with(suffix))
}

/// True when `source` should be served by this backend.
///
/// rati has no generic HTTP backend and should not grow one — proxying a proxy makes no
/// sense — so any http(s) archive is Azure by elimination. Whether the host is *trusted
/// enough to be sent a credential* is a separate question, answered in [`credential`].
pub(super) fn is_azure_url(source: &str) -> bool {
    source.starts_with("https://") || source.starts_with("http://")
}

/// True when the URL carries a shared access signature, meaning no credential is needed.
fn has_sas_token(url: &str) -> bool {
    let Some((_, query)) = url.split_once('?') else {
        return false;
    };
    query
        .split('&')
        .any(|param| param.split_once('=').is_some_and(|(k, _)| k == "sig"))
}

/// Extract the total resource length from a `Content-Range` header value.
///
/// Per RFC 9110 §14.4 the value is `bytes <range>/<complete-length>`, where the
/// complete length is `*` when the server does not know it.
fn parse_content_range_total(header: &str) -> Option<u64> {
    header
        .strip_prefix("bytes ")?
        .rsplit_once('/')
        .map(|(_, total)| total)?
        .trim()
        .parse()
        .ok()
}

/// Non-empty environment variable, or `None`.
fn var(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

/// The credential rati presents to Azure Blob, or `None` when the request needs none.
///
/// Only a host that is demonstrably Azure Blob is ever sent a bearer token, and never over
/// plaintext. An archive behind a CDN or a custom domain therefore authorizes itself — a
/// SAS, or a public container. That costs little: a custom domain mapped straight onto a
/// storage account is http-only, which is never credentialed anyway, and the https form
/// puts a CDN edge in front, which is not the storage service the token was issued for.
///
/// The variables below are the ones every Azure SDK already reads, and the order is the one
/// `DefaultAzureCredential` uses elsewhere: explicit configuration outranks ambient. That
/// ordering is also what disambiguates `AZURE_CLIENT_ID`, which all three arms consult.
fn credential(url: &str) -> Result<Option<Arc<dyn TokenCredential>>, Error> {
    let blob_host = Url::parse(url)
        .ok()
        .filter(|u| u.scheme() == "https")
        .and_then(|u| u.host_str().map(has_blob_host_suffix))
        .unwrap_or(false);
    if !blob_host {
        tracing::warn!(
            "{} is not an https Azure Blob host; connecting anonymously",
            redact(url)
        );
        return Ok(None);
    }
    if has_sas_token(url) {
        tracing::info!("Azure credential: none (the URL carries a SAS)");
        return Ok(None);
    }

    // A projected token file: AKS, or any federated workload.
    if std::env::var_os("AZURE_FEDERATED_TOKEN_FILE").is_some() {
        tracing::info!("Azure credential: workload identity");
        return WorkloadIdentityCredential::new(None)
            .map(|c| Some(c as Arc<dyn TokenCredential>))
            .map_err(|e| Error::Io(format!("workload identity credential: {e}")));
    }

    // A service principal: the only option anywhere managed identity does not reach —
    // on-premises, another cloud, or a developer's machine.
    if let (Some(tenant), Some(client), Some(secret)) = (
        var("AZURE_TENANT_ID"),
        var("AZURE_CLIENT_ID"),
        var("AZURE_CLIENT_SECRET"),
    ) {
        tracing::info!("Azure credential: service principal");
        return ClientSecretCredential::new(&tenant, client, secret.into(), None)
            .map(|c| Some(c as Arc<dyn TokenCredential>))
            .map_err(|e| Error::Io(format!("client secret credential: {e}")));
    }

    // Azure-hosted. AZURE_CLIENT_ID names a user-assigned identity; without it, the
    // system-assigned one. Empty is not a value: an empty id reaches IMDS as `client-id=&`,
    // which can silently resolve to the system-assigned identity instead of failing.
    let user_assigned = var("AZURE_CLIENT_ID");
    tracing::info!(
        "Azure credential: {} managed identity",
        if user_assigned.is_some() {
            "user-assigned"
        } else {
            "system-assigned"
        }
    );
    let options = user_assigned.map(|id| ManagedIdentityCredentialOptions {
        user_assigned_id: Some(UserAssignedId::ClientId(id)),
        ..Default::default()
    });
    ManagedIdentityCredential::new(options)
        .map(|c| Some(c as Arc<dyn TokenCredential>))
        .map_err(|e| Error::Io(format!("managed identity credential: {e}")))
}

/// Open the archive from Azure Blob Storage.
///
/// Reads the leading tar block to pick up the blob's ETag, Last-Modified, and total
/// size in one request: `get_properties` returns its values as raw headers, whereas a
/// ranged `download` hands back a typed `BlobDownloadProperties`. The block itself is
/// handed back in [`ArchiveSource::prefetch`] so the caller need not read it again.
pub(super) async fn open_azure(url: &str) -> Result<(Storage, ArchiveSource), Error> {
    let parsed = Url::parse(url)
        .map_err(|e| Error::Protocol(format!("invalid blob URL {}: {e}", redact(url))))?;
    let credential = credential(url)?;

    // The SDK embeds the raw URL in this error, so drop it rather than forward it: the
    // message would otherwise carry a SAS to stderr through `main`'s `.expect()`.
    let client = BlobClient::new(parsed, credential, None)
        .map_err(|_| Error::Io(format!("creating blob client for {}", redact(url))))?;

    let probe = client
        .download(Some(BlobClientDownloadOptions {
            range: Some(HttpRange::new(0, PROBE_LEN)),
            ..Default::default()
        }))
        .await
        .map_err(|e| {
            // The credential is only exercised on the first request, so an auth failure
            // surfaces here rather than at construction.
            Error::Io(format!(
                "reading {} failed: {e} (check the identity has Storage Blob Data Reader \
                 on the container — account-level Owner or Contributor does not grant \
                 blob read)",
                redact(url),
            ))
        })?;

    let etag: Box<str> = probe
        .properties
        .etag
        .as_ref()
        .map(ToString::to_string)
        .ok_or_else(|| Error::Protocol("blob response carried no ETag".into()))?
        .into();

    let last_modified: Box<str> = probe
        .properties
        .last_modified
        .map(|t| httpdate::fmt_http_date(t.into()))
        .ok_or_else(|| Error::Protocol("blob response carried no Last-Modified".into()))?
        .into();

    let size = probe
        .headers
        .get_optional_str(&CONTENT_RANGE)
        .and_then(parse_content_range_total)
        .ok_or_else(|| Error::Protocol("blob response carried no usable Content-Range".into()))?;

    // The probe *is* the first tar block; keep it rather than paying for it twice.
    let prefetch = probe
        .body
        .collect()
        .await
        .map_err(|e| Error::Io(format!("reading blob probe body: {e}")))?;

    Ok((
        Storage::AzureBlob {
            client: Box::new(client),
            // Pinned so a mid-flight republish fails loudly instead of serving bytes
            // read at stale offsets. See `read_azure_range`.
            etag: etag.clone(),
        },
        ArchiveSource {
            etag,
            last_modified,
            size,
            prefetch: Some(prefetch),
        },
    ))
}

/// Read `length` bytes at `offset` from the blob.
///
/// Every read is conditional on the ETag captured when the archive was opened. The tile
/// index maps ids to byte offsets in one specific version of the blob, so if the archive
/// is replaced while rati is running those offsets now point into different data. Without
/// the guard the service happily returns whatever occupies that range, and rati serves it
/// under the old ETag and `Cache-Control: immutable` — silently poisoning client and CDN
/// caches. With it, Azure answers 412 and the read fails visibly.
fn ranged_read(etag: &str, offset: u64, length: u64) -> BlobClientDownloadOptions<'static> {
    BlobClientDownloadOptions {
        range: Some(HttpRange::new(offset, length)),
        if_match: Some(Etag::from(etag)),
        ..Default::default()
    }
}

pub(super) async fn read_azure_range(
    client: &BlobClient,
    etag: &str,
    offset: u64,
    length: u64,
) -> Result<Bytes, Error> {
    let response = client
        .download(Some(ranged_read(etag, offset, length)))
        .await
        .map_err(|e| {
            Error::Io(format!(
                "blob download(offset={offset}, len={length}): {e} \
                 (a 412 here means the archive was replaced while rati was running; restart it)"
            ))
        })?;

    response
        .body
        .collect()
        .await
        .map_err(|e| Error::Io(format!("reading blob response body: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_azure_url_test() {
        // Any http(s) archive is Azure by elimination; rati has no other HTTP backend.
        assert!(is_azure_url(
            "https://acct.blob.core.windows.net/tiles/planet.tar"
        ));
        assert!(is_azure_url("https://example.com/tiles/planet.tar"));
        assert!(is_azure_url(
            "http://127.0.0.1:10000/devstoreaccount1/valhalla/tiles.tar"
        ));

        assert!(!is_azure_url("s3://bucket/planet.tar"));
        assert!(!is_azure_url("./planet.tar"));
        assert!(!is_azure_url("/data/planet.tar"));
    }

    #[test]
    fn has_blob_host_suffix_test() {
        assert!(has_blob_host_suffix("acct.blob.core.windows.net"));
        assert!(has_blob_host_suffix("acct.blob.core.usgovcloudapi.net"));
        assert!(has_blob_host_suffix("acct.blob.core.chinacloudapi.cn"));

        // The suffix alone, with no account label, is not an endpoint.
        assert!(!has_blob_host_suffix("blob.core.windows.net"));
        assert!(!has_blob_host_suffix("example.com"));
        // A lookalike must not match by prefix.
        assert!(!has_blob_host_suffix(
            "acct.blob.core.windows.net.evil.test"
        ));
    }

    #[test]
    fn has_sas_token_test() {
        assert!(has_sas_token(
            "https://acct.blob.core.windows.net/t/p.tar?sv=2024-11-04&sig=abc%3D"
        ));
        assert!(has_sas_token(
            "https://acct.blob.core.windows.net/t/p.tar?sp=r&st=2026-01-01&sig=x"
        ));

        assert!(!has_sas_token("https://acct.blob.core.windows.net/t/p.tar"));
        // a query string without a signature is not a SAS
        assert!(!has_sas_token(
            "https://acct.blob.core.windows.net/t/p.tar?snapshot=2026-01-01"
        ));
        // a parameter merely ending in "sig" must not match
        assert!(!has_sas_token(
            "https://acct.blob.core.windows.net/t/p.tar?nosig=x"
        ));
    }

    /// A bearer token must never leave for a host that has not proved it is Azure Blob,
    /// and never over plaintext. These are the only inputs that decide it.
    #[test]
    fn only_an_https_blob_host_is_credentialed() {
        let anonymous = |u: &str| credential(u).unwrap().is_none();

        // Plaintext, whatever the host: a token must not go out in the clear.
        assert!(anonymous(
            "http://127.0.0.1:10000/devstoreaccount1/valhalla/tiles.tar"
        ));
        assert!(anonymous("http://acct.blob.core.windows.net/t/p.tar"));

        // An https host that is not Azure Blob — including one claiming the emulator
        // account in its path, which is how the round-1 leak reached a foreign host.
        assert!(anonymous("https://example.com/t/p.tar"));
        assert!(anonymous(
            "https://attacker.example/devstoreaccount1/c/p.tar"
        ));

        // A SAS already authorizes the read, so no token is needed.
        assert!(anonymous(
            "https://acct.blob.core.windows.net/t/p.tar?sv=1&sig=abc"
        ));
    }

    /// Every ranged read must be pinned to the ETag captured at open. Without it a blob
    /// replaced mid-flight is served at stale offsets under the old ETag and
    /// `Cache-Control: immutable`, poisoning client and CDN caches.
    #[test]
    fn every_ranged_read_is_pinned_to_the_etag() {
        let opts = ranged_read("\"0x8DF12581E17C90B\"", 1536, 44_504_000);
        assert_eq!(
            opts.if_match,
            Some(Etag::from("\"0x8DF12581E17C90B\"")),
            "ranged reads must carry If-Match"
        );
        // HttpRange keeps its fields private; its Display is the wire value.
        let range = opts.range.expect("a ranged read must carry a range");
        assert_eq!(range.to_string(), "bytes=1536-44505535");
    }

    #[test]
    fn parse_content_range_total_test() {
        assert_eq!(
            parse_content_range_total("bytes 0-511/85899345920"),
            Some(85_899_345_920)
        );
        assert_eq!(parse_content_range_total("bytes 0-0/1"), Some(1));

        // unsatisfiable / unknown total
        assert_eq!(parse_content_range_total("bytes 0-511/*"), None);
        assert_eq!(parse_content_range_total("bytes */1234"), Some(1234));
        assert_eq!(parse_content_range_total("garbage"), None);
        assert_eq!(parse_content_range_total(""), None);
    }
}
