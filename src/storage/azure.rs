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

const PROBE_LEN: u64 = 512;

// The SDK keeps its own `ContentRange` type private
const CONTENT_RANGE: HeaderName = HeaderName::from_static("content-range");

const BLOB_HOST_SUFFIXES: [&str; 3] = [
    ".blob.core.windows.net",
    ".blob.core.usgovcloudapi.net",
    ".blob.core.chinacloudapi.cn",
];

fn has_blob_host_suffix(host: &str) -> bool {
    BLOB_HOST_SUFFIXES
        .iter()
        .any(|suffix| host.len() > suffix.len() && host.ends_with(suffix))
}

/// rati has no generic HTTP backend, so any http(s) archive is Azure.
pub(super) fn is_azure_url(source: &str) -> bool {
    source.starts_with("https://") || source.starts_with("http://")
}

fn has_sas_token(url: &str) -> bool {
    let Some((_, query)) = url.split_once('?') else {
        return false;
    };
    query
        .split('&')
        .any(|param| param.split_once('=').is_some_and(|(k, _)| k == "sig"))
}

/// Total length from `bytes <range>/<complete-length>`, see RFC 9110 §14.4.
fn parse_content_range_total(header: &str) -> Option<u64> {
    header
        .strip_prefix("bytes ")?
        .rsplit_once('/')
        .map(|(_, total)| total)?
        .trim()
        .parse()
        .ok()
}

fn var(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

/// Bearer token goes only to an https Azure Blob host. Credentials are picked from the
/// standard Azure env vars in `DefaultAzureCredential` order.
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

    if std::env::var_os("AZURE_FEDERATED_TOKEN_FILE").is_some() {
        tracing::info!("Azure credential: workload identity");
        return WorkloadIdentityCredential::new(None)
            .map(|c| Some(c as Arc<dyn TokenCredential>))
            .map_err(|e| Error::Io(format!("workload identity credential: {e}")));
    }

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

    // An empty id would reach IMDS as `client-id=&` and silently pick the system-assigned one
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

/// Uses a ranged download instead of `get_properties`, as it returns typed properties
/// and the first tar block in one request.
pub(super) async fn open_azure(url: &str) -> Result<(Storage, ArchiveSource), Error> {
    let parsed = Url::parse(url)
        .map_err(|e| Error::Protocol(format!("invalid blob URL {}: {e}", redact(url))))?;
    let credential = credential(url)?;

    // The SDK error embeds the raw URL with a SAS
    let client = BlobClient::new(parsed, credential, None)
        .map_err(|_| Error::Io(format!("creating blob client for {}", redact(url))))?;

    let probe = client
        .download(Some(BlobClientDownloadOptions {
            range: Some(HttpRange::new(0, PROBE_LEN)),
            ..Default::default()
        }))
        .await
        .map_err(|e| {
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

    let prefetch = probe
        .body
        .collect()
        .await
        .map_err(|e| Error::Io(format!("reading blob probe body: {e}")))?;

    Ok((
        Storage::AzureBlob {
            client: Box::new(client),
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

fn ranged_read(etag: &str, offset: u64, length: u64) -> BlobClientDownloadOptions<'static> {
    BlobClientDownloadOptions {
        range: Some(HttpRange::new(offset, length)),
        if_match: Some(Etag::from(etag)),
        ..Default::default()
    }
}

/// Reads are pinned to the ETag the index was built from, so a replaced archive fails
/// with 412 instead of serving stale offsets as `immutable`.
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

        assert!(!has_blob_host_suffix("blob.core.windows.net"));
        assert!(!has_blob_host_suffix("example.com"));
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
        assert!(!has_sas_token(
            "https://acct.blob.core.windows.net/t/p.tar?snapshot=2026-01-01"
        ));
        assert!(!has_sas_token(
            "https://acct.blob.core.windows.net/t/p.tar?nosig=x"
        ));
    }

    #[test]
    fn credential_test() {
        let anonymous = |u: &str| credential(u).unwrap().is_none();

        assert!(anonymous(
            "http://127.0.0.1:10000/devstoreaccount1/valhalla/tiles.tar"
        ));
        assert!(anonymous("http://acct.blob.core.windows.net/t/p.tar"));
        assert!(anonymous("https://example.com/t/p.tar"));
        assert!(anonymous(
            "https://attacker.example/devstoreaccount1/c/p.tar"
        ));
        assert!(anonymous(
            "https://acct.blob.core.windows.net/t/p.tar?sv=1&sig=abc"
        ));
    }

    #[test]
    fn ranged_read_test() {
        let opts = ranged_read("\"0x8DF12581E17C90B\"", 1536, 44_504_000);
        assert_eq!(opts.if_match, Some(Etag::from("\"0x8DF12581E17C90B\"")));
        assert_eq!(opts.range.unwrap().to_string(), "bytes=1536-44505535");
    }

    #[test]
    fn parse_content_range_total_test() {
        assert_eq!(
            parse_content_range_total("bytes 0-511/85899345920"),
            Some(85_899_345_920)
        );
        assert_eq!(parse_content_range_total("bytes 0-0/1"), Some(1));
        assert_eq!(parse_content_range_total("bytes */1234"), Some(1234));

        assert_eq!(parse_content_range_total("bytes 0-511/*"), None);
        assert_eq!(parse_content_range_total("garbage"), None);
        assert_eq!(parse_content_range_total(""), None);
    }
}
