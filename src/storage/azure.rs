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

use super::{Backend, Storage, redact};
use crate::archive::Error;

// The SDK keeps its own `ContentRange` type private
const CONTENT_RANGE: HeaderName = HeaderName::from_static("content-range");

const BLOB_HOST_SUFFIXES: [&str; 3] = [
    ".blob.core.windows.net",
    ".blob.core.usgovcloudapi.net",
    ".blob.core.chinacloudapi.cn",
];

/// Uses a ranged download instead of `get_properties`, as only the former returns typed
/// properties.
pub(super) async fn open(url: &str) -> Result<Storage, Error> {
    let parsed = Url::parse(url)
        .map_err(|e| Error::Protocol(format!("invalid blob URL {}: {e}", redact(url))))?;
    let credential = credential(&parsed)?;

    // The SDK error embeds the raw URL with a SAS
    let client = BlobClient::new(parsed, credential, None)
        .map_err(|_| Error::Io(format!("creating blob client for {}", redact(url))))?;

    let probe = client
        .download(Some(BlobClientDownloadOptions {
            range: Some(HttpRange::new(0, 1)),
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

    let etag = probe
        .properties
        .etag
        .as_ref()
        .ok_or_else(|| Error::Protocol("blob response carried no ETag".into()))?;
    let last_modified = probe
        .properties
        .last_modified
        .ok_or_else(|| Error::Protocol("blob response carried no Last-Modified".into()))?;
    let size = probe
        .headers
        .get_optional_str(&CONTENT_RANGE)
        .and_then(parse_content_range_total)
        .ok_or_else(|| Error::Protocol("blob response carried no usable Content-Range".into()))?;

    Ok(Storage {
        etag: etag.to_string().into(),
        last_modified: httpdate::fmt_http_date(last_modified.into()).into(),
        size,
        backend: Backend::Azure(Box::new(client)),
    })
}

pub(super) async fn read_range(
    client: &BlobClient,
    etag: &str,
    offset: u64,
    length: u64,
) -> Result<Bytes, Error> {
    let response = client
        .download(Some(BlobClientDownloadOptions {
            range: Some(HttpRange::new(offset, length)),
            if_match: Some(Etag::from(etag)),
            ..Default::default()
        }))
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

/// Bearer token goes only to an https Azure Blob host. Credentials are picked from the
/// standard Azure env vars in `DefaultAzureCredential` order.
fn credential(url: &Url) -> Result<Option<Arc<dyn TokenCredential>>, Error> {
    let blob_host = url.scheme() == "https"
        && url.host_str().is_some_and(|host| {
            BLOB_HOST_SUFFIXES
                .iter()
                .any(|suffix| host.len() > suffix.len() && host.ends_with(suffix))
        });
    if !blob_host {
        tracing::warn!(
            "{} is not an https Azure Blob host; connecting anonymously",
            redact(url.as_str())
        );
        return Ok(None);
    }
    if url.query_pairs().any(|(k, _)| k == "sig") {
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

fn var(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_test() {
        let anonymous = |u: &str| credential(&Url::parse(u).unwrap()).unwrap().is_none();

        assert!(anonymous(
            "http://127.0.0.1:10000/devstoreaccount1/valhalla/tiles.tar"
        ));
        assert!(anonymous("http://acct.blob.core.windows.net/t/p.tar"));
        assert!(anonymous("https://example.com/t/p.tar"));
        assert!(anonymous("https://blob.core.windows.net/t/p.tar"));
        assert!(anonymous(
            "https://acct.blob.core.windows.net.evil.test/t/p.tar"
        ));
        assert!(anonymous(
            "https://acct.blob.core.windows.net/t/p.tar?sv=1&sig=abc"
        ));
        assert!(!anonymous("https://acct.blob.core.windows.net/t/p.tar"));
        assert!(!anonymous(
            "https://acct.blob.core.chinacloudapi.cn/t/p.tar?nosig=x"
        ));
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
