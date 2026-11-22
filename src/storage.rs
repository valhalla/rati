#[cfg(feature = "azure")]
pub mod azure;

use std::sync::Arc;

use bytes::Bytes;

use crate::archive::Error;

/// Strip the query string from URLs, as it may carry an Azure SAS token.
pub fn redact(source: &str) -> &str {
    if !source.starts_with("http://") && !source.starts_with("https://") {
        return source;
    }
    match source.split_once('?') {
        Some((head, _)) => head,
        None => source,
    }
}

pub struct ArchiveSource {
    pub etag: Box<str>,
    pub last_modified: Box<str>,
    pub size: u64,
    /// Leading bytes the backend already read while fetching metadata, if any.
    pub prefetch: Option<Bytes>,
}

pub enum Storage {
    #[cfg(feature = "s3")]
    S3 {
        client: aws_sdk_s3::Client,
        bucket: Box<str>,
        key: Box<str>,
        etag: Box<str>,
    },
    #[cfg(feature = "azure")]
    AzureBlob {
        client: Box<azure_storage_blob::BlobClient>,
        etag: Box<str>,
    },
    Local {
        // `Arc<std::fs::File>` lets us call `read_at` (which takes `&self`) concurrently
        // from multiple `spawn_blocking` tasks without `try_clone()` syscalls per read.
        file: Arc<std::fs::File>,
    },
}

impl Storage {
    pub async fn open(source: &str) -> Result<(Self, ArchiveSource), Error> {
        #[cfg(feature = "azure")]
        if azure::is_azure_url(source) {
            return azure::open_azure(source).await;
        }

        #[cfg(not(feature = "azure"))]
        if source.starts_with("https://") || source.starts_with("http://") {
            return Err(Error::Protocol(
                "HTTP(S) archives require the 'azure' cargo feature".into(),
            ));
        }

        #[cfg(feature = "s3")]
        if let Some((bucket, key)) = parse_s3_url(source) {
            return open_s3(bucket, key).await;
        }

        #[cfg(not(feature = "s3"))]
        if source.starts_with("s3://") {
            return Err(Error::Protocol(
                "S3 archives require the 's3' cargo feature".into(),
            ));
        }

        open_local(source)
    }

    pub async fn read_range(&self, offset: u64, length: u64) -> Result<Bytes, Error> {
        if length == 0 {
            return Ok(Bytes::new());
        }

        let data = match self {
            #[cfg(feature = "s3")]
            Self::S3 {
                client,
                bucket,
                key,
                etag,
            } => read_s3_range(client, bucket, key, etag, offset, length).await,
            #[cfg(feature = "azure")]
            Self::AzureBlob { client, etag } => {
                azure::read_azure_range(client, etag, offset, length).await
            }
            Self::Local { file } => read_local_range(file.clone(), offset, length).await,
        }?;

        // A ranged GET may return less than asked, and callers rely on the exact length
        if data.len() as u64 != length {
            return Err(Error::Io(format!(
                "short read at offset={offset}: asked for {length} bytes, got {}",
                data.len()
            )));
        }
        Ok(data)
    }
}

/// ETag is synthesized from `mtime+size`, so it changes whenever the archive is replaced.
fn open_local(path: &str) -> Result<(Storage, ArchiveSource), Error> {
    let metadata = std::fs::metadata(path)
        .map_err(|e| Error::Io(format!("stat({}) failed: {e}", redact(path))))?;
    if !metadata.is_file() {
        return Err(Error::Protocol(format!(
            "{} is not a regular file",
            redact(path)
        )));
    }
    let size = metadata.len();
    let mtime = metadata
        .modified()
        .map_err(|e| Error::Io(format!("{} has no mtime: {e}", redact(path))))?;
    let mtime_unix = mtime
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| Error::Protocol("file mtime is before UNIX epoch".into()))?
        .as_secs();
    let etag: Box<str> = format!("\"{mtime_unix}-{size}\"").into();
    let last_modified: Box<str> = httpdate::fmt_http_date(mtime).into();
    let file = std::fs::File::open(path)
        .map_err(|e| Error::Io(format!("open({}) failed: {e}", redact(path))))?;

    Ok((
        Storage::Local {
            file: Arc::new(file),
        },
        ArchiveSource {
            etag,
            last_modified,
            size,
            prefetch: None,
        },
    ))
}

async fn read_local_range(
    file: Arc<std::fs::File>,
    offset: u64,
    length: u64,
) -> Result<Bytes, Error> {
    let len = length as usize;
    tokio::task::spawn_blocking(move || {
        use std::os::unix::fs::FileExt;
        let mut buf = vec![0u8; len];
        file.read_exact_at(&mut buf, offset)
            .map(|()| Bytes::from(buf))
    })
    .await
    .map_err(|e| Error::Io(format!("local read task panicked: {e}")))?
    .map_err(|e| {
        Error::Io(format!(
            "local read_at(offset={offset}, len={length}) failed: {e}"
        ))
    })
}

#[cfg(feature = "s3")]
fn parse_s3_url(url: &str) -> Option<(&str, &str)> {
    let path = url.strip_prefix("s3://")?;
    path.split_once('/')
}

#[cfg(feature = "s3")]
async fn open_s3(bucket: &str, key: &str) -> Result<(Storage, ArchiveSource), Error> {
    use aws_smithy_http_client::tls::{Provider, rustls_provider::CryptoMode};

    let http = aws_smithy_http_client::Builder::new()
        .tls_provider(Provider::Rustls(CryptoMode::Ring))
        .build_https();
    let aws = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .http_client(http)
        .load()
        .await;
    let client = aws_sdk_s3::Client::new(&aws);

    let head = client
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|e| Error::Io(format!("HeadObject failed: {e}")))?;

    let etag: Box<str> = head
        .e_tag()
        .ok_or_else(|| Error::Protocol("S3 HeadObject returned no ETag".into()))?
        .into();

    let last_modified: Box<str> = head
        .last_modified()
        .and_then(|dt| {
            dt.fmt(aws_sdk_s3::primitives::DateTimeFormat::HttpDate)
                .ok()
        })
        .ok_or_else(|| Error::Protocol("S3 HeadObject returned no Last-Modified".into()))?
        .into();

    let size = head
        .content_length()
        .ok_or_else(|| Error::Protocol("S3 HeadObject returned no Content-Length".into()))?
        as u64;

    Ok((
        Storage::S3 {
            client,
            bucket: bucket.into(),
            key: key.into(),
            etag: etag.clone(),
        },
        ArchiveSource {
            etag,
            last_modified,
            size,
            prefetch: None,
        },
    ))
}

#[cfg(feature = "s3")]
fn ranged_read(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    etag: &str,
    offset: u64,
    length: u64,
) -> aws_sdk_s3::operation::get_object::builders::GetObjectFluentBuilder {
    client
        .get_object()
        .bucket(bucket)
        .key(key)
        .range(format!("bytes={}-{}", offset, offset + length - 1))
        .if_match(etag)
}

/// Reads are pinned to the ETag the index was built from, so a replaced archive fails
/// with 412 instead of serving stale offsets as `immutable`.
#[cfg(feature = "s3")]
async fn read_s3_range(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    etag: &str,
    offset: u64,
    length: u64,
) -> Result<Bytes, Error> {
    let resp = ranged_read(client, bucket, key, etag, offset, length)
        .send()
        .await
        .map_err(|e| {
            Error::Io(format!(
                "S3 GetObject failed: {e} (a 412 here means the archive was replaced \
                 while rati was running; restart it)"
            ))
        })?;
    let data = resp
        .body
        .collect()
        .await
        .map_err(|e| Error::Io(format!("reading S3 response body: {e}")))?
        .into_bytes();
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_test() {
        assert_eq!(
            redact("https://acct.blob.core.windows.net/t/p.tar?sv=2024-11-04&sig=SECRET"),
            "https://acct.blob.core.windows.net/t/p.tar"
        );
        assert_eq!(
            redact("https://acct.blob.core.windows.net/t/p.tar"),
            "https://acct.blob.core.windows.net/t/p.tar"
        );
        assert_eq!(redact("https://h/p.tar?"), "https://h/p.tar");
        assert_eq!(redact("s3://bucket/planet.tar"), "s3://bucket/planet.tar");
        assert_eq!(redact("/data/planet.tar"), "/data/planet.tar");
        assert_eq!(redact("/data/tiles?v2.tar"), "/data/tiles?v2.tar");
    }

    #[tokio::test]
    async fn read_range_test() {
        let dir = std::env::temp_dir().join(format!("rati-short-read-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("tiny.tar");
        std::fs::write(&path, b"0123456789").unwrap();

        let (storage, _) = Storage::open(path.to_str().unwrap()).await.unwrap();

        assert_eq!(storage.read_range(0, 4).await.unwrap().as_ref(), b"0123");
        assert!(storage.read_range(4, 0).await.unwrap().is_empty());
        assert!(storage.read_range(6, 8).await.is_err());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(feature = "s3")]
    #[test]
    fn ranged_read_test() {
        let client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version_latest()
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .credentials_provider(aws_sdk_s3::config::Credentials::new(
                    "id", "secret", None, None, "test",
                ))
                .build(),
        );

        let req = ranged_read(
            &client,
            "bucket",
            "planet.tar",
            "\"abc123\"",
            1536,
            44_504_000,
        );
        assert_eq!(req.get_if_match().as_deref(), Some("\"abc123\""));
        assert_eq!(req.get_range().as_deref(), Some("bytes=1536-44505535"));
    }

    #[cfg(feature = "s3")]
    #[test]
    fn parse_s3_url_test() {
        assert_eq!(
            parse_s3_url("s3://my-bucket/path/to/file.tar"),
            Some(("my-bucket", "path/to/file.tar"))
        );
        assert_eq!(
            parse_s3_url("s3://bucket/file.tar"),
            Some(("bucket", "file.tar"))
        );

        assert_eq!(parse_s3_url("bucket/key"), None);
        assert_eq!(parse_s3_url("https://wrong/scheme"), None);
        assert_eq!(parse_s3_url("s3:/bad-url/format"), None);
        assert_eq!(parse_s3_url("s3://bucket-only"), None);
        assert_eq!(parse_s3_url("s3://file-only.tar"), None);
    }
}
