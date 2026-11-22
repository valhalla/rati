#[cfg(feature = "azure")]
mod azure;
#[cfg(feature = "s3")]
mod s3;

use std::sync::Arc;

use bytes::Bytes;

use crate::archive::Error;

pub struct Storage {
    pub etag: Box<str>,
    pub last_modified: Box<str>,
    pub size: u64,
    backend: Backend,
}

enum Backend {
    #[cfg(feature = "s3")]
    S3 {
        client: aws_sdk_s3::Client,
        bucket: Box<str>,
        key: Box<str>,
    },
    #[cfg(feature = "azure")]
    Azure(Box<azure_storage_blob::BlobClient>),
    // `Arc<std::fs::File>` lets us call `read_at` (which takes `&self`) concurrently
    // from multiple `spawn_blocking` tasks without `try_clone()` syscalls per read.
    Local(Arc<std::fs::File>),
}

impl Storage {
    pub async fn open(source: &str) -> Result<Self, Error> {
        #[cfg(feature = "azure")]
        if is_http(source) {
            return azure::open(source).await;
        }

        #[cfg(not(feature = "azure"))]
        if is_http(source) {
            return Err(Error::Protocol(
                "HTTP(S) archives require the 'azure' cargo feature".into(),
            ));
        }

        #[cfg(feature = "s3")]
        if let Some((bucket, key)) = parse_s3_url(source) {
            return s3::open(bucket, key).await;
        }

        #[cfg(not(feature = "s3"))]
        if parse_s3_url(source).is_some() {
            return Err(Error::Protocol(
                "S3 archives require the 's3' cargo feature".into(),
            ));
        }

        open_local(source)
    }

    /// Reads are pinned to the ETag the index was built from, so a replaced archive fails
    /// with 412 instead of serving stale offsets as `immutable`.
    pub async fn read_range(&self, offset: u64, length: u64) -> Result<Bytes, Error> {
        if length == 0 {
            return Ok(Bytes::new());
        }

        let data = match &self.backend {
            #[cfg(feature = "s3")]
            Backend::S3 {
                client,
                bucket,
                key,
            } => s3::read_range(client, bucket, key, &self.etag, offset, length).await,
            #[cfg(feature = "azure")]
            Backend::Azure(client) => azure::read_range(client, &self.etag, offset, length).await,
            Backend::Local(file) => read_local_range(file.clone(), offset, length).await,
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

/// Strip the query string from URLs, as it may carry an Azure SAS token.
pub fn redact(source: &str) -> &str {
    match source.split_once('?') {
        Some((head, _)) if is_http(source) => head,
        _ => source,
    }
}

fn is_http(source: &str) -> bool {
    source.starts_with("https://") || source.starts_with("http://")
}

fn parse_s3_url(url: &str) -> Option<(&str, &str)> {
    let path = url.strip_prefix("s3://")?;
    path.split_once('/')
}

/// ETag is synthesized from `mtime+size`, so it changes whenever the archive is replaced.
fn open_local(path: &str) -> Result<Storage, Error> {
    let metadata =
        std::fs::metadata(path).map_err(|e| Error::Io(format!("stat({path}) failed: {e}")))?;
    if !metadata.is_file() {
        return Err(Error::Protocol(format!("{path} is not a regular file")));
    }
    let size = metadata.len();
    let mtime = metadata
        .modified()
        .map_err(|e| Error::Io(format!("{path} has no mtime: {e}")))?;
    let mtime_unix = mtime
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| Error::Protocol("file mtime is before UNIX epoch".into()))?
        .as_secs();
    let file =
        std::fs::File::open(path).map_err(|e| Error::Io(format!("open({path}) failed: {e}")))?;

    Ok(Storage {
        etag: format!("\"{mtime_unix}-{size}\"").into(),
        last_modified: httpdate::fmt_http_date(mtime).into(),
        size,
        backend: Backend::Local(Arc::new(file)),
    })
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

        let storage = Storage::open(path.to_str().unwrap()).await.unwrap();

        assert_eq!(storage.read_range(0, 4).await.unwrap().as_ref(), b"0123");
        assert!(storage.read_range(4, 0).await.unwrap().is_empty());
        assert!(storage.read_range(6, 8).await.is_err());

        std::fs::remove_dir_all(&dir).ok();
    }

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
