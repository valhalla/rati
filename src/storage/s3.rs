use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::DateTimeFormat;
use aws_smithy_http_client::tls::{Provider, rustls_provider::CryptoMode};
use bytes::Bytes;

use super::{Backend, Error, Storage};

pub(super) async fn open(url: &str) -> Result<Storage, Error> {
    let (bucket, key) = parse_url(url).ok_or_else(|| {
        Error::Protocol(format!("invalid S3 URL {url}, expected s3://bucket/key"))
    })?;

    let http = aws_smithy_http_client::Builder::new()
        .tls_provider(Provider::Rustls(CryptoMode::Ring))
        .build_https();
    let aws = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .http_client(http)
        .load()
        .await;
    let client = Client::new(&aws);

    let head = client
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|e| Error::Io(format!("HeadObject failed: {e}")))?;

    let etag = head
        .e_tag()
        .ok_or_else(|| Error::Protocol("S3 HeadObject returned no ETag".into()))?;
    let last_modified = head
        .last_modified()
        .and_then(|dt| dt.fmt(DateTimeFormat::HttpDate).ok())
        .ok_or_else(|| Error::Protocol("S3 HeadObject returned no Last-Modified".into()))?;
    let size = head
        .content_length()
        .ok_or_else(|| Error::Protocol("S3 HeadObject returned no Content-Length".into()))?;

    Ok(Storage {
        etag: etag.into(),
        last_modified: last_modified.into(),
        size: size as u64,
        backend: Backend::S3 {
            client,
            bucket: bucket.into(),
            key: key.into(),
        },
    })
}

pub(super) async fn read_range(
    client: &Client,
    bucket: &str,
    key: &str,
    etag: &str,
    offset: u64,
    length: u64,
) -> Result<Bytes, Error> {
    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .range(format!("bytes={}-{}", offset, offset + length - 1))
        .if_match(etag)
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

fn parse_url(url: &str) -> Option<(&str, &str)> {
    let path = url.strip_prefix("s3://")?;
    path.split_once('/')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_url_test() {
        assert_eq!(
            parse_url("s3://my-bucket/path/to/file.tar"),
            Some(("my-bucket", "path/to/file.tar"))
        );
        assert_eq!(
            parse_url("s3://bucket/file.tar"),
            Some(("bucket", "file.tar"))
        );

        assert_eq!(parse_url("bucket/key"), None);
        assert_eq!(parse_url("https://wrong/scheme"), None);
        assert_eq!(parse_url("s3:/bad-url/format"), None);
        assert_eq!(parse_url("s3://bucket-only"), None);
        assert_eq!(parse_url("s3://file-only.tar"), None);
    }
}
