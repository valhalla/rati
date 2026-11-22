use std::fs::File;
use std::os::unix::fs::FileExt;
use std::sync::Arc;

use bytes::Bytes;

use super::{Backend, Error, Storage};

/// ETag is synthesized from `mtime+size`, so it changes whenever the archive is replaced.
pub(super) fn open(path: &str) -> Result<Storage, Error> {
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
    let file = File::open(path).map_err(|e| Error::Io(format!("open({path}) failed: {e}")))?;

    Ok(Storage {
        etag: format!("\"{mtime_unix}-{size}\"").into(),
        last_modified: httpdate::fmt_http_date(mtime).into(),
        size,
        backend: Backend::Local(Arc::new(file)),
    })
}

pub(super) async fn read_range(file: Arc<File>, offset: u64, length: u64) -> Result<Bytes, Error> {
    tokio::task::spawn_blocking(move || {
        let mut buf = vec![0u8; length as usize];
        file.read_exact_at(&mut buf, offset)
            .map(|()| Bytes::from(buf))
    })
    .await
    .expect("local read task panicked")
    .map_err(|e| {
        Error::Io(format!(
            "local read_at(offset={offset}, len={length}) failed: {e}"
        ))
    })
}
