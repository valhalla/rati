//! S3-backed tar archive: index parsing, tile lookups, and S3 I/O.
//!
//! Loads the tar index from an S3 object via range requests, then serves individual
//! tiles by reading their byte ranges on demand. Startup performs a handful of small
//! range reads against S3 — no full download:
//!
//! 1. `HeadObject` for ETag, Last-Modified, and total size.
//! 2. First 512 bytes — the leading tar header, expected to name `index.bin`. If it
//!    doesn't and `--scan-index` is set, fall back to [`scan_tar_headers`] which walks
//!    the whole archive in 8 MB chunks with prefetch.
//! 3. The `index.bin` payload — parsed into a `TileId → (offset, size)` map.
//! 4. One more 512-byte header read to detect tile compression from the first tile's
//!    filename suffix ([`detect_compression`]).
//! 5. A 272-byte read (or the full first tile when it's compressed) to extract the
//!    dataset id from the `GraphTileHeader` ([`detect_dataset_id`]).

use bytes::Bytes;
use rustc_hash::FxHashMap;

/// Size of a POSIX tar header block.
const TAR_BLOCK_SIZE: usize = 512;
/// Size of a single index entry in bytes: u64 + u32 + u32.
const TILE_INDEX_ENTRY_SIZE: usize = 16;
/// Name of the index file that must be the first entry in the tar.
const TILE_INDEX_FILE_NAME: &str = "index.bin";

/// Byte offset of `dataset_id_` (`u64`) within Valhalla's `GraphTileHeader`.
///
/// Layout (272-byte POD struct, see `graphtileheader.h`):
///   - bytes  0..8:   bitfield (graphid_, density_, name_quality_, speed_quality_, exit_quality_, has_elevation_, has_ext_directededge_)
///   - bytes  8..16:  base_ll_ (std::array<float, 2>)
///   - bytes 16..32:  version_ (std::array<char, 16>)
///   - bytes 32..40:  dataset_id_ (uint64_t)
const GRAPH_TILE_HEADER_SIZE: usize = 272;
const DATASET_ID_OFFSET: usize = 32;

/// Valhalla tile id that encodes `level | (tile_index << 3)`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TileId(u32);

impl TileId {
    pub fn new(raw: u32) -> Self {
        Self(raw)
    }

    /// Parse a Valhalla tile path like `2/000/818/660.gph` into a packed ID.
    ///
    /// Mirrors `get_tile_id()` from `valhalla_build_extract`: split off level, drop any
    /// suffix from the tail, join the remaining digits → `level | (index << 3)`. Numeric
    /// tile names don't contain dots, so the first dot anywhere after the level is the
    /// start of the suffix(es) (`.gph`, `.gph.zst`, etc.).
    pub fn from_path(path: &str) -> Option<Self> {
        let (level_str, rest) = path.split_once('/')?;
        let level: u32 = level_str.parse().ok()?;
        let digits = rest.split_once('.').map_or(rest, |(head, _)| head);
        let tile_index: u32 = digits.replace('/', "").parse().ok()?;
        Some(Self(level | (tile_index << 3)))
    }
}

/// Compression applied to each tile entry inside the tar. Uniform across an archive — detected
/// once at startup from the first tile's filename suffix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TileCompression {
    None,
    Gzip,
    Zstd,
}

impl TileCompression {
    fn from_name(name: &str) -> Self {
        if name.ends_with(".zst") {
            Self::Zstd
        } else if name.ends_with(".gz") {
            Self::Gzip
        } else {
            Self::None
        }
    }

    /// IANA token used on the wire in `Accept-Encoding` / `Content-Encoding`.
    /// [`Self::None`] returns the conventional `"identity"`; callers emitting
    /// `Content-Encoding` should suppress the header in that case rather than
    /// sending `identity` (which is redundant).
    pub const fn header_name(self) -> &'static str {
        match self {
            Self::None => "identity",
            Self::Gzip => "gzip",
            Self::Zstd => "zstd",
        }
    }
}

impl std::fmt::Display for TileCompression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Use a friendlier label for `None` in logs; the HTTP token is "identity"
        // (see [`Self::header_name`]) but reads oddly outside an HTTP context.
        f.write_str(match self {
            Self::None => "none",
            other => other.header_name(),
        })
    }
}

impl std::fmt::Display for TileId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Matches Valhalla's `tile_index_entry` layout (16 bytes LE: offset u64, tile_id u32, size u32).
struct TileIndexEntry {
    tile_id: TileId,
    /// Byte offset from the start of the tar archive.
    offset: u64,
    size: u32,
}

impl TileIndexEntry {
    /// Parse a single entry from a 16-byte little-endian slice.
    /// Matches the https://github.com/valhalla/valhalla/blob/master/scripts/valhalla_build_extract
    fn from_bytes(data: &[u8; TILE_INDEX_ENTRY_SIZE]) -> Self {
        Self {
            offset: u64::from_le_bytes(data[0..8].try_into().unwrap()),
            tile_id: TileId(u32::from_le_bytes(data[8..12].try_into().unwrap())),
            size: u32::from_le_bytes(data[12..16].try_into().unwrap()),
        }
    }
}

#[derive(Debug)]
struct TileEntry {
    offset: u64,
    size: u32,
}

type TileIndex = FxHashMap<TileId, TileEntry>;

/// Parse index from raw `index.bin` bytes.
fn parse_index(data: &[u8]) -> Result<TileIndex, TarError> {
    if !data.len().is_multiple_of(TILE_INDEX_ENTRY_SIZE) {
        return Err(TarError::InvalidIndexSize {
            size: data.len(),
            entry_size: TILE_INDEX_ENTRY_SIZE,
        });
    }

    let count = data.len() / TILE_INDEX_ENTRY_SIZE;
    let mut entries: TileIndex = FxHashMap::with_capacity_and_hasher(count, Default::default());

    for chunk in data.chunks_exact(TILE_INDEX_ENTRY_SIZE) {
        let e = TileIndexEntry::from_bytes(chunk.try_into().unwrap());
        entries.insert(
            e.tile_id,
            TileEntry {
                offset: e.offset,
                size: e.size,
            },
        );
    }

    if entries.is_empty() {
        return Err(TarError::EmptyIndex);
    }

    Ok(entries)
}

/// Parse the first tar header from raw bytes and extract the index.bin file content range.
///
/// Returns `(data_offset, data_size)` — the byte range within the archive where `index.bin`
/// content lives. The caller should read `archive[data_offset..data_offset + data_size]` to
/// get the raw index data, then pass it to [`parse_index`].
fn read_index_header(header_bytes: &[u8; TAR_BLOCK_SIZE]) -> Result<(u64, u64), TarError> {
    let header = TarHeader::parse(header_bytes)?;

    // The first entry must be index.bin — graphreader.cc:147 enforces this
    if header.name != TILE_INDEX_FILE_NAME {
        return Err(TarError::FirstEntryNotIndex {
            actual: header.name,
        });
    }

    let data_offset = TAR_BLOCK_SIZE as u64; // data starts right after the 512-byte header
    let data_size = header.size;

    Ok((data_offset, data_size))
}

/// Minimal POSIX tar header parser.
///
/// Only extracts fields we need: `name` and `size`. Verifies the header checksum
/// following the same algorithm as `tar::header_t::verify()` in `sequence.h:638-651`.
struct TarHeader {
    name: String,
    size: u64,
}

impl TarHeader {
    fn parse(raw: &[u8; TAR_BLOCK_SIZE]) -> Result<Self, TarError> {
        // Verify checksum (sequence.h:638-651)
        // The checksum is computed over the entire header with the chksum field treated as spaces
        let stored_checksum = octal_to_u64(&raw[148..156]);
        let mut unsigned_sum = 0u64;
        let mut signed_sum = 0i64;
        for (i, &byte) in raw.iter().enumerate() {
            let b = if (148..156).contains(&i) {
                b' ' // treat chksum field as spaces
            } else {
                byte
            };
            unsigned_sum += b as u64;
            signed_sum += (b as i8) as i64;
        }
        if stored_checksum != unsigned_sum && stored_checksum as i64 != signed_sum {
            return Err(TarError::InvalidHeader("checksum mismatch".into()));
        }

        // Extract null-terminated name (first 100 bytes)
        let name_end = raw[..100].iter().position(|&b| b == 0).unwrap_or(100);
        let name = std::str::from_utf8(&raw[..name_end])
            .map_err(|_| TarError::InvalidHeader("name is not valid UTF-8".into()))?
            .to_string();

        // Extract size (octal ASCII in bytes 124-136)
        let size = octal_to_u64(&raw[124..136]);

        Ok(Self { name, size })
    }
}

/// Parse an octal ASCII field from a tar header, handling trailing NULs and spaces.
///
/// Mirrors `tar::header_t::octal_to_int()` in `sequence.h:610-627`.
fn octal_to_u64(field: &[u8]) -> u64 {
    // Find the end of meaningful content (skip trailing NULs and spaces)
    let end = field
        .iter()
        .rposition(|&b| b != 0 && b != b' ')
        .map(|i| i + 1)
        .unwrap_or(0);

    // Parse octal digits
    let mut result = 0u64;
    for &byte in &field[..end] {
        if (b'0'..=b'7').contains(&byte) {
            result = result * 8 + (byte - b'0') as u64;
        }
        // Skip non-octal chars (spaces, NULs can appear before digits in some tars)
    }
    result
}

/// Metadata consumed once at startup: logging, tile headers, status endpoint.
pub struct ArchiveMeta {
    /// S3 object ETag (includes quotes, e.g. `"abc123"`).
    pub etag: Box<str>,
    /// S3 object Last-Modified as an HTTP-date string.
    pub last_modified: Box<str>,
    /// Dataset identifier: extracted from graph tile header, CLI override, or S3 ETag fallback.
    pub dataset_id: Box<str>,
    /// Number of tiles in the index.
    pub tile_count: usize,
}

pub struct S3Archive {
    client: aws_sdk_s3::Client,
    bucket: Box<str>,
    key: Box<str>,
    index: TileIndex,
    compression: TileCompression,
}

impl S3Archive {
    /// Connect to S3 and load the tar index.
    ///
    /// `url` must be in the form `s3://bucket/path/to/key`.
    /// Uses the default AWS credential chain (SSO, IRSA, env vars, IMDS).
    ///
    /// If the first tar entry is not `index.bin` and `scan_index` is true, falls back
    /// to scanning tar headers via range requests to build the index.
    /// N.B.: That might be really slow for large archives as tar has no central directory.
    ///
    /// `dataset_id_override` — if provided, used as the dataset ID instead of auto-detection.
    ///
    /// Returns the archive (for tile fetches) and metadata (consumed once at startup).
    pub async fn open(
        url: &str,
        scan_index: bool,
        dataset_id_override: Option<&str>,
    ) -> Result<(Self, ArchiveMeta), S3Error> {
        let (bucket, key) = parse_s3_url(url)
            .ok_or_else(|| S3Error::Protocol(format!("expected s3:// URL, got: {url}")))?;
        let client = aws_sdk_s3::Client::new(
            &aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await,
        );

        // Fetch S3 object metadata (ETag, Last-Modified) via HeadObject
        let head = client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| S3Error::Request(format!("HeadObject failed: {e}")))?;

        let etag: Box<str> = head
            .e_tag()
            .ok_or_else(|| S3Error::Protocol("S3 HeadObject returned no ETag".into()))?
            .into();

        let last_modified: Box<str> = head
            .last_modified()
            .and_then(|dt| {
                dt.fmt(aws_sdk_s3::primitives::DateTimeFormat::HttpDate)
                    .ok()
            })
            .ok_or_else(|| S3Error::Protocol("S3 HeadObject returned no Last-Modified".into()))?
            .into();

        let archive_size = head
            .content_length()
            .ok_or_else(|| S3Error::Protocol("S3 HeadObject returned no Content-Length".into()))?
            as u64;

        // Step 1: Read the first 512-byte tar header
        let header_bytes = get_range(&client, bucket, key, 0, 512).await?;
        let header: &[u8; 512] = header_bytes
            .as_ref()
            .try_into()
            .map_err(|_| S3Error::Protocol("tar header shorter than 512 bytes".into()))?;

        // Step 2: Try to load index.bin; fall back to tar scan if missing and --scan-index is set.
        // The scan path sees filenames directly and detects compression inline; the index.bin
        // path requires an extra 512-byte range request afterwards to read the first tile's
        // tar header.
        let (index, compression) = match read_index_header(header) {
            Ok((data_offset, data_size)) => {
                let index_bytes = get_range(&client, bucket, key, data_offset, data_size).await?;
                let index = parse_index(&index_bytes).map_err(S3Error::Tar)?;
                let compression = detect_compression(&client, bucket, key, &index).await?;
                (index, compression)
            }
            Err(TarError::FirstEntryNotIndex { .. }) if scan_index => {
                tracing::warn!(
                    "index.bin not found in archive; scanning tar headers to build index \
                     via range requests."
                );
                scan_tar_headers(&client, bucket, key, archive_size).await?
            }
            Err(TarError::FirstEntryNotIndex { .. }) => {
                return Err(S3Error::Tar(TarError::MissingIndexNoScan));
            }
            Err(e) => return Err(S3Error::Tar(e)),
        };

        tracing::info!("Tile compression: {compression}");

        // Step 3: Determine the dataset ID
        let dataset_id: Box<str> = if let Some(override_id) = dataset_id_override {
            tracing::info!("Using CLI-provided dataset ID: {override_id}");
            override_id.into()
        } else {
            // Try to auto-detect from the first tile's GraphTileHeader
            match detect_dataset_id(&client, bucket, key, &index, compression).await {
                Ok(id) => {
                    tracing::info!("Auto-detected dataset ID from graph tile header: {id}");
                    id.to_string().into()
                }
                Err(e) => {
                    // Fall back to S3 ETag
                    tracing::warn!(
                        "Could not detect dataset ID from tile header ({e}); \
                         falling back to S3 ETag: {etag}"
                    );
                    etag.clone()
                }
            }
        };

        let tile_count = index.len();

        let archive = Self {
            client,
            bucket: bucket.into(),
            key: key.into(),
            index,
            compression,
        };

        let meta = ArchiveMeta {
            etag,
            last_modified,
            dataset_id,
            tile_count,
        };

        Ok((archive, meta))
    }

    /// Fetch a tile's on-disk bytes — exactly what's stored in the tar, compressed or not.
    /// Callers inspect [`compression`](Self::compression) to decide whether to decode.
    /// Returns `None` if the tile is not in the index.
    pub async fn get_tile(&self, tile_id: TileId) -> Result<Option<Bytes>, S3Error> {
        let Some(entry) = self.index.get(&tile_id) else {
            return Ok(None);
        };
        let raw = get_range(
            &self.client,
            &self.bucket,
            &self.key,
            entry.offset,
            entry.size as u64,
        )
        .await?;
        Ok(Some(raw))
    }

    /// Returns the on-disk tile size from the index, or `None` if the tile doesn't exist.
    /// For compressed archives this is the compressed size, not the size the client receives.
    pub fn tile_size(&self, tile_id: TileId) -> Option<u32> {
        self.index.get(&tile_id).map(|e| e.size)
    }

    /// Compression scheme used for tiles inside this archive.
    pub fn compression(&self) -> TileCompression {
        self.compression
    }
}

/// Decompress `data` according to `compression`. Returns the input unchanged for `TileCompression::None`.
pub fn decompress(data: Bytes, compression: TileCompression) -> std::io::Result<Bytes> {
    use std::io::Read;
    match compression {
        TileCompression::None => Ok(data),
        TileCompression::Gzip => {
            let mut decoder = flate2::read::GzDecoder::new(data.as_ref());
            let mut out = Vec::with_capacity(data.len() * 3);
            decoder.read_to_end(&mut out)?;
            Ok(Bytes::from(out))
        }
        TileCompression::Zstd => {
            let mut decoder = zstd::stream::Decoder::new(data.as_ref())?;
            let mut out = Vec::with_capacity(data.len() * 4);
            decoder.read_to_end(&mut out)?;
            Ok(Bytes::from(out))
        }
    }
}

async fn get_range(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    offset: u64,
    length: u64,
) -> Result<Bytes, S3Error> {
    if length == 0 {
        return Ok(Bytes::new());
    }
    let range = format!("bytes={}-{}", offset, offset + length - 1);
    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .range(&range)
        .send()
        .await
        .map_err(|e| S3Error::Request(format!("{e}")))?;

    let data = resp
        .body
        .collect()
        .await
        .map_err(|e| S3Error::Request(format!("reading response body: {e}")))?
        .into_bytes();

    Ok(data)
}

/// Scan tar headers via S3 range requests to build the tile index.
///
/// Reads the archive in fixed-offset 8 MB chunks, parsing only the 512-byte tar
/// headers and skipping over tile data. Up to `PREFETCH` chunks are in-flight
/// concurrently (via [`futures_util::StreamExt::buffered`]), so the next chunk is
/// typically ready by the time the scanner finishes the current one.
async fn scan_tar_headers(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    archive_size: u64,
) -> Result<(TileIndex, TileCompression), S3Error> {
    use futures_util::{StreamExt, stream};

    // Full planet tileset might have up to 205k tiles and occupy ~80GB, so making
    // ~10k chunk requests (with prefetch) is much faster than 205k sequential header reads.
    const CHUNK_SIZE: u64 = 8 * 1024 * 1024;
    const PREFETCH: usize = 8;

    let num_chunks = archive_size.div_ceil(CHUNK_SIZE);
    let mut chunks = stream::iter(0..num_chunks)
        .map(|i| {
            let offset = i * CHUNK_SIZE;
            let len = CHUNK_SIZE.min(archive_size - offset);
            get_range(client, bucket, key, offset, len)
        })
        .buffered(PREFETCH);

    let mut entries = FxHashMap::default();
    let mut compression = TileCompression::None;
    let mut pos: u64 = 0;

    // Current chunk and its position within the archive
    let mut chunk = Bytes::new();
    let mut chunk_start: u64 = 0;
    let mut next_chunk_idx: u64 = 0;

    while pos + TAR_BLOCK_SIZE as u64 <= archive_size {
        // Advance stream to the chunk containing `pos`, discarding any skipped chunks
        let needed_chunk = pos / CHUNK_SIZE;
        while next_chunk_idx <= needed_chunk {
            chunk = chunks
                .next()
                .await
                .ok_or_else(|| S3Error::Protocol("unexpected end of chunk stream".into()))??;
            chunk_start = next_chunk_idx * CHUNK_SIZE;
            next_chunk_idx += 1;
        }

        let local = (pos - chunk_start) as usize;
        let chunk_remaining = chunk.len() - local;

        // Read header, stitching across chunk boundary if necessary
        let header_buf;
        let header_array: &[u8; TAR_BLOCK_SIZE] = if chunk_remaining >= TAR_BLOCK_SIZE {
            (&chunk[local..local + TAR_BLOCK_SIZE]).try_into().unwrap()
        } else {
            header_buf = {
                let mut buf = [0u8; TAR_BLOCK_SIZE];
                buf[..chunk_remaining].copy_from_slice(&chunk[local..]);
                chunk = chunks
                    .next()
                    .await
                    .ok_or_else(|| S3Error::Protocol("unexpected end of chunk stream".into()))??;
                chunk_start = next_chunk_idx * CHUNK_SIZE;
                next_chunk_idx += 1;
                buf[chunk_remaining..].copy_from_slice(&chunk[..TAR_BLOCK_SIZE - chunk_remaining]);
                buf
            };
            &header_buf
        };

        // End-of-archive marker (zero block)
        if header_array.iter().all(|&b| b == 0) {
            break;
        }

        let header = TarHeader::parse(header_array).map_err(S3Error::Tar)?;

        let data_offset = pos + TAR_BLOCK_SIZE as u64;
        let data_size = header.size;

        if let Some(tile_id) = TileId::from_path(&header.name) {
            if data_size > u32::MAX as u64 {
                tracing::warn!(
                    "Skipping tile {} ({data_size} bytes): exceeds u32::MAX size limit",
                    header.name,
                );
            } else {
                if entries.is_empty() {
                    compression = TileCompression::from_name(&header.name);
                }
                entries.insert(
                    tile_id,
                    TileEntry {
                        offset: data_offset,
                        size: data_size as u32,
                    },
                );
                if entries.len() % 1000 == 0 {
                    tracing::info!("Scanned {} tiles so far...", entries.len());
                }
            }
        }

        // Advance past header + data (padded to 512-byte boundary)
        let data_blocks = data_size.div_ceil(TAR_BLOCK_SIZE as u64);
        pos = data_offset + data_blocks * TAR_BLOCK_SIZE as u64;
    }

    if entries.is_empty() {
        return Err(S3Error::Tar(TarError::EmptyIndex));
    }
    entries.shrink_to_fit();

    tracing::info!("Built index from tar scan: {} tiles", entries.len());
    Ok((entries, compression))
}

/// Read the tar header of the first tile listed in `index` and detect compression from its name.
///
/// Index entries store only `tile_id`, not filenames, so we need one additional 512-byte range
/// request to recover the name. The tar header sits 512 bytes before the tile data.
async fn detect_compression(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    index: &TileIndex,
) -> Result<TileCompression, S3Error> {
    let Some(entry) = index.values().next() else {
        return Ok(TileCompression::None); // no tiles - no compression
    };
    let header_bytes = get_range(
        client,
        bucket,
        key,
        entry.offset - TAR_BLOCK_SIZE as u64,
        TAR_BLOCK_SIZE as u64,
    )
    .await?;
    let header: &[u8; TAR_BLOCK_SIZE] = header_bytes
        .as_ref()
        .try_into()
        .map_err(|_| S3Error::Protocol("tile header shorter than 512 bytes".into()))?;
    let parsed = TarHeader::parse(header).map_err(S3Error::Tar)?;
    Ok(TileCompression::from_name(&parsed.name))
}

/// Try to detect the dataset ID by reading the first tile's `GraphTileHeader`.
///
/// Picks an arbitrary tile from the index, reads its first 272 bytes (the header),
/// and extracts the `dataset_id` field (`u64` at byte offset 32). For compressed
/// archives the whole tile is fetched and decompressed first.
///
/// Returns an error if no tiles are in the index, the tile is too small, or
/// the `dataset_id` field is zero (likely not a graph tile).
async fn detect_dataset_id(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    index: &TileIndex,
    compression: TileCompression,
) -> Result<u64, S3Error> {
    let entry = index
        .values()
        .next()
        .ok_or_else(|| S3Error::Protocol("no tiles in index to read header from".into()))?;

    // For plain tiles we only need the first 272 bytes; for compressed tiles we have
    // to fetch and decompress the whole thing to read any prefix of the plain data.
    let data = match compression {
        TileCompression::None => {
            let read_size = GRAPH_TILE_HEADER_SIZE as u64;
            if (entry.size as u64) < read_size {
                return Err(S3Error::Protocol(format!(
                    "tile is only {} bytes, too small for GraphTileHeader ({} bytes)",
                    entry.size, GRAPH_TILE_HEADER_SIZE
                )));
            }
            get_range(client, bucket, key, entry.offset, read_size).await?
        }
        _ => {
            let raw = get_range(client, bucket, key, entry.offset, entry.size as u64).await?;
            decompress(raw, compression)
                .map_err(|e| S3Error::Protocol(format!("decode tile for dataset_id: {e}")))?
        }
    };
    let dataset_id = parse_dataset_id(&data)?;

    if dataset_id == 0 {
        return Err(S3Error::Protocol(
            "dataset_id is 0; tile may not be a graph tile".into(),
        ));
    }

    Ok(dataset_id)
}

/// Extract `dataset_id` from a raw `GraphTileHeader` byte slice.
///
/// The `dataset_id_` field is a little-endian `u64` at byte offset 32 within the
/// 272-byte header.
fn parse_dataset_id(header: &[u8]) -> Result<u64, S3Error> {
    if header.len() < DATASET_ID_OFFSET + 8 {
        return Err(S3Error::Protocol(format!(
            "header too short for dataset_id: {} bytes (need at least {})",
            header.len(),
            DATASET_ID_OFFSET + 8
        )));
    }
    let bytes: [u8; 8] = header[DATASET_ID_OFFSET..DATASET_ID_OFFSET + 8]
        .try_into()
        .unwrap();
    Ok(u64::from_le_bytes(bytes))
}

fn parse_s3_url(url: &str) -> Option<(&str, &str)> {
    let path = url.strip_prefix("s3://")?;
    path.split_once('/')
}

#[derive(Debug, thiserror::Error)]
pub enum TarError {
    #[error("index.bin size {size} is not a multiple of entry size {entry_size}")]
    InvalidIndexSize { size: usize, entry_size: usize },

    #[error("index.bin is empty")]
    EmptyIndex,

    #[error("first tar entry must be 'index.bin', got '{actual}'")]
    FirstEntryNotIndex { actual: String },

    #[error("archive has no index.bin; re-run with --scan-index to build index from tar headers")]
    MissingIndexNoScan,

    #[error("invalid tar header: {0}")]
    InvalidHeader(String),
}

#[derive(Debug, thiserror::Error)]
pub enum S3Error {
    #[error("S3 request failed: {0}")]
    Request(String),

    #[error("{0}")]
    Protocol(String),

    #[error(transparent)]
    Tar(TarError),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal tar header for a file with the given name and size.
    fn make_tar_header(name: &str, size: u64) -> [u8; TAR_BLOCK_SIZE] {
        let mut header = [0u8; TAR_BLOCK_SIZE];

        // name (bytes 0-99)
        header[..name.len()].copy_from_slice(name.as_bytes());

        // size as octal ASCII (bytes 124-135), null-terminated
        let size_str = format!("{:011o}\0", size);
        header[124..136].copy_from_slice(size_str.as_bytes());

        // typeflag = '0' (regular file) at byte 156
        header[156] = b'0';

        // magic = "ustar\0" at bytes 257-262
        header[257..263].copy_from_slice(b"ustar\0");

        // version = "00" at bytes 263-264
        header[263..265].copy_from_slice(b"00");

        // Compute checksum: treat chksum field (148-155) as spaces
        let mut sum = 0u64;
        for (i, &byte) in header.iter().enumerate() {
            if (148..156).contains(&i) {
                sum += b' ' as u64;
            } else {
                sum += byte as u64;
            }
        }
        let chksum_str = format!("{:06o}\0 ", sum);
        header[148..156].copy_from_slice(chksum_str.as_bytes());

        header
    }

    /// Build index.bin content from a slice of (offset, tile_id, size) tuples.
    fn make_index_bin(entries: &[(u64, u32, u32)]) -> Vec<u8> {
        let mut data = Vec::with_capacity(entries.len() * TILE_INDEX_ENTRY_SIZE);
        for &(offset, tile_id, size) in entries {
            data.extend_from_slice(&offset.to_le_bytes());
            data.extend_from_slice(&tile_id.to_le_bytes());
            data.extend_from_slice(&size.to_le_bytes());
        }
        data
    }

    #[test]
    fn from_path_gph_test() {
        let id = TileId::from_path("2/000/818/660.gph").unwrap();
        assert_eq!(id.0, 2 | (818660 << 3));

        let id = TileId::from_path("2/000/818/660.csv").unwrap();
        assert_eq!(id.0, 2 | (818660 << 3));

        let id = TileId::from_path("2/000/818/660.spd").unwrap();
        assert_eq!(id.0, 2 | (818660 << 3));

        let id = TileId::from_path("2/000/818/660").unwrap();
        assert_eq!(id.0, 2 | (818660 << 3));

        let id = TileId::from_path("0/000/529.gph").unwrap();
        assert_eq!(id.0, 529 << 3);

        let id = TileId::from_path("0/000/529").unwrap();
        assert_eq!(id.0, 529 << 3);

        // compression suffix on top of an extension
        let id = TileId::from_path("2/000/818/660.gph.zst").unwrap();
        assert_eq!(id.0, 2 | (818660 << 3));

        let id = TileId::from_path("2/000/818/660.gph.gz").unwrap();
        assert_eq!(id.0, 2 | (818660 << 3));

        // invalid
        assert!(TileId::from_path("").is_none());
        assert!(TileId::from_path("660.gph").is_none());
        assert!(TileId::from_path("660.gph").is_none());
        assert!(TileId::from_path("abc/000/818/660.gph").is_none());
        assert!(TileId::from_path("2/abc/818/660.gph").is_none());
    }

    #[test]
    fn parse_index_header() {
        let header = make_tar_header("index.bin", 48); // 3 entries x 16 bytes
        let (offset, size) = read_index_header(&header).unwrap();
        assert_eq!(offset, 512);
        assert_eq!(size, 48);
    }

    #[test]
    fn reject_non_index_first_entry() {
        let header = make_tar_header("0/000/529.gph", 1024);
        let err = read_index_header(&header).unwrap_err();
        assert!(matches!(err, TarError::FirstEntryNotIndex { .. }));
    }

    #[test]
    fn parse_index_entries() {
        // Tile IDs: level 0 tile 529 = 0 | (529 << 3) = 0x1088
        //           level 2 tile 744881 = 2 | (744881 << 3) = 0x005B1B0A
        let entries = [(3281408, 0x1088u32, 648u32), (5000000, 0x005B1B0A, 12345)];
        let data = make_index_bin(&entries);
        let index = parse_index(&data).unwrap();

        assert_eq!(index.len(), 2);

        let e0 = &index[&TileId::new(0x1088)];
        assert_eq!((e0.offset, e0.size), (3281408, 648));
        let e1 = &index[&TileId::new(0x005B1B0A)];
        assert_eq!((e1.offset, e1.size), (5000000, 12345));

        assert!(!index.contains_key(&TileId::new(0xDEAD)));
    }

    #[test]
    fn detect_compression_from_name() {
        assert_eq!(
            TileCompression::from_name("2/000/818/660.gph"),
            TileCompression::None
        );
        assert_eq!(
            TileCompression::from_name("2/000/818/660.gph.gz"),
            TileCompression::Gzip
        );
        assert_eq!(
            TileCompression::from_name("2/000/818/660.gph.zst"),
            TileCompression::Zstd
        );
        assert_eq!(
            TileCompression::from_name("index.bin"),
            TileCompression::None
        );
    }

    #[test]
    fn decompress_round_trips() {
        let plain = b"some tile bytes - graph header would go here".repeat(8);

        // None: passthrough
        let out = decompress(Bytes::copy_from_slice(&plain), TileCompression::None).unwrap();
        assert_eq!(out.as_ref(), plain.as_slice());

        // Gzip
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut enc, &plain).unwrap();
        let gz = enc.finish().unwrap();
        let out = decompress(Bytes::from(gz), TileCompression::Gzip).unwrap();
        assert_eq!(out.as_ref(), plain.as_slice());

        // Zstd
        let zst = zstd::encode_all(plain.as_slice(), 3).unwrap();
        let out = decompress(Bytes::from(zst), TileCompression::Zstd).unwrap();
        assert_eq!(out.as_ref(), plain.as_slice());
    }

    #[test]
    fn reject_invalid_index_size() {
        let data = vec![0u8; 17]; // not a multiple of 16
        let err = parse_index(&data).unwrap_err();
        assert!(matches!(err, TarError::InvalidIndexSize { .. }));
    }

    #[test]
    fn reject_empty_index() {
        let err = parse_index(&[]).unwrap_err();
        assert!(matches!(err, TarError::EmptyIndex));
    }

    #[test]
    fn octal_parsing() {
        // Standard octal: "00000031400\0" = 13056 in decimal
        assert_eq!(octal_to_u64(b"00000031400\0"), 13056);
        // With trailing spaces
        assert_eq!(octal_to_u64(b"0000144 \0\0\0\0"), 100);
        // Zero
        assert_eq!(octal_to_u64(b"00000000000\0"), 0);
    }

    #[test]
    fn parse_dataset_id_valid() {
        // Build a fake 272-byte header with dataset_id = 123456789 at offset 32
        let mut header = vec![0u8; GRAPH_TILE_HEADER_SIZE];
        let id: u64 = 123_456_789;
        header[DATASET_ID_OFFSET..DATASET_ID_OFFSET + 8].copy_from_slice(&id.to_le_bytes());

        let result = parse_dataset_id(&header).unwrap();
        assert_eq!(result, 123_456_789);
    }

    #[test]
    fn parse_dataset_id_zero() {
        let header = vec![0u8; GRAPH_TILE_HEADER_SIZE];
        let result = parse_dataset_id(&header).unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn parse_dataset_id_large_value() {
        let mut header = vec![0u8; GRAPH_TILE_HEADER_SIZE];
        let id: u64 = 0xDEAD_BEEF_CAFE_BABE;
        header[DATASET_ID_OFFSET..DATASET_ID_OFFSET + 8].copy_from_slice(&id.to_le_bytes());

        let result = parse_dataset_id(&header).unwrap();
        assert_eq!(result, 0xDEAD_BEEF_CAFE_BABE);
    }

    #[test]
    fn parse_dataset_id_too_short() {
        let header = vec![0u8; 39]; // needs at least 40 bytes (32 + 8)
        let err = parse_dataset_id(&header).unwrap_err();
        assert!(err.to_string().contains("too short"));
    }

    #[test]
    fn parse_dataset_id_exact_minimum_size() {
        let mut header = vec![0u8; DATASET_ID_OFFSET + 8]; // exactly 40 bytes
        let id: u64 = 42;
        header[DATASET_ID_OFFSET..DATASET_ID_OFFSET + 8].copy_from_slice(&id.to_le_bytes());

        let result = parse_dataset_id(&header).unwrap();
        assert_eq!(result, 42);
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
