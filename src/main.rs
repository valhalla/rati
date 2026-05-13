use std::io::Write;
use std::num::NonZero;
use std::sync::Arc;

use axum::{
    Router,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    middleware,
    response::{IntoResponse, Response},
    routing::get,
};
use bytes::Bytes;
use clap::Parser;
use flate2::Compression;
use serde::Serialize;
use tokio::signal;
use tracing::info;

mod archive;

use archive::TileCompression;

#[derive(Parser)]
struct Config {
    /// S3 location of the archive: s3://bucket/key.tar
    archive: String,
    /// Build index by scanning tar headers if index.bin is missing
    #[arg(long)]
    scan_index: bool,
    /// Override the dataset ID (auto-detected from graph tile headers if omitted)
    #[arg(long)]
    dataset_id: Option<String>,
    /// Cache-Control max-age in seconds
    #[arg(long, default_value_t = 86400)]
    cache_max_age: u32,
    /// Port to listen
    #[arg(long, default_value_t = 3000)]
    port: u16,
    /// Max threads to use
    #[arg(long, default_value_t = NonZero::new(4).unwrap())]
    concurrency: NonZero<u16>,
}

#[derive(Clone)]
struct AppState {
    archive: Arc<archive::S3Archive>,
    /// Pre-built status response (nothing changes at runtime).
    status: StatusResponse,
}

fn main() {
    tracing_subscriber::fmt::init();

    let config = Config::parse();

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(
            std::thread::available_parallelism()
                .map(NonZero::get)
                .unwrap_or(8) // fallback if we can't detect CPU count
                .min(config.concurrency.get() as usize),
        )
        .enable_all()
        .build()
        .unwrap()
        .block_on(run(config))
}

async fn run(config: Config) {
    let (archive, meta) = archive::S3Archive::open(
        &config.archive,
        config.scan_index,
        config.dataset_id.as_deref(),
    )
    .await
    .expect("failed to load tar index from S3");
    info!(
        "Loaded {} with {} tiles (dataset_id={})",
        config.archive, meta.tile_count, meta.dataset_id,
    );

    let cache_headers = build_cache_headers(&meta, config.cache_max_age);
    let state = AppState {
        archive: Arc::new(archive),
        status: StatusResponse {
            dataset_id: meta.dataset_id,
            tile_count: meta.tile_count,
            etag: meta.etag,
        },
    };

    let app = Router::new()
        .route("/tiles/{*path}", get(get_tile))
        .route("/tiles_by_id/{tile_id}", get(get_tile_by_id))
        .layer(middleware::from_fn_with_state(
            cache_headers,
            set_cache_headers,
        ))
        .route("/", get(get_status))
        .route("/health", get(|| async { "OK" }))
        .layer(tower_http::cors::CorsLayer::permissive())
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", config.port))
        .await
        .unwrap();
    info!("Listening at http://0.0.0.0:{}", config.port);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            tokio::select! {
                _ = signal::ctrl_c() => {
                    info!("Ctrl+C received, shutting down");
                }
                _ = async {
                    signal::unix::signal(signal::unix::SignalKind::terminate())
                        .expect("failed to install SIGTERM signal handler")
                        .recv()
                        .await
                } => {
                    info!("SIGTERM received, shutting down");
                }
            }
        })
        .await
        .unwrap();
}

#[derive(Clone, Serialize)]
struct StatusResponse {
    dataset_id: Box<str>,
    tile_count: usize,
    etag: Box<str>,
}

async fn get_status(State(state): State<AppState>) -> axum::Json<StatusResponse> {
    axum::Json(state.status.clone())
}

/// Middleware that merges pre-built tile headers into every response.
async fn set_cache_headers(
    State(cache_headers): State<HeaderMap>,
    request: axum::extract::Request,
    next: middleware::Next,
) -> Response {
    let mut response = next.run(request).await;
    response.headers_mut().extend(cache_headers);
    response
}

async fn get_tile(
    method: Method,
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(path): Path<String>,
) -> Response {
    if is_not_modified(&headers, &state.status.etag) {
        return StatusCode::NOT_MODIFIED.into_response();
    }

    let Some(tile_id) = archive::TileId::from_path(&path) else {
        return StatusCode::BAD_REQUEST.into_response();
    };

    let response_encoding = pick_response_encoding(&headers, state.archive.compression());

    if method == Method::HEAD {
        return tile_head(&state, tile_id, response_encoding);
    }
    serve_tile(&state, tile_id, response_encoding).await
}

async fn get_tile_by_id(
    method: Method,
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(tile_id): Path<u32>,
) -> Response {
    if is_not_modified(&headers, &state.status.etag) {
        return StatusCode::NOT_MODIFIED.into_response();
    }

    let tile_id = archive::TileId::new(tile_id);
    let response_encoding = pick_response_encoding(&headers, state.archive.compression());

    if method == Method::HEAD {
        return tile_head(&state, tile_id, response_encoding);
    }
    serve_tile(&state, tile_id, response_encoding).await
}

/// Body that yields nothing and reports unknown size, so hyper doesn't auto-emit
/// `Content-Length: 0` from the size hint. Used for HEAD responses where we want
/// to omit `Content-Length` (the index size doesn't match what GET would return).
struct EmptyUnknownSize;

impl http_body::Body for EmptyUnknownSize {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        std::task::Poll::Ready(None)
    }

    fn size_hint(&self) -> http_body::SizeHint {
        http_body::SizeHint::default()
    }
}

/// HEAD: never fetch tile bytes. Set `Content-Encoding` for compressed responses,
/// and advertise `Content-Length` only when the response encoding matches what's
/// already on disk — that's the only case where the index size equals the body size
/// we'd send.
///
/// The body is [`EmptyUnknownSize`] rather than [`axum::body::Body::empty`] so hyper
/// doesn't auto-emit `Content-Length: 0` from the body's exact size hint when we
/// deliberately want the header omitted.
fn tile_head(
    state: &AppState,
    tile_id: archive::TileId,
    response_encoding: TileCompression,
) -> Response {
    let Some(size) = state.archive.tile_size(tile_id) else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let mut response = Response::new(axum::body::Body::new(EmptyUnknownSize));
    let h = response.headers_mut();
    if response_encoding != TileCompression::None {
        h.insert(
            header::CONTENT_ENCODING,
            HeaderValue::from_static(response_encoding.header_name()),
        );
    }
    if response_encoding == state.archive.compression() {
        h.insert(header::CONTENT_LENGTH, HeaderValue::from(size));
    }
    response
}

/// GET: fetch the tile and produce bytes in the negotiated encoding. Pass-through when
/// the on-disk encoding already matches; otherwise decompress to plain and re-encode.
async fn serve_tile(
    state: &AppState,
    tile_id: archive::TileId,
    response_encoding: TileCompression,
) -> Response {
    let tile = match state.archive.get_tile(tile_id).await {
        Ok(Some(b)) => b,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!(tile_id = %tile_id, "S3 error: {e}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let compression = state.archive.compression();
    let body = if compression == response_encoding {
        tile
    } else {
        let plain = match archive::decompress(tile, compression) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(tile_id = %tile_id, "decode error: {e}");
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        };
        match response_encoding {
            TileCompression::None => plain,
            TileCompression::Gzip => Bytes::from(gzip_compress(&plain)),
            TileCompression::Zstd => Bytes::from(zstd_compress(&plain)),
        }
    };

    let mut response = body.into_response();
    if response_encoding != TileCompression::None {
        response.headers_mut().insert(
            header::CONTENT_ENCODING,
            HeaderValue::from_static(response_encoding.header_name()),
        );
    }
    response
}

/// Pick the response encoding from `Accept-Encoding` and the on-disk compression.
///
/// Single pass over the header populates `(zstd_ok, gzip_ok)`; the decision then
/// mirrors the negotiation table:
/// - Source matches an accepted encoding → pass through (no decode, no re-encode).
/// - Otherwise pick the best accepted encoding, preferring zstd over gzip.
/// - Nothing matches → plain.
fn pick_response_encoding(headers: &HeaderMap, source: TileCompression) -> TileCompression {
    use TileCompression::*;

    const ZSTD: &str = TileCompression::Zstd.header_name();
    const GZIP: &str = TileCompression::Gzip.header_name();

    let mut zstd_ok = false;
    let mut gzip_ok = false;
    if let Some(accept) = headers
        .get(header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok())
    {
        for token in accept.split(',') {
            let token = token.trim();
            let (name, params) = token.split_once(';').unwrap_or((token, ""));
            // Per RFC 7231 §5.3.4: `<encoding>;q=0` rejects the encoding.
            let rejected = params.split(';').any(|p| {
                p.trim()
                    .strip_prefix("q=")
                    .and_then(|q| q.trim().parse::<f32>().ok())
                    .is_some_and(|q| q <= 0.0)
            });
            if rejected {
                continue;
            }
            if name.eq_ignore_ascii_case(ZSTD) {
                zstd_ok = true;
            } else if name.eq_ignore_ascii_case(GZIP) {
                gzip_ok = true;
            }
        }
    }

    match (source, zstd_ok, gzip_ok) {
        (Zstd, true, _) => Zstd,  // passthrough
        (Gzip, _, true) => Gzip,  // passthrough
        (_, true, _) => Zstd,     // re-encode, prefer zstd
        (_, false, true) => Gzip, // re-encode to gzip
        _ => None,
    }
}

/// Per RFC 9110 §13.1.2, returns `true` if any ETag in `If-None-Match` matches
/// (or the wildcard `*` is present), meaning the server should respond with 304.
fn is_not_modified(request_headers: &HeaderMap, etag: &str) -> bool {
    request_headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(',').any(|part| {
                let part = part.trim();
                part == "*" || part == etag
            })
        })
}

fn gzip_compress(data: &[u8]) -> Vec<u8> {
    const GZIP_LEVEL: Compression = Compression::new(6); // good balance between size and performance
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), GZIP_LEVEL);
    encoder
        .write_all(data)
        .expect("gzip write to Vec cannot fail");
    encoder.finish().expect("gzip finish on Vec cannot fail")
}

fn zstd_compress(data: &[u8]) -> Vec<u8> {
    zstd::encode_all(data, 0).expect("zstd encode to Vec cannot fail")
}

/// Derived once at startup; the `set_cache_headers` middleware merges these into every tile response.
fn build_cache_headers(meta: &archive::ArchiveMeta, cache_max_age: u32) -> HeaderMap {
    let mut headers = HeaderMap::with_capacity(8);

    if let Ok(val) = HeaderValue::from_str(&meta.etag) {
        headers.insert(header::ETAG, val);
    }

    if let Ok(val) = HeaderValue::from_str(&meta.last_modified) {
        headers.insert(header::LAST_MODIFIED, val);
    }

    let cache_control = format!("public, max-age={cache_max_age}, immutable");
    if let Ok(val) = HeaderValue::from_str(&cache_control) {
        headers.insert(header::CACHE_CONTROL, val);
    }

    headers.insert(header::VARY, HeaderValue::from_static("Accept-Encoding"));

    if let Ok(val) = HeaderValue::from_str(&meta.dataset_id) {
        headers.insert("X-Dataset-Id", val);
    }

    headers
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_headers_include_all_required_fields() {
        let meta = archive::ArchiveMeta {
            etag: "\"abc123\"".into(),
            last_modified: "Thu, 17 Apr 2025 12:00:00 GMT".into(),
            dataset_id: "42".into(),
            tile_count: 100,
        };
        let headers = build_cache_headers(&meta, 3600);

        assert_eq!(headers.get(header::ETAG).unwrap(), "\"abc123\"");
        assert_eq!(
            headers.get(header::LAST_MODIFIED).unwrap(),
            "Thu, 17 Apr 2025 12:00:00 GMT"
        );
        assert_eq!(
            headers.get(header::CACHE_CONTROL).unwrap(),
            "public, max-age=3600, immutable"
        );
        assert_eq!(headers.get(header::VARY).unwrap(), "Accept-Encoding");
        assert_eq!(headers.get("X-Dataset-Id").unwrap(), "42");
    }

    #[test]
    fn cache_headers_default_cache_max_age() {
        let meta = archive::ArchiveMeta {
            etag: "\"x\"".into(),
            last_modified: "Thu, 01 Jan 2025 00:00:00 GMT".into(),
            dataset_id: "test-dataset".into(),
            tile_count: 1,
        };
        let headers = build_cache_headers(&meta, 86400);

        assert_eq!(
            headers.get(header::CACHE_CONTROL).unwrap(),
            "public, max-age=86400, immutable"
        );
    }

    #[test]
    fn cache_headers_zero_max_age() {
        let meta = archive::ArchiveMeta {
            etag: "\"x\"".into(),
            last_modified: "Thu, 01 Jan 2025 00:00:00 GMT".into(),
            dataset_id: "ds".into(),
            tile_count: 1,
        };
        let headers = build_cache_headers(&meta, 0);

        assert_eq!(
            headers.get(header::CACHE_CONTROL).unwrap(),
            "public, max-age=0, immutable"
        );
    }

    #[test]
    fn is_not_modified_test() {
        let with = |val: &'static str| {
            let mut h = HeaderMap::new();
            h.insert(header::IF_NONE_MATCH, HeaderValue::from_static(val));
            h
        };
        let etag = "\"abc123\"";

        // Matches
        assert!(is_not_modified(&with("\"abc123\""), etag));
        assert!(is_not_modified(&with("\"other\", \"abc123\""), etag));
        assert!(is_not_modified(&with("*"), etag));

        // No match
        assert!(!is_not_modified(&with("\"different\""), etag));
        assert!(!is_not_modified(&HeaderMap::new(), etag));
        assert!(!is_not_modified(&with("\"abc123-modified\""), etag));
    }

    fn with_accept_encoding(val: &'static str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(header::ACCEPT_ENCODING, HeaderValue::from_static(val));
        h
    }

    #[test]
    fn pick_response_encoding_test() {
        use TileCompression::*;
        let pick = |hdr, src| pick_response_encoding(&with_accept_encoding(hdr), src);

        // Passthrough wins, even when other encodings are accepted.
        assert_eq!(pick("gzip, zstd", Gzip), Gzip);
        assert_eq!(pick("gzip, zstd", Zstd), Zstd);
        assert_eq!(pick("gzip", Gzip), Gzip);
        assert_eq!(pick("zstd", Zstd), Zstd);

        // Plain source: prefer zstd over gzip.
        assert_eq!(pick("gzip, zstd", None), Zstd);
        assert_eq!(pick("gzip", None), Gzip);
        assert_eq!(pick("zstd", None), Zstd);

        // Re-encode when source isn't accepted.
        assert_eq!(pick("gzip", Zstd), Gzip);
        assert_eq!(pick("zstd", Gzip), Zstd);

        // q=0 rejects.
        assert_eq!(pick("gzip;q=0, zstd", Gzip), Zstd);
        assert_eq!(pick("gzip;q=0.0", Gzip), None);

        // q parsing still accepts positive quality values.
        assert_eq!(pick("gzip;q=0.5", None), Gzip);
        assert_eq!(pick("gzip;q=1.0, deflate;q=0.5", None), Gzip);

        // Nothing useful accepted.
        assert_eq!(pick("deflate", Zstd), None);
        assert_eq!(pick("gzip-foo", None), None); // prefix lookalike
        assert_eq!(pick_response_encoding(&HeaderMap::new(), Gzip), None);
    }

    #[test]
    fn gzip_round_trip() {
        use flate2::read::GzDecoder;
        use std::io::Read;

        let original = b"Hello, Valhalla tile data!";
        let compressed = gzip_compress(original);
        let mut decoder = GzDecoder::new(compressed.as_slice());
        let mut result = Vec::new();
        decoder.read_to_end(&mut result).unwrap();
        assert_eq!(result, original);
    }

    #[test]
    fn gzip_starts_with_magic() {
        let compressed = gzip_compress(b"test");
        assert_eq!(compressed[0], 0x1f);
        assert_eq!(compressed[1], 0x8b);
    }
}
