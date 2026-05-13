use std::io::Write;
use std::num::NonZero;
use std::sync::Arc;

use axum::{
    Router,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode},
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

    let (path, raw_gzip) = match path.strip_suffix(".gz") {
        Some(base) => (base, true),
        None => (path.as_str(), false),
    };

    let Some(tile_id) = archive::TileId::from_path(path) else {
        return StatusCode::BAD_REQUEST.into_response();
    };

    // HEAD: return Content-Length without fetching from S3.
    // Must come before gzip — browsers send Accept-Encoding on HEAD too.
    if method == Method::HEAD {
        if raw_gzip || accepts_gzip(&headers) {
            return tile_head_gzip(&state, tile_id).into_response();
        }
        return tile_head(&state, tile_id).into_response();
    }

    // Mode 2: `.gz` extension — raw gzip file, no Content-Encoding
    if raw_gzip {
        return get_tile_data(&state, tile_id)
            .await
            .map(|data| Bytes::from(gzip_compress(&data)))
            .into_response();
    }

    // Mode 1: `Accept-Encoding: gzip` — compress on the fly with Content-Encoding
    if accepts_gzip(&headers) {
        return gzip_tile(&state, tile_id).await.into_response();
    }

    get_tile_data(&state, tile_id).await.into_response()
}

/// Supports `Accept-Encoding: gzip` (mode 1) but not `.gz` extension (mode 2),
/// because numeric IDs have no file extension to append `.gz` to.
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

    if method == Method::HEAD {
        if accepts_gzip(&headers) {
            return tile_head_gzip(&state, tile_id).into_response();
        }
        return tile_head(&state, tile_id).into_response();
    }

    if accepts_gzip(&headers) {
        return gzip_tile(&state, tile_id).await.into_response();
    }

    get_tile_data(&state, tile_id).await.into_response()
}

/// HEAD for plain tiles: return Content-Length from the index without fetching from S3.
fn tile_head(state: &AppState, tile_id: archive::TileId) -> Result<impl IntoResponse, StatusCode> {
    let size = state
        .archive
        .tile_size(tile_id)
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok([(axum::http::header::CONTENT_LENGTH, size.to_string())])
}

async fn get_tile_data(state: &AppState, tile_id: archive::TileId) -> Result<Bytes, StatusCode> {
    match state.archive.get_tile(tile_id).await {
        Ok(Some(data)) => Ok(data),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(e) => {
            tracing::error!(tile_id = %tile_id, "S3 error: {e}");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Compress tile on the fly and set `Content-Encoding: gzip`.
async fn gzip_tile(
    state: &AppState,
    tile_id: archive::TileId,
) -> Result<impl IntoResponse, StatusCode> {
    let data = get_tile_data(state, tile_id).await?;
    Ok((
        [(axum::http::header::CONTENT_ENCODING, "gzip")],
        Bytes::from(gzip_compress(&data)),
    ))
}

/// HEAD for gzip-encoded responses: only confirm the tile exists. No `Content-Length` —
/// we'd have to fetch and compress just to measure it, which defeats the point of HEAD.
fn tile_head_gzip(
    state: &AppState,
    tile_id: archive::TileId,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .archive
        .tile_size(tile_id)
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok([(axum::http::header::CONTENT_ENCODING, "gzip")])
}

/// Per RFC 9110 §13.1.2, returns `true` if any ETag in `If-None-Match` matches
/// (or the wildcard `*` is present), meaning the server should respond with 304.
fn is_not_modified(request_headers: &HeaderMap, etag: &str) -> bool {
    request_headers
        .get(axum::http::header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(',').any(|part| {
                let part = part.trim();
                part == "*" || part == etag
            })
        })
}

/// Per RFC 7231 section 5.3.4, `gzip;q=0` means gzip is explicitly unacceptable.
fn accepts_gzip(headers: &HeaderMap) -> bool {
    headers
        .get(axum::http::header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(',').any(|part| {
                let part = part.trim();
                if !part.starts_with("gzip") {
                    return false;
                }
                let after_gzip = &part["gzip".len()..];
                if after_gzip.is_empty() {
                    return true;
                }
                if let Some(rest) = after_gzip.strip_prefix(";q=") {
                    rest.trim().parse::<f32>().unwrap_or(1.0) > 0.0
                } else {
                    true
                }
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

/// Derived once at startup; the `set_cache_headers` middleware merges these into every tile response.
fn build_cache_headers(meta: &archive::ArchiveMeta, cache_max_age: u32) -> HeaderMap {
    let mut headers = HeaderMap::with_capacity(8);

    if let Ok(val) = HeaderValue::from_str(&meta.etag) {
        headers.insert(axum::http::header::ETAG, val);
    }

    if let Ok(val) = HeaderValue::from_str(&meta.last_modified) {
        headers.insert(axum::http::header::LAST_MODIFIED, val);
    }

    let cache_control = format!("public, max-age={cache_max_age}, immutable");
    if let Ok(val) = HeaderValue::from_str(&cache_control) {
        headers.insert(axum::http::header::CACHE_CONTROL, val);
    }

    headers.insert(
        axum::http::header::VARY,
        HeaderValue::from_static("Accept-Encoding"),
    );

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

        assert_eq!(headers.get(axum::http::header::ETAG).unwrap(), "\"abc123\"");
        assert_eq!(
            headers.get(axum::http::header::LAST_MODIFIED).unwrap(),
            "Thu, 17 Apr 2025 12:00:00 GMT"
        );
        assert_eq!(
            headers.get(axum::http::header::CACHE_CONTROL).unwrap(),
            "public, max-age=3600, immutable"
        );
        assert_eq!(
            headers.get(axum::http::header::VARY).unwrap(),
            "Accept-Encoding"
        );
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
            headers.get(axum::http::header::CACHE_CONTROL).unwrap(),
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
            headers.get(axum::http::header::CACHE_CONTROL).unwrap(),
            "public, max-age=0, immutable"
        );
    }

    #[test]
    fn is_not_modified_test() {
        let with = |val: &'static str| {
            let mut h = HeaderMap::new();
            h.insert(
                axum::http::header::IF_NONE_MATCH,
                HeaderValue::from_static(val),
            );
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

    #[test]
    fn accepts_gzip_test() {
        let with = |val: &'static str| {
            let mut h = HeaderMap::new();
            h.insert(
                axum::http::header::ACCEPT_ENCODING,
                HeaderValue::from_static(val),
            );
            h
        };

        // Accepts
        assert!(accepts_gzip(&with("gzip, deflate, br")));
        assert!(accepts_gzip(&with("gzip")));
        assert!(accepts_gzip(&with("gzip;q=1.0, deflate;q=0.5")));
        assert!(accepts_gzip(&with("gzip;q=0.5")));

        // Rejects
        assert!(!accepts_gzip(&with("deflate, br")));
        assert!(!accepts_gzip(&HeaderMap::new()));
        assert!(!accepts_gzip(&with("gzip;q=0, deflate")));
        assert!(!accepts_gzip(&with("gzip;q=0.0")));
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
