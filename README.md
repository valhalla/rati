# Rati

Rati (Range-Accessed Tar Index) is a lightweight HTTP server that serves individual [Valhalla](https://github.com/valhalla/valhalla) tiles from tar archives — stored on S3, Azure Blob Storage, or on the local filesystem — via byte-range reads.
Named after the auger Odin used to bore through a mountain to reach the mead of poetry locked within.

Rati was created with two use cases in mind:

- **Predictive caching for offline navigation** — a CDN-friendly endpoint that lets mobile apps prefetch individual routing tiles along a planned route while still online, enabling fully offline navigation later.
- **Zero-download Valhalla setup** — Valhalla supports loading tiles from HTTP via `mjolnir.tile_url`. Point Valhalla at a Rati instance backed by S3 and get a working router with near-zero startup time — no need to download an 80 GB planet tarball first.

## Usage

```
rati <archive> [OPTIONS]
```

**Arguments:**
- `<archive>` — Archive location. An S3 URL (`s3://bucket/path/to/tiles.tar`), an Azure Blob
  URL (`https://<account>.blob.core.windows.net/<container>/tiles.tar`), or a path to a local
  `.tar` file. Anything else is treated as a local path.

**Options:**
| Flag | Default | Description |
|------|---------|-------------|
| `--scan-index` | off | Build index by scanning tar headers if `index.bin` is missing |
| `--dataset-id <ID>` | auto | Override the dataset ID (auto-detected from `GraphTileHeader` if omitted) |
| `--cache-max-age <SECONDS>` | `86400` | `Cache-Control` max-age in seconds |
| `--port <PORT>` | `3000` | Port to listen on |
| `--concurrency <N>` | `4` | Max worker threads |

### Example with Valhalla

```sh
# Start Rati pointing at an S3 tile archive...
rati s3://my-bucket/valhalla/tiles.tar --port 8080

# ...or at a local tar file
rati ./tiles.tar --port 8080

# Generate a Valhalla config pointing at Rati
./valhalla_build_config \
    --mjolnir-tile-url "http://localhost:8080/tiles/{tilePath}" \
    --mjolnir-tile-dir "./valhalla_data" \
    --mjolnir-use-lru-mem-cache=True \
    --mjolnir-max-cache-size=100000000 \
    > ./valhalla.json
```

See [`valhalla_build_config`](https://github.com/valhalla/valhalla/blob/master/scripts/valhalla_build_config) for the full list of flags.

### Azure Blob Storage

```sh
rati "https://myaccount.blob.core.windows.net/valhalla/tiles.tar" --port 8080
```

There are no Azure flags. The identity is chosen from the environment, using the same
variables every Azure SDK reads, in the order `DefaultAzureCredential` uses elsewhere — the
first row that matches wins:

| Condition | Identity |
|-----------|----------|
| URL is `http://`, or carries a SAS (`?...&sig=...`) | none — anonymous |
| Host is not an Azure Blob endpoint | none — anonymous |
| `AZURE_FEDERATED_TOKEN_FILE` | workload identity (AKS) |
| `AZURE_TENANT_ID` + `AZURE_CLIENT_ID` + `AZURE_CLIENT_SECRET` | service principal |
| `AZURE_CLIENT_ID` | user-assigned managed identity |
| otherwise | system-assigned managed identity |

Three rati-specific notes:

- A bearer token is only ever sent to an `https` Azure Blob host. An archive behind a CDN,
  Front Door, or a storage-account custom domain is reached anonymously, so it must be
  public or the URL must carry a SAS.
- Plaintext (`http://`) endpoints are always anonymous — rati will not put a bearer token on
  the wire in the clear. Azurite therefore needs no configuration at all; point rati at
  `http://127.0.0.1:10000/devstoreaccount1/valhalla/tiles.tar`. For local development against
  real blob storage, use a service principal or a SAS.
- Do not set `Content-Encoding` on the archive blob. A blob-level encoding confuses CDNs and
  proxies in front of rati.

## Build Features

| Feature | Default | Pulls in |
|---------|---------|----------|
| `s3` | yes | `aws-config`, `aws-sdk-s3` |
| `azure` | yes | `azure_storage_blob`, `azure_identity` |

Local archives need neither. Dropping an unused backend removes its HTTP and TLS stack:

```sh
cargo build --release --no-default-features --features azure
```

## Endpoints

```
GET /                              Status: dataset_id, tile_count, etag
GET /tiles/{tilePath}              Tile by path (Valhalla-compatible)
GET /tiles_by_id/{tile_id}         Tile by numeric packed ID
GET /health                        Health check
```

The `/tiles/{tilePath}` endpoint is directly compatible with Valhalla's `mjolnir.tile_url` setting, e.g. `/tiles/2/000/818/660.gph`.

## Tile Path Convention

Valhalla identifies tiles by a packed ID that encodes `level | (tile_index << 3)` — 3 bits for the hierarchy level, 22 bits for the tile index within a level's grid (see [`valhalla::baldr::GraphId`](https://github.com/valhalla/valhalla/blob/master/valhalla/baldr/graphid.h)).

File paths are derived by zero-padding `tile_index` to the nearest multiple of 3 digits and splitting into groups of 3 separated by `/`, with the level as the first path component. This keeps directory fan-out under ~1000 entries.

Examples:
- Level 2, tile index 818660 → `2/000/818/660.gph`
- Level 0, tile index 529 → `0/000/529.gph`

## Compression

Rati negotiates the response encoding via `Accept-Encoding`. Both **gzip** and **zstd**
are supported on the wire; when both are accepted, rati prefers zstd (better ratio at
similar speed).

Tiles inside the archive can themselves be compressed — `.gph`, `.gph.gz`, or
`.gph.zst`. The on-disk compression is detected once at startup from the first tile's
filename suffix (assumed uniform across the archive) and rati decompresses transparently
when serving clients that asked for a different encoding. When the on-disk encoding
matches what the client wants, rati passes the bytes straight through — no decode, no
re-encode.

The HEAD path advertises `Content-Length` only when the response encoding matches what's
on disk (the only case where the index size equals the body size we'd send); otherwise
the header is omitted rather than fetching+decoding the tile just to measure it.

## CDN Headers

Every tile response includes headers suitable for CDN caching:

| Header | Description |
|--------|-------------|
| `ETag` | The object or blob ETag, or synthesized `"<mtime>-<size>"` for local archives — all change whenever the archive is replaced |
| `Last-Modified` | The object or blob last-modified timestamp, or the file's mtime for local archives |
| `Cache-Control` | `public, max-age=<n>, immutable` — `<n>` from `--cache-max-age` (default 86400) |
| `X-Dataset-Id` | Auto-detected from `GraphTileHeader`, overridden with `--dataset-id`, or the ETag as fallback |
| `Vary` | `Accept-Encoding` — ensures correct CDN behavior with encoding negotiation |
| `Content-Type` | `application/octet-stream` |

## Dataset ID

For graph tile archives (`.gph`), the dataset ID is automatically extracted from the `GraphTileHeader` of the first tile in the archive. This is typically the OSM changeset ID (`dataset_id_` field, a `u64` at byte offset 32 in the 272-byte header).

For any other kind of archive, use `--dataset-id` to provide an explicit value. If neither works, the archive ETag is used as a fallback.

## Index Modes

Rati supports two archive formats:

**Tile extracts with `index.bin` (default)** — The archive contains `index.bin` as its first entry, a flat binary index where each 16-byte entry holds `(offset: u64, tile_id: u32, size: u32)` in little-endian format. This is the format produced by [`valhalla_build_extract`](https://github.com/valhalla/valhalla/blob/master/scripts/valhalla_build_extract). At startup, Rati reads only the first tar header (512 bytes) plus the index payload — two small range requests, fast regardless of archive size.

**Plain tars (`--scan-index`)** — For tar archives without `index.bin` but with files following Valhalla's [naming convention](#tile-path-convention), pass `--scan-index`. Rati scans all tar headers and indexes each filename that parses as a valid tile path. Non-tile entries are silently skipped. This requires reading the full archive at startup, so it is slower for large files.

## Build

```sh
cargo build --release
```

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE) at your option.
