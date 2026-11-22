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
- `<archive>` — Archive location: `s3://bucket/tiles.tar`, `https://<account>.blob.core.windows.net/<container>/tiles.tar`, or a local path.

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
# Start Rati pointing at a tile archive
rati s3://my-bucket/valhalla/tiles.tar --port 8080

# Generate a Valhalla config pointing at Rati
./valhalla_build_config \
    --mjolnir-tile-url "http://localhost:8080/tiles/{tilePath}" \
    --mjolnir-tile-dir "./valhalla_data" \
    --mjolnir-use-lru-mem-cache=True \
    --mjolnir-max-cache-size=100000000 \
    > ./valhalla.json
```

See [`valhalla_build_config`](https://github.com/valhalla/valhalla/blob/master/scripts/valhalla_build_config) for the full list of flags.

## Storage Backends

AWS S3 and Azure Blob Storage are supported by default. Keep only what you need, or neither for local archives only:

```sh
cargo install rati --no-default-features --features s3
```

**S3** uses the default AWS credential chain (env vars, SSO, IRSA, IMDS).

**Azure** reads the standard `AZURE_*` environment variables: workload identity (`AZURE_FEDERATED_TOKEN_FILE`), service principal (`AZURE_TENANT_ID` + `AZURE_CLIENT_ID` + `AZURE_CLIENT_SECRET`), or managed identity (`AZURE_CLIENT_ID` for a user-assigned one). A SAS in the URL works too, and `http://` URLs (e.g. Azurite) are read anonymously.

Replacing the archive under a running rati makes tile reads fail with 500 instead of serving stale bytes, so restart it after uploading a new one.

## Endpoints

```
GET /                              Status: dataset_id, tile_count, etag
GET /tiles/{tilePath}              Tile by path (Valhalla-compatible)
GET /tiles_by_id/{tile_id}         Tile by numeric packed ID
GET /health                        Health check
```

The `/tiles/{tilePath}` endpoint is directly compatible with Valhalla's `mjolnir.tile_url` setting, e.g. `/tiles/2/000/818/660.gph`.

## Tile Path Convention

Valhalla tile ids encode `level | (tile_index << 3)`, and paths split the zero-padded `tile_index` into groups of 3 digits (see [`valhalla::baldr::GraphId`](https://github.com/valhalla/valhalla/blob/master/valhalla/baldr/graphid.h)):

- Level 2, tile index 818660 → `2/000/818/660.gph`
- Level 0, tile index 529 → `0/000/529.gph`

## Compression

Tiles in the archive may be plain or compressed (`.gph`, `.gph.gz`, `.gph.zst`). Responses are encoded per `Accept-Encoding`, preferring zstd over gzip, and tiles are passed through as-is when the client accepts the on-disk encoding.

## CDN Headers

Every tile response includes headers suitable for CDN caching:

| Header | Description |
|--------|-------------|
| `ETag` | Archive ETag, or `"<mtime>-<size>"` for local archives |
| `Last-Modified` | Archive last-modified time |
| `Cache-Control` | `public, max-age=<n>, immutable` — `<n>` from `--cache-max-age` (default 86400) |
| `X-Dataset-Id` | Auto-detected from `GraphTileHeader`, overridden with `--dataset-id`, or the ETag as fallback |
| `Vary` | `Accept-Encoding` |
| `Content-Type` | `application/octet-stream` |

## Dataset ID

For graph tile archives the dataset ID (typically the OSM changeset ID) is read from the `GraphTileHeader` of the first tile. Otherwise pass `--dataset-id`, or the archive ETag is used.

## Index Modes

**Tile extracts with `index.bin` (default)** — archives produced by [`valhalla_build_extract`](https://github.com/valhalla/valhalla/blob/master/scripts/valhalla_build_extract). Startup takes a few small range reads regardless of archive size.

**Plain tars (`--scan-index`)** — archives without `index.bin` but with files following the [tile path convention](#tile-path-convention). Rati scans all tar headers at startup, which is slow for large archives. Non-tile entries are skipped.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE) at your option.
