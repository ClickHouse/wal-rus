# wal-g compatibility

Bidirectional bucket interop is the project's acceptance bar: a bucket
written by wal-g v3.x is listable / fetchable / replayable / verifiable
/ deletable by walrus, and the reverse. CI gates the claim
(`ci/cross_tool_{forward,reverse}.sh` against a pinned wal-g
version in `.github/workflows/pg-compat.yml`; bumping the pin is a
deliberate one-line change so an interop-breaking wal-g release fails
the bump PR, not master).

## Shared on-bucket format

The on-bucket format is wal-g's verbatim, so this doc covers only the
gaps. Matched without further note: key layout `005`, the
`BackupSentinelDtoV2` sentinel (PascalCase, tolerant-deserialized so
either tool's sentinels parse), `files_metadata.json`, delta naming
(`base_<24hex>_D_<parent_24hex>`, chain via sentinel `IncrementFrom`,
format magic-detected per file), `wi1` and PG17 native INCREMENTAL
payloads, libsodium secretstream framing, prefetch dir layout, the
daemon Unix-socket protocol, the `delete` mode + modifier vocabulary,
and `WALG_*` / `PG*` / `AWS_*` / `GOOGLE_*` env naming.

## Delta page selection

Both tools emit byte-identical `wi1` / native increments (see above), so a
delta produced by either restores under either. They diverge only in how the
producer decides *which* blocks an increment carries.

wal-g defaults to a full scan (`WALG_USE_WAL_DELTA` is false by default): it
reads every page of every paged relation and ships a page only if the page is
new (`pd_upper == 0`) or its header LSN is at or past the increment-base LSN
(`incremental_page_reader.go:SelectNewValidPage`, a predicate lifted from
PostgreSQL's own page-validity checks and refined on pgsql-hackers). This is
self-validating: it needs no WAL and re-derives "changed" from each page's own
header, so a gap in the archived WAL cannot silently drop a changed block.
Setting `WALG_USE_WAL_DELTA=true` switches wal-g to instead trust a WAL-derived
changed-block bitmap (file-size gated, no per-page LSN recheck), warning and
falling back to the full scan when the bitmap can't be loaded
(`WALG_FORCE_WAL_DELTA` forbids that fallback).

walrus implements only the map-trusting path. `classify_for_delta` ships
exactly the blocks the changed-block map reports, filtered to blocks within the
current file size, with no page-LSN recheck. The map is built from WAL
`<group>_delta` sidecars (raw-WAL walk when a sidecar is missing) or from
`pg_walsummary` under `--delta-from-wal-summaries`; if it can't be built, walrus
produces a full backup rather than a scan-based delta. So for walrus
`WALG_USE_WAL_DELTA` only governs sidecar recording during wal-push, not
selection — backup-push always selects blocks from a WAL/summary map regardless
— and a walrus delta is correct only if that map is complete, whereas wal-g's
default would still catch a missed block by its page LSN.

## Deliberate divergences

| Area | walrus behavior |
|---|---|
| OpenPGP (`WALG_PGP_*`) | hard error at startup, never silently plaintext; re-encrypt to libsodium when migrating (see DESIGN.md for rationale) |
| wal-receive slot (`WALG_SLOTNAME`) | unset/empty runs slotless (no slot created or used); wal-g instead defaults to a `walg` slot. Slotless avoids pinning primary WAL when the archiver lags or dies (server recycles WAL, archive may gap) at the cost of slot retention; set `WALG_SLOTNAME` explicitly to opt into a slot |
| Backends | `file://`, `s3://`, `gs://` only; no azure / oss / swift / sh |
| statsd (`WALG_STATSD_*`) | not implemented |
| catchup-push / -fetch / -send / -receive / -list | not implemented |
| Windows | non-goal |
| Other databases (mongo, mysql, redis, …) | non-goal, Postgres only |

## Unsupported wal-g Postgres env vars

Compared against wal-g Postgres `CommonAllowedSettings`,
`PGAllowedSettings`, storage adapters, and `pgx.ParseConfig` environment
support. Unsupported means walrus ignores the variable unless noted
otherwise.

### Postgres connection

walrus parses a subset of libpq-style environment variables. wal-g uses pgx,
which accepts more connection variables:

- `PGPASSFILE`
- `PGSERVICE`
- `PGSERVICEFILE`
- `PGSSLPASSWORD`
- `PGOPTIONS`
- `PGAPPNAME`
- `PGCONNECT_TIMEOUT`
- `PGTARGETSESSIONATTRS`
- `PGTZ`
- `PGMINPROTOCOLVERSION`
- `PGMAXPROTOCOLVERSION`
- `PGSSLSNI`
- `PGSSLNEGOTIATION`
- `PGCHANNELBINDING`
- `PGREQUIREAUTH`

Partial support:

- `PGDATA`: walrus uses it only for daemon path resolution, not as
  backup-push data directory config. `backup-push <PGDATA>` positional
  syntax matches wal-g CLI behavior
- `PGHOST`, `PGPORT`: walrus supports single host/port only, not pgx
  multihost semantics

### Postgres behavior

- `WALG_UPLOAD_DISK_CONCURRENCY`
- `WALG_SENTINEL_USER_DATA`
- `WALG_UPLOAD_WAL_METADATA`
- `WALG_TAR_DISABLE_FSYNC`
- `WALG_DIRECT_IO`
- `PG_READY_RENAME`
- `WALG_SLOTNAME`
- `WALG_PG_WAL_SIZE`
- `WALG_PG_WAL_PAGE_SIZE`
- `WALG_PG_BLOCK_SIZE`
- `WALG_ALIVE_CHECK_INTERVAL`
- `WALG_STOP_BACKUP_TIMEOUT`
- `WALG_FORCE_WAL_DELTA`
- `WALG_DISABLE_PARTIAL_RESTORE`
- `WALG_USE_REVERSE_UNPACK`
- `WALG_SKIP_REDUNDANT_TARS`
- `WALG_VERIFY_PAGE_CHECKSUMS`
- `WALG_STORE_ALL_CORRUPT_BLOCKS`
- `WALG_USE_RATING_COMPOSER`
- `WALG_USE_COPY_COMPOSER`
- `WALG_USE_DATABASE_COMPOSER`
- `WALG_WITHOUT_FILES_METADATA`
- `WALG_INTEGRITY_MAX_DELAYED_WALS`
- `WALG_TARGET_STORAGE`
- `PGBACKREST_STANZA`

### Config, logging, metrics, profiling

- `WALG_CONFIG_PATH`
- `WALG_STORAGE_PREFIX`
- `WALG_LOG_DESTINATION`
- `WALG_STATSD_ADDRESS`
- `WALG_STATSD_EXTRA_TAGS`
- `PROFILE_SAMPLING_RATIO`
- `PROFILE_MODE`
- `PROFILE_PATH`
- `HTTP_LISTEN`
- `HTTP_EXPOSE_PPROF`
- `HTTP_EXPOSE_EXPVAR`
- `GOMAXPROCS`
- `GODEBUG`

Accepted by wal-g Postgres config but not used by PG code paths:

- `WALG_SERIALIZER_TYPE`
- `WALG_STREAM_CREATE_COMMAND`
- `WALG_STREAM_RESTORE_COMMAND`

### Encryption and KMS

- `WALG_CSE_KMS_ID`
- `WALG_CSE_KMS_REGION`
- `YC_CSE_KMS_KEY_ID`
- `YC_SERVICE_ACCOUNT_KEY_FILE`

### S3 storage

- `WALE_S3_PREFIX`
- `AWS_DEFAULT_REGION`
- `AWS_DEFAULT_OUTPUT`
- `AWS_PROFILE`
- `AWS_SHARED_CREDENTIALS_FILE`
- `AWS_CONFIG_FILE`
- `AWS_CA_BUNDLE`
- `AWS_ROLE_ARN`
- `AWS_ROLE_SESSION_NAME`
- `AWS_WEB_IDENTITY_TOKEN_FILE`
- `AWS_DUAL_STACK`
- `WALG_S3_CA_CERT_FILE`
- `S3_CA_CERT_FILE`
- `WALG_S3_STORAGE_CLASS`
- `S3_STORAGE_CLASS`
- `WALG_S3_SSE`
- `S3_SSE`
- `WALG_S3_SSE_C`
- `S3_SSE_C`
- `WALG_S3_SSE_KMS_ID`
- `S3_SSE_KMS_ID`
- `WALG_S3_MAX_PART_SIZE`
- `S3_MAX_PART_SIZE`
- `WALG_S3_ENDPOINT_SOURCE`
- `S3_ENDPOINT_SOURCE`
- `WALG_S3_ENDPOINT_PORT`
- `S3_ENDPOINT_PORT`
- `WALG_S3_USE_LIST_OBJECTS_V1`
- `S3_USE_LIST_OBJECTS_V1`
- `WALG_S3_LOG_LEVEL`
- `S3_LOG_LEVEL`
- `WALG_S3_RANGE_BATCH_ENABLED`
- `S3_RANGE_BATCH_ENABLED`
- `WALG_S3_RANGE_MAX_RETRIES`
- `S3_RANGE_MAX_RETRIES`
- `WALG_S3_MAX_RETRIES`
- `S3_MAX_RETRIES`
- `S3_SKIP_VALIDATION`
- `S3_USE_YC_SESSION_TOKEN`
- `UPLOAD_CONCURRENCY`
- `S3_REQUEST_ADDITIONAL_HEADERS`
- `S3_MIN_THROTTLING_RETRY_DELAY`
- `S3_MAX_THROTTLING_RETRY_DELAY`
- `S3_RETENTION_PERIOD`
- `S3_RETENTION_MODE`
- `S3_DISABLE_100_CONTINUE`
- `S3_ENABLE_VERSIONING`
- `S3_DELETE_BATCH_SIZE`

### GCS storage

- `WALE_GS_PREFIX`
- `GCS_CONTEXT_TIMEOUT`
- `GCS_NORMALIZE_PREFIX`
- `GCS_ENCRYPTION_KEY`
- `GCS_MAX_CHUNK_SIZE`
- `GCS_MAX_RETRIES`

GCE/GKE metadata-server auth is not implemented.

### Storage backends not implemented

Azure, Alicloud OSS, Swift, and SSH backends are absent (see the
divergence table), so all their env vars (`WALG_AZ_PREFIX` / `AZURE_*`,
`WALG_OSS_PREFIX` / `OSS_*`, `WALG_SWIFT_PREFIX` / `OS_*`,
`WALG_SSH_PREFIX` / `SSH_*`), the `WALE_FILE_PREFIX` file alias, and
failover storages (`WALG_FAILOVER_STORAGES*`) are unsupported.

### Storage aliases

wal-g storage adapter settings accept exact backend keys first, then
`WALG_<key>` and `WALE_<key>` compatibility variants. walrus does not
implement this generic alias rule, so aliases like
`WALG_S3_SKIP_VALIDATION`, `WALE_S3_SKIP_VALIDATION`,
`WALG_GCS_MAX_RETRIES`, `WALE_GCS_MAX_RETRIES`,
`WALG_OSS_REGION`, and `WALE_OSS_REGION` are unsupported unless
explicitly listed as supported elsewhere in this document.
