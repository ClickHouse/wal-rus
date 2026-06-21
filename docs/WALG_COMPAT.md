# wal-g compatibility

Bidirectional bucket interop is the project's acceptance bar: a bucket
written by wal-g v3.x is listable / fetchable / replayable / verifiable
/ deletable by walrus, and the reverse. CI gates the claim
(`ci/cross_tool_{forward,reverse}.sh` against a pinned wal-g
version in `.github/workflows/pg-compat.yml`; bumping the pin is a
deliberate one-line change so an interop-breaking wal-g release fails
the bump PR, not master).

## Shared on-bucket format

- Key layout version `005`: `wal_005/<segment>[.<ext>]`,
  `basebackups_005/<name>/tar_partitions/part_NNN.tar.<ext>`,
  `pg_control.tar.<ext>` tee, sentinel at
  `basebackups_005/<name>_backup_stop_sentinel.json` (one level above
  the per-backup dir, same asymmetry as wal-g)
- Sentinel mirrors `BackupSentinelDtoV2` field-for-field, PascalCase
  keys, `Spec` for tablespaces; every Option field tolerant-deserializes
  so sentinels from either tool parse
- `files_metadata.json` schema (`Files`, `TarFileSets`)
- Delta naming `base_<24hex>_D_<parent_24hex>`; chain discovered via
  sentinel `IncrementFrom`, format detected per-file by magic byte, no
  sentinel format flag (wal-g convention)
- `wi1` increment format and PG17 native INCREMENTAL format
  (magic `0xd3ae1f0d`); native layout verified field-by-field against
  postgres source (`src/common/blkreftable.c`,
  `src/backend/backup/basebackup.c`,
  `src/bin/pg_combinebackup/reconstruct.c`)
- libsodium framing: 24-byte secretstream header, 8 KiB plaintext
  chunks, 17-byte per-chunk overhead, explicit FINAL chunk on close;
  a wire-format pin test fails on any drift
- Prefetch dir layout `pg_wal/.wal-g/prefetch/{running/,}` so a sidecar
  can run either tool against the same pg_wal
- Daemon Unix-socket protocol byte format (Check / WalPush / WalFetch)
- `delete` mode + modifier vocabulary (`before` / `retain` /
  `everything` / `target` / `garbage`; `FULL`, `FIND_FULL`, `FORCE`,
  `ARCHIVES`, `BACKUPS`, `--after`), permanent-backup WAL reservation,
  `--confirm` gate
- Env vars follow `WALG_*` / `PG*` / `AWS_*` / `GOOGLE_*` naming

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
  backup-push data directory config
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
- `AWS_ENDPOINT`
- `AWS_S3_FORCE_PATH_STYLE`
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

Azure:

- `WALG_AZ_PREFIX`
- `WALE_AZ_PREFIX`
- `AZURE_STORAGE_ACCOUNT`
- `AZURE_STORAGE_ACCESS_KEY`
- `AZURE_STORAGE_SAS_TOKEN`
- `AZURE_CLIENT_ID`
- `AZURE_TENANT_ID`
- `AZURE_CLIENT_SECRET`
- `AZURE_ENVIRONMENT_NAME`
- `AZURE_ENDPOINT_SUFFIX`
- `AZURE_BUFFER_SIZE`
- `WALG_AZURE_BUFFER_SIZE`
- `AZURE_MAX_BUFFERS`
- `WALG_AZURE_MAX_BUFFERS`
- `AZURE_TRY_TIMEOUT`
- `AZURE_BLOB_STORE_API_VERSION`

Alicloud OSS:

- `WALG_OSS_PREFIX`
- `WALE_OSS_PREFIX`
- `OSS_ACCESS_KEY_ID`
- `OSS_ACCESS_KEY_SECRET`
- `OSS_SESSION_TOKEN`
- `OSS_ENDPOINT`
- `OSS_REGION`
- `OSS_ROLE_ARN`
- `OSS_ROLE_SESSION_NAME`
- `OSS_SKIP_VALIDATION`
- `OSS_MAX_RETRIES`
- `OSS_CONNECT_TIMEOUT`
- `OSS_UPLOAD_PART_SIZE`
- `OSS_COPY_PART_SIZE`

Swift:

- `WALG_SWIFT_PREFIX`
- `WALE_SWIFT_PREFIX`
- `OS_AUTH_URL`
- `OS_USERNAME`
- `OS_PASSWORD`
- `OS_TENANT_NAME`
- `OS_REGION_NAME`

SSH storage:

- `WALG_SSH_PREFIX`
- `WALE_SSH_PREFIX`
- `SSH_PORT`
- `SSH_USERNAME`
- `SSH_PASSWORD`
- `SSH_PRIVATE_KEY_PATH`

File storage alias:

- `WALE_FILE_PREFIX`

### Failover storage

- `WALG_FAILOVER_STORAGES`
- `WALG_FAILOVER_STORAGES_CHECK`
- `WALG_FAILOVER_STORAGES_CHECK_TIMEOUT`
- `WALG_FAILOVER_STORAGES_CHECK_SIZE`
- `WALG_FAILOVER_STORAGES_CACHE_LIFETIME`
- `WALG_FAILOVER_STORAGES_CACHE_EMA_ALIVE_LIMIT`
- `WALG_FAILOVER_STORAGES_CACHE_EMA_DEAD_LIMIT`
- `WALG_FAILOVER_STORAGES_CACHE_EMA_ALPHA_ALIVE_MAX`
- `WALG_FAILOVER_STORAGES_CACHE_EMA_ALPHA_ALIVE_MIN`
- `WALG_FAILOVER_STORAGES_CACHE_EMA_ALPHA_DEAD_MAX`
- `WALG_FAILOVER_STORAGES_CACHE_EMA_ALPHA_DEAD_MIN`

### Storage aliases

wal-g storage adapter settings accept exact backend keys first, then
`WALG_<key>` and `WALE_<key>` compatibility variants. walrus does not
implement this generic alias rule, so aliases like
`WALG_S3_SKIP_VALIDATION`, `WALE_S3_SKIP_VALIDATION`,
`WALG_GCS_MAX_RETRIES`, `WALE_GCS_MAX_RETRIES`,
`WALG_OSS_REGION`, and `WALE_OSS_REGION` are unsupported unless
explicitly listed as supported elsewhere in this document.
