//! `st` storage tools: cat/get/put/rm/ls/copy handlers
//!
//! Mirror wal-g `internal/storagetools` HandleCatObject / HandleGetObject /
//! HandlePutObject / HandleRemove / HandleFolderList semantics over the walrus
//! `Storage` trait. `copy` diverges from wal-g (URI endpoints, not config
//! files); wal-g's failover-storage `transfer` has no walrus equivalent

use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};
use futures::StreamExt;
use glob::Pattern;
use tokio::io::{self, AsyncWriteExt};

use super::DynStorage;
use crate::compression::{self, Method};
use crate::config::Settings;

fn extension(path: &str) -> &str {
    let base = basename(path);
    match base.rfind('.') {
        Some(i) if i > 0 => &base[i..],
        _ => "",
    }
}

fn basename(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

/// `st cat`: stream an object to stdout. decrypt/decompress default OFF; when
/// decompress is on, codec is chosen by the object's extension, warning &
/// passing through on an unknown extension. `glob` treats `path` as a pattern
pub async fn cat(
    settings: &Settings,
    storage: &DynStorage,
    path: &str,
    decrypt: bool,
    decompress: bool,
    glob: bool,
) -> Result<()> {
    let all;
    let keys: Vec<&str> = if glob {
        all = list_keys(storage, "").await?;
        glob_objects(&all, path)?
    } else {
        vec![path]
    };
    let mut out = io::stdout();
    for key in &keys {
        let mut reader = download(settings, storage, key, decrypt, decompress).await?;
        io::copy(&mut reader, &mut out)
            .await
            .with_context(|| format!("cat {key}"))?;
    }
    out.flush().await?;
    Ok(())
}

/// `st get`: download an object to a local path. decrypt/decompress default ON.
/// Destination resolves like wal-g `getTargetFilePath`: a missing path is used
/// verbatim, a directory becomes `<dir>/<basename>`, an existing file is
/// overwritten only if it is the named target. Created exclusively (O_EXCL);
/// a partial file is removed on error
pub async fn get(
    settings: &Settings,
    storage: &DynStorage,
    path: &str,
    dst: &Path,
    decrypt: bool,
    decompress: bool,
) -> Result<()> {
    let target = target_path(dst, basename(path)).context("determine the destination path")?;
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&target)
        .await
        .with_context(|| format!("open the destination file {}", target.display()))?;

    let result = async {
        let mut reader = download(settings, storage, path, decrypt, decompress).await?;
        io::copy(&mut reader, &mut file)
            .await
            .with_context(|| format!("download {path}"))?;
        file.flush().await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;

    if let Err(e) = result {
        let _ = tokio::fs::remove_file(&target).await;
        return Err(e);
    }
    Ok(())
}

/// Open a downloaded object as a reader, optionally decrypting then
/// decompressing-by-extension. Shared by cat & get
async fn download(
    settings: &Settings,
    storage: &DynStorage,
    key: &str,
    decrypt: bool,
    decompress: bool,
) -> Result<compression::AsyncReader> {
    let body = storage
        .get(key)
        .await
        .with_context(|| format!("download the file {key}"))?;
    let body = if decrypt {
        settings.decrypt(body)
    } else {
        body
    };
    if !decompress {
        return Ok(body);
    }
    match Method::from_extension(extension(key)) {
        Some(method) => Ok(compression::decode(method, body)),
        None => {
            tracing::warn!(
                "decompressor for extension '{}' was not found, will download uncompressed",
                extension(key)
            );
            Ok(body)
        }
    }
}

/// `st put`: upload a local file or stdin to a destination key. encrypt/compress
/// default ON. When compress, the configured compressor's extension is appended
/// to the destination key for both the overwrite check and the upload. Without
/// `-f`, an existing object at the resolved key errors. `size_hint` is disabled
/// whenever the bytes are transformed (compress or encrypt)
#[allow(clippy::too_many_arguments)]
pub async fn put(
    settings: &Settings,
    storage: &DynStorage,
    local_path: Option<&Path>,
    dst: &str,
    overwrite: bool,
    encrypt: bool,
    compress: bool,
    read_stdin: bool,
) -> Result<()> {
    let method = if compress {
        settings.compression
    } else {
        Method::None
    };
    let key = match method.extension() {
        "" => dst.to_string(),
        ext => format!("{dst}.{ext}"),
    };

    if !overwrite
        && storage
            .exists(&key)
            .await
            .with_context(|| format!("check object existence {key}"))?
    {
        bail!("object {key} already exists. To overwrite it, add the -f flag");
    }

    let (source, size): (compression::AsyncReader, Option<u64>) = if read_stdin {
        (Box::pin(io::stdin()), None)
    } else {
        let local = local_path.ok_or_else(|| anyhow!("missing local file path"))?;
        let meta = tokio::fs::metadata(local)
            .await
            .with_context(|| format!("stat the local file {}", local.display()))?;
        if meta.is_dir() {
            bail!(
                "provided local path ({}) points to a directory, exiting",
                local.display()
            );
        }
        let file = tokio::fs::File::open(local)
            .await
            .with_context(|| format!("open the local file {}", local.display()))?;
        (Box::pin(file), Some(meta.len()))
    };

    let compressed = compression::encode(method, source, settings.compression_level);
    let body = if encrypt {
        settings.encrypt(compressed)
    } else {
        compressed
    };

    // length only known when bytes pass through untransformed
    let size_hint = if compress || encrypt { None } else { size };

    storage
        .put(&key, body, size_hint)
        .await
        .with_context(|| format!("upload {key}"))?;
    tracing::info!("uploaded {key}");
    Ok(())
}

/// `st rm`: delete every object under a prefix, or matching a glob pattern.
/// Erroring on an empty match set mirrors wal-g `HandleRemove`
pub async fn rm(storage: &DynStorage, prefix: &str, glob: bool) -> Result<()> {
    let all;
    let keys: Vec<&str> = if glob {
        // wal-g globs objects and folders, then prefix-deletes each match
        all = list_keys(storage, "").await?;
        let mut del = glob_objects(&all, prefix)?;
        for folder in glob_folders(&all, prefix)? {
            del.extend(
                all.iter()
                    .filter(|k| k.starts_with(&folder))
                    .map(String::as_str),
            );
        }
        // object matches & folder members never overlap, sort+dedup is insurance
        del.sort_unstable();
        del.dedup();
        del
    } else {
        all = list_keys(storage, prefix).await?;
        all.iter().map(String::as_str).collect()
    };
    if keys.is_empty() {
        bail!("object or folder {prefix:?} does not exist");
    }
    for key in &keys {
        storage
            .delete(key)
            .await
            .with_context(|| format!("delete {key}"))?;
    }
    tracing::info!("removed {} objects", keys.len());
    Ok(())
}

/// `st ls`: print a tab-separated table of objects under a path. Recursive
/// lists every object; non-recursive lists immediate objects plus synthesized
/// directory rows. `glob` lists keys matching a pattern
pub async fn ls(
    storage: &DynStorage,
    path: Option<&str>,
    recursive: bool,
    glob: bool,
) -> Result<()> {
    let out = if glob {
        // wal-g lists each folder matched by the pattern under its own header
        let folders = glob_folders(&list_keys(storage, "").await?, path.unwrap_or(""))?;
        let mut out = String::new();
        for folder in folders {
            out.push_str(&format!("{folder}:\n"));
            out.push_str(&listing_table(storage, &folder, recursive).await?);
            out.push('\n');
        }
        out
    } else {
        listing_table(storage, path.unwrap_or(""), recursive).await?
    };

    let mut stdout = io::stdout();
    stdout.write_all(out.as_bytes()).await?;
    stdout.flush().await?;
    Ok(())
}

/// Render a `ls` table (header + rows) for one folder. Recursive lists every
/// object; non-recursive lists immediate objects plus synthesized directory rows
async fn listing_table(storage: &DynStorage, prefix: &str, recursive: bool) -> Result<String> {
    let mut metas = list_metas(storage, prefix).await?;
    let mut out = String::from("type\tsize\tlast modified\tname\n");
    if recursive {
        metas.sort_by(|a, b| a.key.cmp(&b.key));
        for m in metas {
            let name = strip_prefix(prefix, &m.key);
            let modified = m.last_modified.map(|t| t.to_rfc3339()).unwrap_or_default();
            out.push_str(&format!("obj\t{}\t{}\t{}\n", m.size, modified, name));
        }
    } else {
        let (dirs, objs) = split_one_level(prefix, &metas);
        for d in dirs {
            out.push_str(&format!("dir\t0\t\t{d}\n"));
        }
        for o in objs {
            let modified = o.last_modified.map(|t| t.to_rfc3339()).unwrap_or_default();
            out.push_str(&format!("obj\t{}\t{}\t{}\n", o.size, modified, o.name));
        }
    }
    Ok(out)
}

/// `st copy`: stream every object under `prefix` from one storage to another.
/// Unlike wal-g (config-file endpoints, multistorage) `from`/`to` are walrus
/// storage URIs (`s3://`, `gs://`, `file://`, bare path); empty `from` is the
/// configured storage. `decrypt_source`/`encrypt_target` re-pipe bytes through
/// the configured crypter; size_hint drops whenever bytes are transformed
pub async fn copy(
    settings: &Settings,
    from: &str,
    to: &str,
    prefix: &str,
    decrypt_source: bool,
    encrypt_target: bool,
) -> Result<()> {
    if to.is_empty() {
        bail!("st copy requires --to <uri>");
    }
    let src = if from.is_empty() {
        settings.build_storage()?
    } else {
        settings.build_dst_storage(from)?
    };
    let dst = settings.build_dst_storage(to)?;

    let metas = list_metas(&src, prefix).await?;
    let transform = decrypt_source || encrypt_target;
    for m in &metas {
        let body = src
            .get(&m.key)
            .await
            .with_context(|| format!("download {}", m.key))?;
        let body = if decrypt_source {
            settings.decrypt(body)
        } else {
            body
        };
        let body = if encrypt_target {
            settings.encrypt(body)
        } else {
            body
        };
        let size_hint = if transform { None } else { Some(m.size) };
        dst.put(&m.key, body, size_hint)
            .await
            .with_context(|| format!("upload {}", m.key))?;
    }
    tracing::info!("copied {} objects", metas.len());
    Ok(())
}

/// Collect object keys under a prefix (recursive, keys include the prefix)
async fn list_keys(storage: &DynStorage, prefix: &str) -> Result<Vec<String>> {
    Ok(list_metas(storage, prefix)
        .await?
        .into_iter()
        .map(|m| m.key)
        .collect())
}

/// Collect object metadata under a prefix (recursive)
async fn list_metas(storage: &DynStorage, prefix: &str) -> Result<Vec<super::ObjectMeta>> {
    let mut stream = storage
        .list(prefix)
        .await
        .with_context(|| format!("list {prefix:?}"))?;
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        out.push(item.with_context(|| format!("list {prefix:?}"))?);
    }
    Ok(out)
}

/// Pattern segments with a leading slash dropped, plus whether a trailing slash
/// (folder-only address) was present. Mirrors wal-g `Glob` splitting the pattern
/// and consuming it segment by segment
fn pattern_segments(pattern: &str) -> (Vec<&str>, bool) {
    let mut parts: Vec<&str> = pattern.trim_start_matches('/').split('/').collect();
    let trailing_slash = matches!(parts.last(), Some(&"")) && parts.len() > 1;
    if trailing_slash {
        parts.pop();
    }
    (parts, trailing_slash)
}

/// Compile each pattern segment; a segment glob never spans `/` since keys are
/// matched segment-aligned
fn compile_segments(segs: &[&str]) -> Result<Vec<Pattern>> {
    segs.iter()
        .map(|s| Pattern::new(s).with_context(|| format!("bad glob pattern segment {s:?}")))
        .collect()
}

/// wal-g object glob: a key matches when its segment count equals the pattern's
/// and every segment matches. Objects resolve only at the final segment, so `*`
/// never spans `/`. A trailing-slash pattern addresses folders, never objects
fn glob_objects<'a>(keys: &'a [String], pattern: &str) -> Result<Vec<&'a str>> {
    let (segs, trailing_slash) = pattern_segments(pattern);
    if trailing_slash {
        return Ok(Vec::new());
    }
    let pats = compile_segments(&segs)?;
    let mut out = Vec::new();
    for k in keys {
        let ks: Vec<&str> = k.split('/').collect();
        if ks.len() == pats.len() && pats.iter().zip(&ks).all(|(p, s)| p.matches(s)) {
            out.push(k.as_str());
        }
    }
    Ok(out)
}

/// wal-g folder glob: distinct prefixes at the pattern's segment depth whose
/// every segment matches and that actually contain nested keys (real
/// subfolders). Returned with a trailing slash, as wal-g folder paths
fn glob_folders(keys: &[String], pattern: &str) -> Result<Vec<String>> {
    let (segs, _) = pattern_segments(pattern);
    let depth = segs.len();
    let pats = compile_segments(&segs)?;
    let mut out: Vec<&str> = Vec::new();
    for k in keys {
        let ks: Vec<&str> = k.split('/').collect();
        if ks.len() > depth && pats.iter().zip(&ks).all(|(p, s)| p.matches(s)) {
            // first `depth` segments of k, borrowed; only survivors allocate below
            let end = ks[..depth].iter().map(|s| s.len()).sum::<usize>() + depth - 1;
            out.push(&k[..end]);
        }
    }
    out.sort_unstable();
    out.dedup();
    Ok(out.into_iter().map(|p| format!("{p}/")).collect())
}

/// Strip a list prefix from a key, yielding the path relative to the listed
/// folder. Leaves the key unchanged when it doesn't carry the prefix
fn strip_prefix<'a>(prefix: &str, key: &'a str) -> &'a str {
    let p = prefix.trim_end_matches('/');
    if p.is_empty() {
        return key;
    }
    match key.strip_prefix(p) {
        Some(rest) => rest.trim_start_matches('/'),
        None => key,
    }
}

/// An object visible at one level below `prefix`
struct LevelObject {
    name: String,
    size: u64,
    last_modified: Option<chrono::DateTime<chrono::Utc>>,
}

/// Split a recursive listing into immediate objects and synthesized directory
/// names for a non-recursive `ls`. A relative key with no `/` is an object at
/// this level; one with a `/` contributes its first segment as a `<segment>/`
/// directory. Directories are de-duplicated & sorted; mirrors wal-g
/// `folder.ListFolder` returning objects + subfolders
fn split_one_level(prefix: &str, metas: &[super::ObjectMeta]) -> (Vec<String>, Vec<LevelObject>) {
    let mut dirs: Vec<&str> = Vec::new();
    let mut objs = Vec::new();
    for m in metas {
        let rel = strip_prefix(prefix, &m.key);
        match rel.split_once('/') {
            Some((seg, _)) if !seg.is_empty() => dirs.push(seg),
            _ => objs.push(LevelObject {
                name: rel.to_string(),
                size: m.size,
                last_modified: m.last_modified,
            }),
        }
    }
    dirs.sort_unstable();
    dirs.dedup();
    objs.sort_by(|a, b| a.name.cmp(&b.name));
    (dirs.into_iter().map(|d| format!("{d}/")).collect(), objs)
}

/// State of a `get` destination path on disk, factoring wal-g
/// `getTargetFilePath` into a pure decision
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DstState {
    Missing,
    Dir,
    File,
}

fn dst_state(dst: &Path) -> DstState {
    match std::fs::metadata(dst) {
        Err(_) => DstState::Missing,
        Ok(m) if m.is_dir() => DstState::Dir,
        Ok(_) => DstState::File,
    }
}

/// Resolve a `get` destination: missing path used verbatim, a directory gets
/// the object basename appended, an existing file is the target itself
fn resolve_target(dst: &Path, state: DstState, basename: &str) -> std::path::PathBuf {
    match state {
        DstState::Dir => dst.join(basename),
        DstState::Missing | DstState::File => dst.to_path_buf(),
    }
}

fn target_path(dst: &Path, basename: &str) -> Result<std::path::PathBuf> {
    Ok(resolve_target(dst, dst_state(dst), basename))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn meta(key: &str, size: u64) -> super::super::ObjectMeta {
        super::super::ObjectMeta {
            key: key.to_string(),
            size,
            last_modified: None,
        }
    }

    #[test]
    fn extension_and_basename() {
        assert_eq!(basename("a/b/c.lz4"), "c.lz4");
        assert_eq!(basename("noslash"), "noslash");
        assert_eq!(extension("wal_005/seg.lz4"), ".lz4");
        assert_eq!(extension("wal_005/seg"), "");
        assert_eq!(extension("archive/base.tar.zst"), ".zst");
        // leading-dot file has no extension (dotfile, not an ext)
        assert_eq!(extension("dir/.hidden"), "");
    }

    #[test]
    fn target_resolution_states() {
        let dst = Path::new("/tmp/out");
        assert_eq!(
            resolve_target(dst, DstState::Missing, "seg.lz4"),
            PathBuf::from("/tmp/out")
        );
        assert_eq!(
            resolve_target(dst, DstState::File, "seg.lz4"),
            PathBuf::from("/tmp/out")
        );
        assert_eq!(
            resolve_target(dst, DstState::Dir, "seg.lz4"),
            PathBuf::from("/tmp/out/seg.lz4")
        );
    }

    #[test]
    fn target_path_against_tempdir() {
        let dir = tempfile::tempdir().unwrap();
        // existing dir -> basename appended
        let into_dir = target_path(dir.path(), "obj.bin").unwrap();
        assert_eq!(into_dir, dir.path().join("obj.bin"));
        // missing path -> verbatim
        let missing = dir.path().join("does-not-exist");
        assert_eq!(target_path(&missing, "obj.bin").unwrap(), missing);
        // existing file -> verbatim (overwrites that exact name)
        let file = dir.path().join("afile");
        std::fs::write(&file, b"x").unwrap();
        assert_eq!(target_path(&file, "obj.bin").unwrap(), file);
    }

    #[test]
    fn strip_prefix_relative_names() {
        assert_eq!(strip_prefix("", "a/b/c"), "a/b/c");
        assert_eq!(strip_prefix("a", "a/b/c"), "b/c");
        assert_eq!(strip_prefix("a/", "a/b/c"), "b/c");
        // key outside the prefix is left intact
        assert_eq!(strip_prefix("z", "a/b"), "a/b");
    }

    #[test]
    fn split_one_level_dirs_and_objects() {
        let metas = [
            meta("p/file1", 10),
            meta("p/file2", 20),
            meta("p/sub/deep", 30),
            meta("p/sub/deeper/x", 40),
            meta("p/other/y", 50),
        ];
        let (dirs, objs) = split_one_level("p", &metas);
        assert_eq!(dirs, vec!["other/".to_string(), "sub/".to_string()]);
        let names: Vec<_> = objs.iter().map(|o| o.name.clone()).collect();
        assert_eq!(names, vec!["file1".to_string(), "file2".to_string()]);
        assert_eq!(objs[0].size, 10);
        assert_eq!(objs[1].size, 20);
    }

    #[test]
    fn split_one_level_empty_prefix() {
        let metas = [meta("top", 1), meta("dir/inner", 2)];
        let (dirs, objs) = split_one_level("", &metas);
        assert_eq!(dirs, vec!["dir/".to_string()]);
        assert_eq!(objs.len(), 1);
        assert_eq!(objs[0].name, "top");
    }

    fn keys() -> Vec<String> {
        [
            "wal_005/000000010000000000000001.lz4",
            "wal_005/000000010000000000000002.lz4",
            "basebackups_005/base_000/metadata.json",
            "wal_005/history.history",
            "top.lz4",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }

    #[test]
    fn glob_objects_are_segment_aligned() {
        let k = keys();
        // one folder + final-segment glob
        let mut m = glob_objects(&k, "wal_005/*.lz4").unwrap();
        m.sort();
        assert_eq!(
            m,
            vec![
                "wal_005/000000010000000000000001.lz4",
                "wal_005/000000010000000000000002.lz4"
            ]
        );
        // top-level glob does NOT span `/`, unlike a flat matcher: only the
        // single-segment key matches, nested `*.lz4` keys do not
        assert_eq!(glob_objects(&k, "*.lz4").unwrap(), vec!["top.lz4"]);
        assert_eq!(glob_objects(&k, "*").unwrap(), vec!["top.lz4"]);
        // a two-segment pattern can't match the one-segment key
        assert!(
            !glob_objects(&k, "wal_005/*.lz4")
                .unwrap()
                .contains(&"top.lz4")
        );
        // trailing slash addresses folders, never objects
        assert!(glob_objects(&k, "wal_005/").unwrap().is_empty());
    }

    #[test]
    fn glob_folders_match_real_subfolders() {
        let k = keys();
        assert_eq!(glob_folders(&k, "wal_005").unwrap(), vec!["wal_005/"]);
        // depth-1 wildcard yields every top-level folder, not objects
        let mut all = glob_folders(&k, "*").unwrap();
        all.sort();
        assert_eq!(all, vec!["basebackups_005/", "wal_005/"]);
        // nested folder match
        assert_eq!(
            glob_folders(&k, "basebackups_005/base_*").unwrap(),
            vec!["basebackups_005/base_000/"]
        );
    }

    #[test]
    fn pattern_segments_trailing_slash() {
        assert_eq!(pattern_segments("a/b"), (vec!["a", "b"], false));
        assert_eq!(pattern_segments("a/b/"), (vec!["a", "b"], true));
        assert_eq!(pattern_segments("/a"), (vec!["a"], false));
    }
}
