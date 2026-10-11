//! Object stores the segment log can live in: an S3-compatible service, or a local directory.
//!
//! The log needs three operations: a create-only PUT (fails if the key exists; this is what fencing
//! rests on), GET, and LIST of a prefix in lexicographic order.

use anyhow::{Context, Result};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

pub enum PutOutcome {
    Created,
    Exists,
}

pub trait Store: Send + Sync {
    /// Create `key` with `body` unless it already exists (then `Exists`, and nothing is written).
    fn put_new(&self, key: &str, body: &[u8]) -> Result<PutOutcome>;
    fn get(&self, key: &str) -> Result<Vec<u8>>;
    /// Bytes `[off, off + len)` of `key` (fewer if the object is shorter).
    fn get_range(&self, key: &str, off: u64, len: u64) -> Result<Vec<u8>>;
    /// All keys starting with `prefix`, sorted.
    fn list(&self, prefix: &str) -> Result<Vec<String>>;
}

/// Objects as files under a root directory (key `a/b/c` is file `<root>/a/b/c`). Useful for local
/// development and tests, or a second local disk / NAS mount as the durable copy.
///
/// Create-only PUT: write a temporary file, fsync it, then `link(2)` it to the key's path. `link`
/// fails with EEXIST if the key exists, which is atomic on local filesystems (and on NFS it is the
/// classic lock-file primitive). The parent directory is fsync'd so the new name is durable.
pub struct LocalDir {
    root: PathBuf,
    tmp_seq: AtomicU64,
}

impl LocalDir {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root).with_context(|| format!("create {}", root.display()))?;
        Ok(LocalDir { root, tmp_seq: AtomicU64::new(0) })
    }

    fn path(&self, key: &str) -> Result<PathBuf> {
        anyhow::ensure!(
            !key.is_empty() && !key.starts_with('/') && !key.split('/').any(|c| c.is_empty() || c == "." || c == ".."),
            "invalid object key {key:?}"
        );
        Ok(self.root.join(key))
    }
}

fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

impl Store for LocalDir {
    fn put_new(&self, key: &str, body: &[u8]) -> Result<PutOutcome> {
        let path = self.path(key)?;
        let dir = path.parent().expect("key has a parent under root");
        fs::create_dir_all(dir)?;
        let tmp = dir.join(format!(".tmp.{}.{}", std::process::id(), self.tmp_seq.fetch_add(1, Relaxed)));
        let mut f = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
        let res =
            f.write_all(body).and_then(|_| f.sync_all()).map_err(anyhow::Error::from).and_then(|_| match fs::hard_link(&tmp, &path) {
                Ok(()) => Ok(PutOutcome::Created),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(PutOutcome::Exists),
                Err(e) => Err(e.into()),
            });
        let _ = fs::remove_file(&tmp);
        if let Ok(PutOutcome::Created) = res {
            sync_dir(dir)?;
        }
        res
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        let p = self.path(key)?;
        fs::read(&p).with_context(|| format!("read {}", p.display()))
    }

    fn get_range(&self, key: &str, off: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let p = self.path(key)?;
        let f = File::open(&p).with_context(|| format!("open {}", p.display()))?;
        let size = f.metadata()?.len();
        let n = len.min(size.saturating_sub(off)) as usize;
        let mut buf = vec![0u8; n];
        f.read_exact_at(&mut buf, off)?;
        Ok(buf)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        // walk from the deepest directory the prefix names, then filter by the full prefix
        let base = match prefix.rfind('/') {
            Some(i) => &prefix[..i],
            None => "",
        };
        let start = if base.is_empty() { self.root.clone() } else { self.path(base)? };
        let mut out = Vec::new();
        let mut stack = vec![start];
        while let Some(dir) = stack.pop() {
            let entries = match fs::read_dir(&dir) {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            for entry in entries {
                let entry = entry?;
                let name = entry.file_name();
                if name.to_string_lossy().starts_with(".tmp.") {
                    continue;
                }
                let path = entry.path();
                if entry.file_type()?.is_dir() {
                    stack.push(path);
                } else if let Ok(rel) = path.strip_prefix(&self.root) {
                    let key = rel.to_string_lossy().into_owned();
                    if key.starts_with(prefix) {
                        out.push(key);
                    }
                }
            }
        }
        out.sort();
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_root(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("blockgw-store-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&p);
        p
    }

    #[test]
    fn create_only_put() {
        let root = tmp_root("put");
        let s = LocalDir::new(&root).unwrap();
        assert!(matches!(s.put_new("d/seg/0001/0000", b"one").unwrap(), PutOutcome::Created));
        assert!(matches!(s.put_new("d/seg/0001/0000", b"two").unwrap(), PutOutcome::Exists));
        assert_eq!(s.get("d/seg/0001/0000").unwrap(), b"one", "the losing writer changed nothing");
        assert_eq!(s.get_range("d/seg/0001/0000", 1, 1).unwrap(), b"n");
        assert_eq!(s.get_range("d/seg/0001/0000", 2, 100).unwrap(), b"e", "clamped at the end");
        assert!(s.get("d/seg/0001/0001").is_err());
        // no temporary files are left behind
        assert_eq!(fs::read_dir(root.join("d/seg/0001")).unwrap().count(), 1);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn list_is_sorted_and_prefix_filtered() {
        let root = tmp_root("list");
        let s = LocalDir::new(&root).unwrap();
        for k in ["p/seg/0002/0000", "p/seg/0001/0001", "p/seg/0001/0000", "p/epoch/0001", "q/seg/0001/0000"] {
            s.put_new(k, b"x").unwrap();
        }
        assert_eq!(s.list("p/seg/").unwrap(), vec!["p/seg/0001/0000", "p/seg/0001/0001", "p/seg/0002/0000"]);
        assert_eq!(s.list("p/seg/0001/").unwrap(), vec!["p/seg/0001/0000", "p/seg/0001/0001"]);
        assert_eq!(s.list("p/epoch/").unwrap(), vec!["p/epoch/0001"]);
        assert!(s.list("missing/").unwrap().is_empty());
        assert_eq!(s.list("p/seg/0001/000").unwrap().len(), 2, "partial last component");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_keys_that_escape_the_root() {
        let s = LocalDir::new(tmp_root("keys")).unwrap();
        for k in ["", "/abs", "a/../b", "a//b", "./a", "a/."] {
            assert!(s.put_new(k, b"x").is_err(), "{k:?}");
        }
    }

    #[test]
    fn concurrent_creators_have_exactly_one_winner() {
        let root = tmp_root("race");
        let s = std::sync::Arc::new(LocalDir::new(&root).unwrap());
        let wins: usize = std::thread::scope(|sc| {
            let hs: Vec<_> = (0..16)
                .map(|i| {
                    let s = s.clone();
                    sc.spawn(move || matches!(s.put_new("e/epoch/0001", format!("{i}").as_bytes()).unwrap(), PutOutcome::Created))
                })
                .collect();
            hs.into_iter().map(|h| h.join().unwrap() as usize).sum()
        });
        assert_eq!(wins, 1);
        fs::remove_dir_all(root).unwrap();
    }
}
