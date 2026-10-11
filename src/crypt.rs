//! Encryption at rest: a [`Store`] wrapper that seals every object with a per-disk key.
//!
//! ```text
//! object: "BGE1" | nonce prefix (16 random bytes) | plaintext length u64 | chunk 0 | chunk 1 | ...
//! chunk i: XChaCha20-Poly1305(plaintext[i * 64 KiB ..][..64 KiB]) + 16-byte tag
//!   nonce = nonce prefix | i (u64 LE);  associated data = object key | 28-byte header
//! ```
//!
//! * Every object (segments, checkpoints, epoch markers, tombstones) goes through it, so the log
//!   format above it is unchanged and compression still happens before encryption.
//! * A ranged read fetches only the chunks it needs. The header of each object is cached after the
//!   first read; segment reads always start with the meta at offset 0, so they never pay an extra
//!   request for it.
//! * The key name and the header are authenticated: an object copied to another key (another
//!   position in the log, or another disk), a chunk moved within an object, a truncated object or
//!   one encrypted under another key all fail to decrypt.
//! * Listings report plaintext sizes, so size-based checks (tombstones) work unchanged.
//! * Fail closed: with a key configured, an object that is not encrypted is an error, never
//!   accepted as plaintext.
//! * Key rotation: objects are always written with the current key; old keys (decrypt only) are
//!   tried in turn when the current one fails, and the key that worked is remembered per object.
//!   `Dev::rekey` rewrites everything still sealed with an old key, after which the old key can go.

use crate::store::{PutOutcome, Store};
use anyhow::{bail, ensure, Context, Result};
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub const MAGIC: &[u8; 4] = b"BGE1";
const HEADER: usize = 28;
const CHUNK: u64 = 64 << 10;
const TAG: u64 = 16;
const HEADER_CACHE: usize = 65536;

/// Does this object look like one written by [`CryptStore`]? (for clear errors without a key)
pub fn looks_encrypted(obj: &[u8]) -> bool {
    obj.len() >= HEADER && &obj[..4] == MAGIC
}

/// Read a 32-byte key: 64 hex digits (surrounding whitespace ignored) or 32 raw bytes. The file
/// must not be readable by group or others.
pub fn load_key(path: &str) -> Result<[u8; 32]> {
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(path).with_context(|| format!("key file {path}"))?.permissions().mode();
    ensure!(mode & 0o077 == 0, "key file {path} is accessible by group or others (mode {:o}); chmod 600 it", mode & 0o777);
    let raw = std::fs::read(path).with_context(|| format!("key file {path}"))?;
    let text = String::from_utf8_lossy(&raw);
    let hex = text.trim();
    let mut key = [0u8; 32];
    if hex.len() == 64 && hex.bytes().all(|c| c.is_ascii_hexdigit()) {
        for (i, k) in key.iter_mut().enumerate() {
            *k = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16)?;
        }
    } else if raw.len() == 32 {
        key.copy_from_slice(&raw);
    } else {
        bail!("key file {path}: expected 64 hex digits or 32 raw bytes (e.g. `head -c 32 /dev/urandom > {path}`)");
    }
    Ok(key)
}

fn chunks_for(plain: u64) -> u64 {
    plain.div_ceil(CHUNK).max(1) // an empty object still has one (empty, authenticated) chunk
}

/// Stored size of a `plain`-byte object.
pub fn sealed_len(plain: u64) -> u64 {
    HEADER as u64 + plain + TAG * chunks_for(plain)
}

/// Plaintext size of a stored object of `size` bytes (inverse of [`sealed_len`]).
fn plain_len(size: u64) -> u64 {
    if size < HEADER as u64 + TAG {
        return size; // not ours: reported as is, and reading it fails
    }
    let n = (size - HEADER as u64).div_ceil(CHUNK + TAG).max(1);
    size - HEADER as u64 - TAG * n
}

pub struct CryptStore {
    inner: Arc<dyn Store>,
    /// [current key, old keys...]: writes use the first, reads try them in order
    aeads: Vec<XChaCha20Poly1305>,
    /// per object: header and the index of the key that opened it
    headers: Mutex<HashMap<String, ([u8; HEADER], usize)>>,
}

impl CryptStore {
    #[cfg(test)]
    pub fn new(inner: Arc<dyn Store>, key: &[u8; 32]) -> Self {
        Self::with_old_keys(inner, key, &[])
    }

    pub fn with_old_keys(inner: Arc<dyn Store>, key: &[u8; 32], old: &[[u8; 32]]) -> Self {
        let aeads = std::iter::once(key).chain(old).map(|k| XChaCha20Poly1305::new(k.into())).collect();
        CryptStore { inner, aeads, headers: Mutex::new(HashMap::new()) }
    }

    fn nonce(hdr: &[u8; HEADER], i: u64) -> XNonce {
        let mut n = [0u8; 24];
        n[..16].copy_from_slice(&hdr[4..20]);
        n[16..].copy_from_slice(&i.to_le_bytes());
        n.into()
    }

    fn aad(key: &str, hdr: &[u8; HEADER]) -> Vec<u8> {
        let mut a = Vec::with_capacity(key.len() + HEADER);
        a.extend_from_slice(key.as_bytes());
        a.extend_from_slice(hdr);
        a
    }

    fn seal(&self, key: &str, body: &[u8]) -> Result<Vec<u8>> {
        let mut hdr = [0u8; HEADER];
        hdr[..4].copy_from_slice(MAGIC);
        getrandom::getrandom(&mut hdr[4..20]).map_err(|e| anyhow::anyhow!("random nonce: {e}"))?;
        hdr[20..].copy_from_slice(&(body.len() as u64).to_le_bytes());
        let aad = Self::aad(key, &hdr);
        let mut out = Vec::with_capacity(sealed_len(body.len() as u64) as usize);
        out.extend_from_slice(&hdr);
        for i in 0..chunks_for(body.len() as u64) {
            let s = (i * CHUNK) as usize;
            let e = (s + CHUNK as usize).min(body.len());
            let ct = self.aeads[0]
                .encrypt(&Self::nonce(&hdr, i), Payload { msg: &body[s..e], aad: &aad })
                .map_err(|_| anyhow::anyhow!("encrypt"))?;
            out.extend_from_slice(&ct);
        }
        Ok(out)
    }

    fn parse_header(key: &str, b: &[u8]) -> Result<[u8; HEADER]> {
        ensure!(
            b.len() >= HEADER && &b[..4] == MAGIC,
            "{key}: object is not encrypted, but this disk has a key (refusing to read it as plaintext)"
        );
        Ok(b[..HEADER].try_into().unwrap())
    }

    fn remember(&self, key: &str, hdr: [u8; HEADER], kidx: usize) {
        let mut h = self.headers.lock().unwrap();
        if h.len() >= HEADER_CACHE {
            h.clear();
        }
        h.insert(key.to_string(), (hdr, kidx));
    }

    /// Decrypt consecutive chunks starting at chunk `first`, with key `kidx` if known, else with
    /// the first key that opens the first chunk. Returns the plaintext and the key index.
    fn open_chunks(&self, key: &str, hdr: &[u8; HEADER], first: u64, ct: &[u8], kidx: Option<usize>) -> Result<(Vec<u8>, usize)> {
        let aad = Self::aad(key, hdr);
        let failed = || anyhow::anyhow!("{key}: decryption failed (wrong key, or the object was modified, moved or truncated)");
        let mut chunks = ct.chunks((CHUNK + TAG) as usize);
        let c0 = chunks.next().unwrap_or(&[]);
        let open0 = |a: &XChaCha20Poly1305| a.decrypt(&Self::nonce(hdr, first), Payload { msg: c0, aad: &aad }).ok();
        let (k, mut out) = match kidx {
            Some(k) => (k, open0(&self.aeads[k]).ok_or_else(failed)?),
            None => self.aeads.iter().enumerate().find_map(|(k, a)| open0(a).map(|pt| (k, pt))).ok_or_else(failed)?,
        };
        for (i, c) in chunks.enumerate() {
            let pt = self.aeads[k].decrypt(&Self::nonce(hdr, first + 1 + i as u64), Payload { msg: c, aad: &aad }).map_err(|_| failed())?;
            out.extend_from_slice(&pt);
        }
        Ok((out, k))
    }
}

fn hdr_plain_len(hdr: &[u8; HEADER]) -> u64 {
    u64::from_le_bytes(hdr[20..28].try_into().unwrap())
}

impl Store for CryptStore {
    fn put_new(&self, key: &str, body: &[u8]) -> Result<PutOutcome> {
        let sealed = self.seal(key, body)?;
        self.inner.put_new(key, &sealed)
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        let obj = self.inner.get(key)?;
        let hdr = Self::parse_header(key, &obj)?;
        let plain = hdr_plain_len(&hdr);
        ensure!(obj.len() as u64 == sealed_len(plain), "{key}: encrypted object has the wrong size (truncated?)");
        let kidx = self.headers.lock().unwrap().get(key).map(|(_, k)| *k);
        let (pt, k) = self.open_chunks(key, &hdr, 0, &obj[HEADER..], kidx)?;
        self.remember(key, hdr, k);
        Ok(pt)
    }

    fn get_range(&self, key: &str, off: u64, len: u64) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let cached = self.headers.lock().unwrap().get(key).copied();
        let kidx = cached.map(|(_, k)| k);
        let cached = cached.map(|(h, _)| h);
        let end_hint = cached.map(|h| hdr_plain_len(&h)).unwrap_or(u64::MAX);
        if off >= end_hint {
            return Ok(Vec::new());
        }
        let end = (off + len).min(end_hint);
        let (c0, c1) = (off / CHUNK, (end - 1) / CHUNK);
        let ct_at = |c: u64| HEADER as u64 + c * (CHUNK + TAG);
        let (hdr, ct) = match cached {
            Some(h) => (h, self.inner.get_range(key, ct_at(c0), ct_at(c1 + 1) - ct_at(c0))?),
            None if c0 == 0 => {
                let b = self.inner.get_range(key, 0, ct_at(c1 + 1))?;
                let h = Self::parse_header(key, &b)?;
                (h, b[HEADER..].to_vec())
            }
            None => {
                let h = Self::parse_header(key, &self.inner.get_range(key, 0, HEADER as u64)?)?;
                (h, self.inner.get_range(key, ct_at(c0), ct_at(c1 + 1) - ct_at(c0))?)
            }
        };
        let plain = hdr_plain_len(&hdr);
        if off >= plain {
            return Ok(Vec::new());
        }
        // the last chunk is short; anything else short means a truncated object
        let want_chunks = (plain.min(end).saturating_sub(1)) / CHUNK + 1 - c0;
        let expect = (c0..c0 + want_chunks).map(|c| (plain - c * CHUNK).min(CHUNK) + TAG).sum::<u64>();
        ensure!(ct.len() as u64 >= expect, "{key}: encrypted object truncated");
        let (pt, k) = self.open_chunks(key, &hdr, c0, &ct[..expect as usize], kidx)?;
        self.remember(key, hdr, k);
        let s = (off - c0 * CHUNK) as usize;
        let e = (end.min(plain) - c0 * CHUNK) as usize;
        Ok(pt[s..e].to_vec())
    }

    fn list_sized(&self, prefix: &str) -> Result<Vec<(String, u64)>> {
        Ok(self.inner.list_sized(prefix)?.into_iter().map(|(k, s)| (k, plain_len(s))).collect())
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.headers.lock().unwrap().remove(key);
        self.inner.delete(key)
    }

    fn seal_blob(&self, name: &str, body: Vec<u8>) -> Result<Vec<u8>> {
        self.seal(name, &body)
    }

    fn open_blob(&self, name: &str, blob: Vec<u8>) -> Result<Vec<u8>> {
        let hdr = Self::parse_header(name, &blob)?;
        ensure!(blob.len() as u64 == sealed_len(hdr_plain_len(&hdr)), "{name}: sealed blob has the wrong size");
        Ok(self.open_chunks(name, &hdr, 0, &blob[HEADER..], None)?.0)
    }

    fn needs_old_key(&self, key: &str) -> Result<bool> {
        self.get_range(key, 0, 1)?;
        Ok(self.headers.lock().unwrap().get(key).is_some_and(|(_, k)| *k > 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::LocalDir;

    fn tmp(name: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("blockgw-crypt-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    fn data(n: usize) -> Vec<u8> {
        (0..n).map(|i| (i * 31 + i / 977) as u8).collect()
    }

    #[test]
    fn sizes_round_trip() {
        for p in [0, 1, 100, CHUNK - 1, CHUNK, CHUNK + 1, 3 * CHUNK, 5 * CHUNK + 7] {
            assert_eq!(plain_len(sealed_len(p)), p, "plain {p}");
        }
    }

    #[test]
    fn get_range_and_listing_match_plaintext() {
        let d = tmp("range");
        let inner: Arc<dyn Store> = Arc::new(LocalDir::new(&d).unwrap());
        let st = CryptStore::new(inner.clone(), &[7u8; 32]);
        let body = data(3 * CHUNK as usize + 1234);
        assert!(matches!(st.put_new("p/seg/1", &body).unwrap(), PutOutcome::Created));
        assert!(matches!(st.put_new("p/seg/1", b"other").unwrap(), PutOutcome::Exists));
        st.put_new("p/tomb", b"TOMB").unwrap();
        st.put_new("p/empty", b"").unwrap();
        assert_eq!(st.get("p/seg/1").unwrap(), body);
        assert_eq!(st.get("p/empty").unwrap(), b"");
        let raw = inner.get("p/seg/1").unwrap();
        assert!(looks_encrypted(&raw) && !raw.windows(64).any(|w| w == &body[1000..1064]), "stored bytes are ciphertext");
        let ranges = [
            (0, 10),
            (5, CHUNK),
            (CHUNK - 3, 6),
            (CHUNK, CHUNK),
            (2 * CHUNK + 100, 5000),
            (3 * CHUNK, 4000),
            (10, 10 << 20),
            (body.len() as u64, 5),
        ];
        for (off, len) in ranges {
            for fresh in [true, false] {
                let s = CryptStore::new(inner.clone(), &[7u8; 32]);
                if !fresh {
                    s.get_range("p/seg/1", 0, 1).unwrap(); // header cached
                }
                let got = s.get_range("p/seg/1", off, len).unwrap();
                let e = ((off + len) as usize).min(body.len());
                let want = if (off as usize) < body.len() { &body[off as usize..e] } else { &[][..] };
                assert_eq!(got, want, "range {off}+{len} fresh={fresh}");
            }
        }
        let l = st.list_sized("p/").unwrap();
        assert_eq!(l, vec![("p/empty".into(), 0), ("p/seg/1".into(), body.len() as u64), ("p/tomb".into(), 4)]);
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn old_keys_still_read_and_new_writes_use_the_current_key() {
        let d = tmp("rotate");
        let inner: Arc<dyn Store> = Arc::new(LocalDir::new(&d).unwrap());
        let old = CryptStore::new(inner.clone(), &[1u8; 32]);
        let body = data(CHUNK as usize + 300);
        old.put_new("a", &body).unwrap();
        let rotated = CryptStore::with_old_keys(inner.clone(), &[2u8; 32], &[[1u8; 32]]);
        assert_eq!(rotated.get_range("a", 10, CHUNK).unwrap(), &body[10..10 + CHUNK as usize]);
        assert_eq!(rotated.get("a").unwrap(), body);
        assert!(rotated.needs_old_key("a").unwrap());
        rotated.put_new("b", &body).unwrap();
        assert!(!rotated.needs_old_key("b").unwrap());
        // the new key alone reads b, not a; the old key alone reads a, not b
        let new_only = CryptStore::new(inner.clone(), &[2u8; 32]);
        assert!(new_only.get("b").is_ok() && new_only.get("a").is_err());
        assert!(CryptStore::new(inner.clone(), &[1u8; 32]).get("b").is_err());
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn tampering_and_wrong_keys_fail_closed() {
        let d = tmp("tamper");
        let inner: Arc<dyn Store> = Arc::new(LocalDir::new(&d).unwrap());
        let st = CryptStore::new(inner.clone(), &[1u8; 32]);
        let body = data(2 * CHUNK as usize);
        st.put_new("a", &body).unwrap();
        // wrong key
        assert!(CryptStore::new(inner.clone(), &[2u8; 32]).get("a").is_err());
        // moved to another key
        inner.put_new("b", &inner.get("a").unwrap()).unwrap();
        assert!(st.get("b").is_err(), "an object moved to another key");
        // flipped bit
        let mut raw = inner.get("a").unwrap();
        raw[HEADER + 10] ^= 1;
        inner.put_new("c", &raw).unwrap();
        let s2 = CryptStore::new(inner.clone(), &[1u8; 32]);
        assert!(s2.get("c").is_err() && s2.get_range("c", 0, 100).is_err());
        // swapped chunks
        let raw = inner.get("a").unwrap();
        let k = (CHUNK + TAG) as usize;
        let mut sw = raw[..HEADER].to_vec();
        sw.extend_from_slice(&raw[HEADER + k..HEADER + 2 * k]);
        sw.extend_from_slice(&raw[HEADER..HEADER + k]);
        inner.put_new("d", &sw).unwrap();
        assert!(s2.get("d").is_err(), "swapped chunks");
        // truncated
        inner.put_new("e", &raw[..raw.len() - 1]).unwrap();
        assert!(s2.get("e").is_err());
        assert!(CryptStore::new(inner.clone(), &[1u8; 32]).get_range("e", CHUNK + 5, 100).is_err());
        // plaintext object
        inner.put_new("f", b"BGS2 plaintext segment................").unwrap();
        let e = s2.get("f").unwrap_err().to_string();
        assert!(e.contains("not encrypted"), "{e}");
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn key_file_formats_and_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let d = tmp("key");
        std::fs::create_dir_all(&d).unwrap();
        let p = d.join("k");
        let w = |c: &[u8], mode: u32| {
            let _ = std::fs::remove_file(&p); // the previous key may be read-only
            std::fs::write(&p, c).unwrap();
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(mode)).unwrap();
            load_key(p.to_str().unwrap())
        };
        assert_eq!(w(&[9u8; 32], 0o600).unwrap(), [9u8; 32]);
        assert_eq!(w(format!("{}\n", "ab".repeat(32)).as_bytes(), 0o400).unwrap(), [0xab; 32]);
        assert!(w(&[9u8; 31], 0o600).is_err());
        assert!(w(&[9u8; 32], 0o644).unwrap_err().to_string().contains("chmod 600"));
        let _ = std::fs::remove_dir_all(&d);
    }
}
