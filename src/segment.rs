//! Segment format v2: a self-describing object whose data can be read by byte range.
//!
//! ```text
//! header (40 bytes):
//!   "BGS2" | epoch u64 | seq u64 | n_entries u32 | n_frames u32 | frame_size u32 | meta_len u32 | crc32(meta) u32
//! meta (meta_len bytes):
//!   n_entries × (kind u8, disk_off u64, len u32, data_off u64)       kind 0 = data, 1 = discard
//!   n_frames  × (obj_off u64, obj_len u32, raw_len u32, codec u8, crc32(raw) u32)
//! frames: the data stream (the concatenation of all data entries) cut into `frame_size` pieces,
//!   each stored raw (codec 0) or zstd (codec 1), starting at offset 40 + meta_len
//! ```
//!
//! Entry offsets and lengths are multiples of the 4 KiB block size; `data_off` is the entry's
//! position in the data stream. Frame `i` covers data bytes `[i * frame_size, i * frame_size + raw_len)`.

use anyhow::{bail, ensure, Result};

pub const MAGIC: &[u8; 4] = b"BGS2";
pub const HEADER: usize = 40;
const ENTRY: usize = 21;
const FRAME: usize = 21;
pub const KIND_DATA: u8 = 0;
pub const KIND_DISCARD: u8 = 1;
pub const FRAME_SIZE: u32 = 256 << 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Entry {
    pub kind: u8,
    pub off: u64,
    pub len: u32,
    pub data_off: u64,
}

/// Writes collected since the last seal: the entry table and the data stream.
#[derive(Default, Debug)]
pub struct Builder {
    pub entries: Vec<Entry>,
    pub data: Vec<u8>,
}

impl Builder {
    pub fn push_data(&mut self, off: u64, d: &[u8]) {
        self.entries.push(Entry { kind: KIND_DATA, off, len: d.len() as u32, data_off: self.data.len() as u64 });
        self.data.extend_from_slice(d);
    }
    pub fn push_discard(&mut self, off: u64, len: u32) {
        self.entries.push(Entry { kind: KIND_DISCARD, off, len, data_off: self.data.len() as u64 });
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    pub fn data_len(&self) -> usize {
        self.data.len()
    }
    /// Approximate encoded size, for queue accounting.
    pub fn size(&self) -> usize {
        HEADER + self.entries.len() * ENTRY + self.data.len()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Frame {
    pub obj_off: u64,
    pub obj_len: u32,
    pub raw_len: u32,
    pub codec: u8,
    pub crc: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Meta {
    pub epoch: u64,
    pub seq: u64,
    pub frame_size: u32,
    pub entries: Vec<Entry>,
    pub frames: Vec<Frame>,
}

impl Meta {
    pub fn data_len(&self) -> u64 {
        self.frames.iter().map(|f| f.raw_len as u64).sum()
    }
    /// Frames covering data bytes `[off, off + len)`, as an index range.
    pub fn frames_for(&self, off: u64, len: u64) -> std::ops::Range<usize> {
        if len == 0 || self.frames.is_empty() {
            return 0..0;
        }
        let fs = self.frame_size as u64;
        let first = (off / fs) as usize;
        let last = ((off + len - 1) / fs) as usize;
        first..(last + 1).min(self.frames.len())
    }
}

fn compress_frame(raw: &[u8]) -> (u8, Vec<u8>) {
    // skip zstd when a frame does not shrink by at least 5% (media, model weights, ciphertext)
    match zstd::bulk::compress(raw, 1) {
        Ok(z) if z.len() < raw.len() * 95 / 100 => (1, z),
        _ => (0, raw.to_vec()),
    }
}

/// Encode a sealed segment as an object.
pub fn encode(epoch: u64, seq: u64, b: &Builder) -> Vec<u8> {
    let frames_raw: Vec<&[u8]> = b.data.chunks(FRAME_SIZE as usize).collect();
    let packed: Vec<(u8, Vec<u8>, u32, u32)> = frames_raw
        .iter()
        .map(|raw| {
            let (codec, bytes) = compress_frame(raw);
            (codec, bytes, raw.len() as u32, crc32fast::hash(raw))
        })
        .collect();
    let meta_len = b.entries.len() * ENTRY + packed.len() * FRAME;
    let mut meta = Vec::with_capacity(meta_len);
    for e in &b.entries {
        meta.push(e.kind);
        meta.extend_from_slice(&e.off.to_le_bytes());
        meta.extend_from_slice(&e.len.to_le_bytes());
        meta.extend_from_slice(&e.data_off.to_le_bytes());
    }
    let mut obj_off = (HEADER + meta_len) as u64;
    for (codec, bytes, raw_len, crc) in &packed {
        meta.extend_from_slice(&obj_off.to_le_bytes());
        meta.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        meta.extend_from_slice(&raw_len.to_le_bytes());
        meta.push(*codec);
        meta.extend_from_slice(&crc.to_le_bytes());
        obj_off += bytes.len() as u64;
    }
    let mut out = Vec::with_capacity(obj_off as usize);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&epoch.to_le_bytes());
    out.extend_from_slice(&seq.to_le_bytes());
    out.extend_from_slice(&(b.entries.len() as u32).to_le_bytes());
    out.extend_from_slice(&(packed.len() as u32).to_le_bytes());
    out.extend_from_slice(&FRAME_SIZE.to_le_bytes());
    out.extend_from_slice(&(meta_len as u32).to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(&meta).to_le_bytes());
    out.extend_from_slice(&meta);
    for (_, bytes, _, _) in &packed {
        out.extend_from_slice(bytes);
    }
    out
}

/// Length of header + meta, from the header alone (so a reader knows how much to fetch).
pub fn meta_end(header: &[u8]) -> Result<usize> {
    ensure!(!crate::crypt::looks_encrypted(header), "the object is encrypted, but this disk has no key_file");
    ensure!(header.len() >= HEADER && &header[0..4] == MAGIC, "not a v2 segment (bad magic or short header)");
    Ok(HEADER + u32::from_le_bytes(header[32..36].try_into()?) as usize)
}

/// Parse header + meta (`buf` must hold at least `meta_end` bytes), validating the meta checksum
/// and every offset against the object layout.
pub fn parse_meta(buf: &[u8]) -> Result<Meta> {
    let end = meta_end(buf)?;
    ensure!(buf.len() >= end, "segment meta truncated");
    let u32at = |p: usize| u32::from_le_bytes(buf[p..p + 4].try_into().unwrap());
    let u64at = |p: usize| u64::from_le_bytes(buf[p..p + 8].try_into().unwrap());
    let (epoch, seq) = (u64at(4), u64at(12));
    let (n, m, frame_size, meta_len, crc) = (u32at(20) as usize, u32at(24) as usize, u32at(28), u32at(32) as usize, u32at(36));
    let meta = &buf[HEADER..end];
    ensure!(crc32fast::hash(meta) == crc, "segment meta checksum mismatch");
    ensure!(n.checked_mul(ENTRY).and_then(|a| a.checked_add(m * FRAME)) == Some(meta_len), "segment meta length mismatch");
    ensure!(frame_size > 0, "segment frame size is 0");
    let mut entries = Vec::with_capacity(n);
    for i in 0..n {
        let p = HEADER + i * ENTRY;
        let e = Entry { kind: buf[p], off: u64at(p + 1), len: u32at(p + 9), data_off: u64at(p + 13) };
        ensure!(e.kind == KIND_DATA || e.kind == KIND_DISCARD, "entry {i}: unknown kind {}", e.kind);
        entries.push(e);
    }
    let mut frames = Vec::with_capacity(m);
    let mut expect_off = end as u64;
    for i in 0..m {
        let p = HEADER + n * ENTRY + i * FRAME;
        let f = Frame { obj_off: u64at(p), obj_len: u32at(p + 8), raw_len: u32at(p + 12), codec: buf[p + 16], crc: u32at(p + 17) };
        ensure!(f.obj_off == expect_off, "frame {i}: not contiguous");
        ensure!(f.codec <= 1, "frame {i}: unknown codec {}", f.codec);
        ensure!(f.raw_len <= frame_size && (i + 1 == m || f.raw_len == frame_size), "frame {i}: bad raw length");
        expect_off += f.obj_len as u64;
        frames.push(f);
    }
    let meta = Meta { epoch, seq, frame_size, entries, frames };
    let dlen = meta.data_len();
    for (i, e) in meta.entries.iter().enumerate() {
        if e.kind == KIND_DATA {
            ensure!(e.data_off + e.len as u64 <= dlen, "entry {i}: data beyond the data stream");
        }
    }
    Ok(meta)
}

/// Decode one frame's bytes (as stored) and verify its checksum.
pub fn decode_frame(f: &Frame, bytes: &[u8]) -> Result<Vec<u8>> {
    ensure!(bytes.len() == f.obj_len as usize, "frame length mismatch");
    let raw = match f.codec {
        0 => bytes.to_vec(),
        1 => zstd::bulk::decompress(bytes, f.raw_len as usize)?,
        c => bail!("unknown codec {c}"),
    };
    ensure!(raw.len() == f.raw_len as usize && crc32fast::hash(&raw) == f.crc, "frame checksum mismatch");
    Ok(raw)
}

/// Object byte range holding frames `r` (for a ranged GET).
pub fn frames_span(meta: &Meta, r: &std::ops::Range<usize>) -> (u64, u64) {
    let first = &meta.frames[r.start];
    let last = &meta.frames[r.end - 1];
    (first.obj_off, last.obj_off + last.obj_len as u64 - first.obj_off)
}

/// Data bytes `[off, off + len)` given the object bytes of frames `r` (as returned by a ranged GET
/// of `frames_span`).
pub fn read_data(meta: &Meta, r: &std::ops::Range<usize>, span: &[u8], off: u64, len: u64) -> Result<Vec<u8>> {
    let base = meta.frames[r.start].obj_off;
    let mut raw = Vec::with_capacity(r.len() * meta.frame_size as usize);
    for f in &meta.frames[r.clone()] {
        let s = (f.obj_off - base) as usize;
        raw.extend_from_slice(&decode_frame(f, &span[s..s + f.obj_len as usize])?);
    }
    let start = (off - r.start as u64 * meta.frame_size as u64) as usize;
    ensure!(start + len as usize <= raw.len(), "requested data beyond the fetched frames");
    Ok(raw[start..start + len as usize].to_vec())
}

/// Decode a whole object: its meta and the full data stream.
pub fn decode_all(obj: &[u8]) -> Result<(Meta, Vec<u8>)> {
    let meta = parse_meta(obj)?;
    let r = 0..meta.frames.len();
    let data = if r.is_empty() {
        Vec::new()
    } else {
        let (s, l) = frames_span(&meta, &r);
        ensure!((s + l) as usize == obj.len(), "segment has trailing or missing bytes");
        read_data(&meta, &r, &obj[s as usize..], 0, meta.data_len())?
    };
    Ok((meta, data))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn noise(n: usize, seed: u64) -> Vec<u8> {
        let mut x = seed | 1;
        (0..n)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect()
    }

    fn sample() -> Builder {
        let mut b = Builder::default();
        b.push_data(0, &vec![7u8; 600 << 10]); // compressible, spans 3 frames
        b.push_discard(1 << 30, 64 << 10);
        b.push_data(8 << 20, &noise(300 << 10, 3)); // incompressible
        b.push_data(4096, &[1u8; 4096]);
        b
    }

    #[test]
    fn round_trip() {
        let b = sample();
        let obj = encode(5, 9, &b);
        let (meta, data) = decode_all(&obj).unwrap();
        assert_eq!((meta.epoch, meta.seq), (5, 9));
        assert_eq!(meta.entries, b.entries);
        assert_eq!(data, b.data);
        assert_eq!(meta.frames.len(), b.data.len().div_ceil(FRAME_SIZE as usize));
        assert!(meta.frames.iter().any(|f| f.codec == 1) && meta.frames.iter().any(|f| f.codec == 0));
        assert!(obj.len() < b.size(), "compressible frames shrink the object");
    }

    #[test]
    fn ranged_reads_match_the_data_stream() {
        let b = sample();
        let obj = encode(1, 1, &b);
        let meta = parse_meta(&obj[..meta_end(&obj).unwrap()]).unwrap();
        let mut x = 12345u64;
        for _ in 0..200 {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let off = (x >> 16) % b.data.len() as u64;
            let len = 1 + (x >> 40) % (b.data.len() as u64 - off).min(700 << 10);
            let r = meta.frames_for(off, len);
            let (s, l) = frames_span(&meta, &r);
            let got = read_data(&meta, &r, &obj[s as usize..(s + l) as usize], off, len).unwrap();
            assert_eq!(got, &b.data[off as usize..(off + len) as usize], "off={off} len={len}");
        }
    }

    #[test]
    fn empty_and_discard_only_segments() {
        let mut b = Builder::default();
        b.push_discard(0, 4096);
        let (meta, data) = decode_all(&encode(1, 0, &b)).unwrap();
        assert_eq!(meta.entries.len(), 1);
        assert!(meta.frames.is_empty() && data.is_empty());
        assert_eq!(meta.frames_for(0, 10), 0..0);
    }

    #[test]
    fn corruption_is_detected() {
        let obj = encode(1, 1, &sample());
        let end = meta_end(&obj).unwrap();
        let mut bad_meta = obj.clone();
        bad_meta[HEADER + 3] ^= 1;
        assert!(parse_meta(&bad_meta).is_err(), "meta checksum");
        let mut bad_data = obj.clone();
        let last = bad_data.len() - 10;
        bad_data[last] ^= 1;
        assert!(decode_all(&bad_data).is_err(), "frame checksum");
        assert!(parse_meta(&obj[..end - 1]).is_err(), "truncated meta");
        assert!(decode_all(&obj[..obj.len() - 1]).is_err(), "truncated frames");
        let mut magic = obj;
        magic[0] = b'X';
        assert!(meta_end(&magic).is_err());
    }
}
