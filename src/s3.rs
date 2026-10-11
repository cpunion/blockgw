//! Minimal blocking S3 client: presigned URLs (rusty-s3) over ureq. Create-only PUTs carry a signed
//! `If-None-Match: *` header; a 412/409 answer means another writer got the key first.

use crate::store::{PutOutcome, Store};
use anyhow::{anyhow, bail, Result};
use rusty_s3::actions::{DeleteObject, GetObject, ListObjectsV2, PutObject, S3Action};
use rusty_s3::{Bucket, Credentials, UrlStyle};
use std::time::Duration;

#[derive(serde::Deserialize, Clone)]
pub struct S3Config {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    #[serde(default)]
    pub path_style: bool,
}

pub struct S3 {
    bucket: Bucket,
    creds: Credentials,
    agent: ureq::Agent,
}

const TTL: Duration = Duration::from_secs(600);

impl S3 {
    pub fn new(c: &S3Config) -> Result<Self> {
        let style = if c.path_style { UrlStyle::Path } else { UrlStyle::VirtualHost };
        let bucket = Bucket::new(c.endpoint.parse()?, style, c.bucket.clone(), c.region.clone())?;
        let agent = ureq::AgentBuilder::new().timeout_connect(Duration::from_secs(10)).timeout(Duration::from_secs(120)).build();
        Ok(S3 { bucket, creds: Credentials::new(c.access_key.clone(), c.secret_key.clone()), agent })
    }
}

impl Store for S3 {
    /// Create-only PUT. `Exists` when the key is already there (S3 412, some stores 409).
    fn put_new(&self, key: &str, body: &[u8]) -> Result<PutOutcome> {
        let mut a = PutObject::new(&self.bucket, Some(&self.creds), key);
        a.headers_mut().insert("if-none-match", "*");
        let url = a.sign(TTL);
        match self.agent.put(url.as_str()).set("If-None-Match", "*").send_bytes(body) {
            Ok(_) => Ok(PutOutcome::Created),
            Err(ureq::Error::Status(412, _)) | Err(ureq::Error::Status(409, _)) => Ok(PutOutcome::Exists),
            Err(ureq::Error::Status(code, r)) => bail!("PUT {key}: HTTP {code}: {}", r.into_string().unwrap_or_default()),
            Err(e) => Err(anyhow!("PUT {key}: {e}")),
        }
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        let url = GetObject::new(&self.bucket, Some(&self.creds), key).sign(TTL);
        let r = self.agent.get(url.as_str()).call().map_err(|e| anyhow!("GET {key}: {e}"))?;
        let mut v = Vec::new();
        std::io::Read::read_to_end(&mut r.into_reader(), &mut v)?;
        Ok(v)
    }

    /// All keys under `prefix`, lexicographically sorted (keys use zero-padded numbers).
    fn get_range(&self, key: &str, off: u64, len: u64) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let url = GetObject::new(&self.bucket, Some(&self.creds), key).sign(TTL);
        let r = self
            .agent
            .get(url.as_str())
            .set("Range", &format!("bytes={}-{}", off, off + len - 1))
            .call()
            .map_err(|e| anyhow!("GET {key} range {off}+{len}: {e}"))?;
        let mut v = Vec::new();
        std::io::Read::read_to_end(&mut r.into_reader(), &mut v)?;
        Ok(v)
    }

    fn list_sized(&self, prefix: &str) -> Result<Vec<(String, u64)>> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut a = ListObjectsV2::new(&self.bucket, Some(&self.creds));
            a.with_prefix(prefix);
            if let Some(t) = &token {
                a.with_continuation_token(t.clone());
            }
            let url = a.sign(TTL);
            let body = self.agent.get(url.as_str()).call().map_err(|e| anyhow!("LIST {prefix}: {e}"))?.into_string()?;
            let resp = ListObjectsV2::parse_response(&body).map_err(|e| anyhow!("LIST parse: {e}"))?;
            out.extend(resp.contents.into_iter().map(|c| (c.key, c.size)));
            match resp.next_continuation_token {
                Some(t) => token = Some(t),
                None => break,
            }
        }
        out.sort();
        Ok(out)
    }

    fn delete(&self, key: &str) -> Result<()> {
        let url = DeleteObject::new(&self.bucket, Some(&self.creds), key).sign(TTL);
        match self.agent.delete(url.as_str()).call() {
            Ok(_) | Err(ureq::Error::Status(404, _)) => Ok(()),
            Err(e) => Err(anyhow!("DELETE {key}: {e}")),
        }
    }
}
