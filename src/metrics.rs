//! Metrics: a per-disk snapshot, rendered as Prometheus text exposition or JSON, and an optional
//! HTTP listener that serves `GET /metrics`.

use std::fmt::Write;
use std::io::{Read, Write as _};
use std::net::TcpListener;

#[derive(serde::Serialize, Default, Clone, Debug)]
pub struct DiskSnapshot {
    pub id: i32,
    pub epoch: u64,
    pub policy: &'static str,
    pub fenced: bool,
    pub committed_seq: u64,
    pub queued_bytes: u64,
    pub open_bytes: u64,
    pub max_lag_bytes: u64,
    pub puts: u64,
    pub put_bytes: u64,
    pub raw_bytes: u64,
    pub upload_errors: u64,
    pub upload_seconds: f64,
    pub flushes: u64,
    pub flush_wait_seconds: f64,
    pub intent_syncs: u64,
    pub lag_waits: u64,
}

#[derive(serde::Serialize, Clone, Debug)]
pub struct Snapshot {
    pub version: &'static str,
    pub rss_bytes: u64,
    pub disks: Vec<DiskSnapshot>,
}

/// One metric family: HELP, TYPE, then one sample per disk.
fn family(out: &mut String, name: &str, kind: &str, help: &str, disks: &[DiskSnapshot], value: impl Fn(&DiskSnapshot) -> String) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
    for d in disks {
        let _ = writeln!(out, "{name}{{disk=\"{}\"}} {}", d.id, value(d));
    }
}

pub fn render_prometheus(s: &Snapshot) -> String {
    let mut o = String::new();
    let _ = writeln!(o, "# HELP blockgw_info Build information.\n# TYPE blockgw_info gauge\nblockgw_info{{version=\"{}\"}} 1", s.version);
    let _ = writeln!(o, "# HELP blockgw_process_resident_memory_bytes Resident set size of the worker.\n# TYPE blockgw_process_resident_memory_bytes gauge\nblockgw_process_resident_memory_bytes {}", s.rss_bytes);
    let d = &s.disks;
    family(&mut o, "blockgw_epoch", "gauge", "Epoch this server writes the disk under.", d, |x| x.epoch.to_string());
    family(&mut o, "blockgw_policy_remote", "gauge", "1 if fsync waits for the object store (remote policy), 0 for local.", d, |x| {
        u8::from(x.policy == "remote").to_string()
    });
    family(&mut o, "blockgw_fenced", "gauge", "1 once a newer writer has fenced this server out of the disk.", d, |x| {
        u8::from(x.fenced).to_string()
    });
    family(&mut o, "blockgw_committed_segments", "gauge", "Segments committed to the object store in this epoch.", d, |x| {
        x.committed_seq.to_string()
    });
    family(
        &mut o,
        "blockgw_upload_lag_bytes",
        "gauge",
        "Sealed segment bytes not yet committed (the loss window under the local policy).",
        d,
        |x| x.queued_bytes.to_string(),
    );
    family(&mut o, "blockgw_open_segment_bytes", "gauge", "Bytes written since the last seal.", d, |x| x.open_bytes.to_string());
    family(&mut o, "blockgw_max_lag_bytes", "gauge", "Configured upload lag limit.", d, |x| x.max_lag_bytes.to_string());
    family(&mut o, "blockgw_puts_total", "counter", "Segments uploaded.", d, |x| x.puts.to_string());
    family(&mut o, "blockgw_put_bytes_total", "counter", "Object bytes uploaded (after compression).", d, |x| x.put_bytes.to_string());
    family(&mut o, "blockgw_raw_bytes_total", "counter", "Segment bytes uploaded before compression.", d, |x| x.raw_bytes.to_string());
    family(&mut o, "blockgw_upload_errors_total", "counter", "Failed upload attempts (retried).", d, |x| x.upload_errors.to_string());
    family(&mut o, "blockgw_upload_seconds_total", "counter", "Time spent in successful uploads.", d, |x| {
        format!("{:.6}", x.upload_seconds)
    });
    family(&mut o, "blockgw_flushes_total", "counter", "Flushes (fsync) handled.", d, |x| x.flushes.to_string());
    family(
        &mut o,
        "blockgw_flush_wait_seconds_total",
        "counter",
        "Time flushes spent waiting for uploads (remote policy or lag backpressure).",
        d,
        |x| format!("{:.6}", x.flush_wait_seconds),
    );
    family(&mut o, "blockgw_intent_syncs_total", "counter", "Write-intent bitmap fdatasyncs.", d, |x| x.intent_syncs.to_string());
    family(&mut o, "blockgw_lag_waits_total", "counter", "Times a writer or flush waited for the upload lag to shrink.", d, |x| {
        x.lag_waits.to_string()
    });
    o
}

/// Serve `GET /metrics` (anything else gets 404) from a background thread.
pub fn serve(addr: &str, snapshot: impl Fn() -> Snapshot + Send + 'static) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr)?;
    std::thread::Builder::new().name("metrics".into()).spawn(move || {
        for mut conn in listener.incoming().flatten() {
            let mut buf = [0u8; 1024];
            let n = conn.read(&mut buf).unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]);
            let (status, body) = if req.starts_with("GET /metrics") {
                ("200 OK", render_prometheus(&snapshot()))
            } else {
                ("404 Not Found", "not found\n".to_string())
            };
            let _ = write!(
                conn,
                "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap() -> Snapshot {
        Snapshot {
            version: "0.1.0",
            rss_bytes: 1234,
            disks: vec![
                DiskSnapshot { id: 3, policy: "remote", fenced: true, queued_bytes: 77, upload_seconds: 1.5, ..Default::default() },
                DiskSnapshot { id: 4, policy: "local", puts: 9, ..Default::default() },
            ],
        }
    }

    #[test]
    fn prometheus_text_has_help_type_and_one_sample_per_disk() {
        let t = render_prometheus(&snap());
        assert!(t.contains("blockgw_info{version=\"0.1.0\"} 1\n"));
        assert!(t.contains("blockgw_process_resident_memory_bytes 1234\n"));
        assert!(t.contains("# TYPE blockgw_puts_total counter\n"));
        assert!(t.contains("blockgw_puts_total{disk=\"4\"} 9\n"));
        assert!(t.contains("blockgw_policy_remote{disk=\"3\"} 1\n"));
        assert!(t.contains("blockgw_policy_remote{disk=\"4\"} 0\n"));
        assert!(t.contains("blockgw_fenced{disk=\"3\"} 1\n"));
        assert!(t.contains("blockgw_upload_lag_bytes{disk=\"3\"} 77\n"));
        assert!(t.contains("blockgw_upload_seconds_total{disk=\"3\"} 1.500000\n"));
        // every non-comment line is "name{labels} value" or "name value"
        for l in t.lines().filter(|l| !l.starts_with('#')) {
            let parts: Vec<&str> = l.rsplitn(2, ' ').collect();
            assert_eq!(parts.len(), 2, "{l}");
            assert!(parts[0].parse::<f64>().is_ok(), "{l}");
        }
    }

    #[test]
    fn json_snapshot_round_trips() {
        let v: serde_json::Value = serde_json::from_str(&serde_json::to_string(&snap()).unwrap()).unwrap();
        assert_eq!(v["disks"][0]["id"], 3);
        assert_eq!(v["disks"][0]["policy"], "remote");
        assert_eq!(v["disks"][1]["puts"], 9);
    }
}
