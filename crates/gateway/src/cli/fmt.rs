//! Plain text output: tables and durations.

use crate::service::pb;

/// Rows as an aligned table, the first row the header. Columns are separated by two spaces, the last is not padded.
pub fn table(rows: &[Vec<String>]) -> String {
    let columns = rows.iter().map(Vec::len).max().unwrap_or(0);
    let widths: Vec<usize> = (0..columns)
        .map(|c| {
            rows.iter()
                .map(|r| r.get(c).map_or(0, |s| s.chars().count()))
                .max()
                .unwrap_or(0)
        })
        .collect();
    let mut out = String::new();
    for row in rows {
        let mut line = String::new();
        for (i, cell) in row.iter().enumerate() {
            if i + 1 == row.len() {
                line.push_str(cell);
            } else {
                line.push_str(cell);
                line.push_str(&" ".repeat(widths[i] - cell.chars().count() + 2));
            }
        }
        out.push_str(line.trim_end());
        out.push('\n');
    }
    out
}

/// `3s`, `5m02s`, `2h13m`, `1d04h`; `-` for nothing.
pub fn duration(ns: u64) -> String {
    if ns == 0 {
        return "-".into();
    }
    let s = ns / 1_000_000_000;
    match s {
        0 => "<1s".into(),
        1..=59 => format!("{s}s"),
        60..=3599 => format!("{}m{:02}s", s / 60, s % 60),
        3600..=86399 => format!("{}h{:02}m", s / 3600, (s % 3600) / 60),
        _ => format!("{}d{:02}h", s / 86400, (s % 86400) / 3600),
    }
}

/// `12.3 MB`.
pub fn bytes(n: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KB", "MB", "GB"];
    let mut v = n as f64;
    let mut unit = 0;
    while v >= 1024.0 && unit + 1 < UNITS.len() {
        v /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", UNITS[unit])
    }
}

/// The memory of a process. `0` is not a reading of a process (it has some): the service has not read it yet, as in the
/// first moments after the start, so it is `-` and not `0 B`.
pub fn memory(n: u64) -> String {
    if n == 0 { "-".into() } else { bytes(n) }
}

/// Bots that are people's programs, not the tools of brokapi.
pub fn is_tool(name: &str) -> bool {
    name.starts_with("brokapi-")
}

/// How full the busiest bucket of the rate limit is, in percent. `None` without a reading.
pub fn limit_used(g: &pb::GatewayInfo) -> Option<u32> {
    let h = g.health.as_ref()?;
    h.limits
        .iter()
        .filter(|l| l.capacity > 0)
        .map(|l| 100 * l.capacity.saturating_sub(l.available) / l.capacity)
        .max()
}

/// Bots that are connected now (they read the event stream).
pub fn bots_connected(g: &pb::GatewayInfo) -> Option<usize> {
    let h = g.health.as_ref()?;
    Some(
        h.bots
            .iter()
            .filter(|b| b.streaming && !is_tool(&b.name))
            .count(),
    )
}

fn or_dash(v: Option<String>) -> String {
    v.unwrap_or_else(|| "-".into())
}

/// The table of `brokapi ps` (`wide`: with CPU and memory, as `top` shows).
pub fn gateways_table(list: &[pb::GatewayInfo], wide: bool) -> String {
    let mut header = vec![
        "NAME", "BROKER", "STATE", "PID", "UPTIME", "RESTARTS", "BOTS", "EVENTS/S", "ORDERS",
        "LIMIT",
    ];
    if wide {
        header.extend(["CPU", "MEM"]);
    }
    header.push("LAST ERROR");
    let mut rows = vec![header.into_iter().map(String::from).collect::<Vec<_>>()];
    for g in list {
        let h = g.health.as_ref();
        let mut row = vec![
            g.name.clone(),
            g.broker.clone(),
            if g.state == "restarting" && g.restart_in_ns > 0 {
                format!("restarting ({})", duration(g.restart_in_ns))
            } else {
                g.state.clone()
            },
            if g.pid == 0 {
                "-".into()
            } else {
                g.pid.to_string()
            },
            duration(g.uptime_ns),
            g.restarts.to_string(),
            or_dash(bots_connected(g).map(|n| n.to_string())),
            or_dash(h.map(|h| format!("{:.1}", h.events_per_sec))),
            or_dash(h.map(|h| h.open_orders.to_string())),
            or_dash(limit_used(g).map(|p| format!("{p}%"))),
        ];
        if wide {
            row.push(if g.pid == 0 {
                "-".into()
            } else {
                format!("{:.1}%", g.cpu_percent)
            });
            row.push(if g.pid == 0 {
                "-".into()
            } else {
                memory(g.memory_bytes)
            });
        }
        row.push(g.last_error.clone());
        rows.push(row);
    }
    table(&rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tables_align_and_trim() {
        let rows = vec![
            vec!["A".into(), "BB".into(), "C".into()],
            vec!["long one".into(), "x".into(), "".into()],
        ];
        assert_eq!(table(&rows), "A         BB  C\nlong one  x\n");
    }

    #[test]
    fn durations_and_sizes() {
        assert_eq!(duration(0), "-");
        assert_eq!(duration(400_000_000), "<1s");
        assert_eq!(duration(5_000_000_000), "5s");
        assert_eq!(duration(302_000_000_000), "5m02s");
        assert_eq!(duration(7_980_000_000_000), "2h13m");
        assert_eq!(duration(100_800_000_000_000), "1d04h");
        assert_eq!(bytes(512), "512 B");
        assert_eq!(bytes(5 * 1024 * 1024 + 300_000), "5.3 MB");
        assert_eq!(memory(0), "-");
        assert_eq!(memory(512), "512 B");
    }
}
