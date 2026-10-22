//! Schedule mapping.
//!
//! `GET /trading-schedule/daily-schedule` (docs, checked 2026-10-19) takes `classCode` AND `ticker`
//! (both required) and answers `isWorkDay` and `dailySchedule[]`, where `startDate` / `endDate` are
//! TIMES OF DAY (`"14:30:00"`, the doc example), `tradingSessionType` is a Russian name (`Торговый
//! период`, `Вечерняя торговая сессия`, `Аукцион открытия`, `Аукцион закрытия`, `Технический перерыв
//! QUIK`, `Не рабочее время/Выходной/Праздник`) and `tradingSessionStatus` is `OPEN` or `CLOSE`.
//!
//! The answer is for ONE day (today), so the intervals are put on the day of `range.from`. The time
//! zone of the times is not stated: they are read as Moscow time (UTC+3), which is an assumption. A full datetime (the form older samples had) is read as it is.

use std::time::Instant;

use brokapi_bcs_protocol::http::models::{DailyScheduleResponse, ScheduleEntry};
use brokapi_core::{Schedule, SessionInterval, SessionKind, UnixNanos};
use chrono::{NaiveDate, NaiveTime};

use super::timestamps;

/// Seconds east of UTC of the exchange clock the times are read in (Moscow, no DST).
const MSK_OFFSET_SECS: i64 = 3 * 3600;

/// Maps the daily schedule of an instrument to a [`Schedule`] for `exchange` (the label the caller
/// used, `"SBER@TQBR"`).
///
/// Only intervals with the status `OPEN` are kept, and a day that is not a working day gives no
/// intervals at all. The times of day are put on `day` and read as Moscow time, which is an assumption
/// (see the module docs).
pub fn map_daily_schedule(
    exchange: &str,
    day: NaiveDate,
    resp: &DailyScheduleResponse,
    now: Instant,
) -> Schedule {
    // A day that is not a working day has no trading intervals, whatever the list says.
    let sessions = if resp.is_work_day {
        resp.daily_schedule
            .iter()
            .filter(|e| e.trading_session_status.eq_ignore_ascii_case("OPEN"))
            .filter_map(|e| map_session(e, day))
            .collect()
    } else {
        Vec::new()
    };
    Schedule {
        exchange: exchange.to_string(),
        sessions,
        ts: timestamps(now, None),
    }
}

fn map_session(entry: &ScheduleEntry, day: NaiveDate) -> Option<SessionInterval> {
    let start = parse_schedule_ts(&entry.start_date, day)?;
    let end = parse_schedule_ts(&entry.end_date, day)?;
    Some(SessionInterval {
        start,
        end,
        kind: map_session_kind(&entry.trading_session_type),
    })
}

fn map_session_kind(raw: &str) -> SessionKind {
    let lower = raw.to_lowercase();
    if lower.contains("аукцион") || lower.contains("auction") {
        SessionKind::Auction
    } else if lower.contains("вечерн") || lower.contains("evening") {
        SessionKind::PostMarket
    } else if lower.contains("торговый период") || lower.contains("main") || lower.contains("open")
    {
        SessionKind::Main
    } else {
        SessionKind::Other
    }
}

fn parse_schedule_ts(raw: &str, day: NaiveDate) -> Option<UnixNanos> {
    if let Ok(t) = NaiveTime::parse_from_str(raw, "%H:%M:%S") {
        let local = day.and_time(t).and_utc().timestamp() - MSK_OFFSET_SECS;
        return Some(UnixNanos::from_nanos(local * 1_000_000_000));
    }
    chrono::DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|dt| super::unix_nanos(dt.with_timezone(&chrono::Utc)))
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S%.3fZ")
                .ok()
                .map(|ndt| super::unix_nanos(ndt.and_utc()))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resp(json: &str) -> DailyScheduleResponse {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn times_of_day_are_put_on_the_requested_day_in_moscow_time() {
        let r = resp(
            r#"{"isWorkDay":true,"dailySchedule":[
              {"startDate":"09:50:00","endDate":"10:00:00","tradingSessionType":"Аукцион открытия","tradingSessionStatus":"OPEN"},
              {"startDate":"10:00:00","endDate":"18:50:00","tradingSessionType":"Торговый период","tradingSessionStatus":"OPEN"},
              {"startDate":"18:50:00","endDate":"19:00:00","tradingSessionType":"Технический перерыв QUIK","tradingSessionStatus":"CLOSE"},
              {"startDate":"19:05:00","endDate":"23:50:00","tradingSessionType":"Вечерняя торговая сессия","tradingSessionStatus":"OPEN"}]}"#,
        );
        let day = NaiveDate::from_ymd_opt(2026, 10, 19).unwrap();
        let s = map_daily_schedule("SBER@TQBR", day, &r, Instant::now());
        assert_eq!(
            s.sessions.len(),
            3,
            "the CLOSE interval is a break, not a session"
        );
        let kinds: Vec<_> = s.sessions.iter().map(|x| x.kind).collect();
        assert_eq!(
            kinds,
            vec![
                SessionKind::Auction,
                SessionKind::Main,
                SessionKind::PostMarket
            ]
        );
        // 10:00 Moscow = 07:00 UTC on 2026-10-19
        let main = &s.sessions[1];
        let utc7 = day.and_hms_opt(7, 0, 0).unwrap().and_utc().timestamp() * 1_000_000_000;
        assert_eq!(main.start.as_nanos(), utc7);
        assert!(main.end.as_nanos() > main.start.as_nanos());
    }

    #[test]
    fn a_day_off_has_no_sessions() {
        let r = resp(
            r#"{"isWorkDay":false,"dailySchedule":[{"startDate":"10:00:00","endDate":"18:50:00","tradingSessionType":"Торговый период","tradingSessionStatus":"OPEN"}]}"#,
        );
        let s = map_daily_schedule(
            "SBER@TQBR",
            NaiveDate::from_ymd_opt(2026, 10, 18).unwrap(),
            &r,
            Instant::now(),
        );
        assert!(s.sessions.is_empty());
    }

    #[test]
    fn full_datetimes_are_still_read() {
        let r = resp(
            r#"{"isWorkDay":true,"dailySchedule":[{"startDate":"2026-10-19T07:00:00.000Z","endDate":"2026-10-19T15:40:00.000Z","tradingSessionType":"MAIN","tradingSessionStatus":"OPEN"}]}"#,
        );
        let s = map_daily_schedule(
            "x",
            NaiveDate::from_ymd_opt(2026, 10, 19).unwrap(),
            &r,
            Instant::now(),
        );
        assert_eq!(s.sessions.len(), 1);
        assert_eq!(s.sessions[0].kind, SessionKind::Main);
    }
}
