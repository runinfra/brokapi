//! Trading schedule.

use std::time::Instant;

use brokapi_core::{Schedule, SessionInterval, SessionKind};
use brokapi_tbank_protocol::v1 as pb;
use prost_types::Timestamp;

use crate::ids::{proto_to_nanos, stamps};

fn push(
    out: &mut Vec<SessionInterval>,
    start: &Option<Timestamp>,
    end: &Option<Timestamp>,
    kind: SessionKind,
) {
    if let (Some(s), Some(e)) = (start, end)
        && s.seconds != 0
        && e.seconds != 0
        && e.seconds >= s.seconds
    {
        out.push(SessionInterval {
            start: proto_to_nanos(s),
            end: proto_to_nanos(e),
            kind,
        });
    }
}

/// Main session, premarket, evening session (post-market) and auctions of every trading day.
pub fn schedule(exchange: &str, sched: &pb::TradingSchedule, now: Instant) -> Schedule {
    let mut sessions = Vec::new();
    for day in sched.days.iter().filter(|d| d.is_trading_day) {
        push(
            &mut sessions,
            &day.premarket_start_time,
            &day.premarket_end_time,
            SessionKind::PreMarket,
        );
        push(
            &mut sessions,
            &day.opening_auction_start_time,
            &day.opening_auction_end_time,
            SessionKind::Auction,
        );
        push(
            &mut sessions,
            &day.start_time,
            &day.end_time,
            SessionKind::Main,
        );
        push(
            &mut sessions,
            &day.closing_auction_start_time,
            &day.closing_auction_end_time,
            SessionKind::Auction,
        );
        push(
            &mut sessions,
            &day.evening_start_time,
            &day.evening_end_time,
            SessionKind::PostMarket,
        );
    }
    sessions.sort_by_key(|s| s.start);
    Schedule {
        exchange: exchange.to_string(),
        sessions,
        ts: stamps(now, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: i64) -> Option<Timestamp> {
        Some(Timestamp {
            seconds: s,
            nanos: 0,
        })
    }

    #[test]
    fn sessions_of_trading_days_sorted() {
        let day = pb::TradingDay {
            is_trading_day: true,
            start_time: t(1_000),
            end_time: t(2_000),
            evening_start_time: t(3_000),
            evening_end_time: t(4_000),
            premarket_start_time: t(500),
            premarket_end_time: t(900),
            ..Default::default()
        };
        let off = pb::TradingDay {
            is_trading_day: false,
            start_time: t(10),
            end_time: t(20),
            ..Default::default()
        };
        let s = schedule(
            "MOEX",
            &pb::TradingSchedule {
                exchange: "MOEX".into(),
                days: vec![day, off],
            },
            Instant::now(),
        );
        let kinds: Vec<_> = s.sessions.iter().map(|x| x.kind).collect();
        assert_eq!(
            kinds,
            vec![
                SessionKind::PreMarket,
                SessionKind::Main,
                SessionKind::PostMarket
            ]
        );
    }
}
