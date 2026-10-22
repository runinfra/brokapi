//! Small helpers: the identity of an instrument, the timeframe labels and the time conversions.

use brokapi_core::{
    ConfigError, Error, InstrumentId, InstrumentKey, NativeId, Timeframe, UnixNanos,
};
use chrono::{DateTime, TimeZone, Utc};

/// The ticker and the trading mode (board) of an instrument. ALOR needs them as two fields, so the native id
/// must be [`NativeId::TickerClass`] (the class code is the board, for example `TQBR`).
pub fn ticker_board(id: &InstrumentId) -> Result<(String, String), Error> {
    match &id.native {
        NativeId::TickerClass { ticker, class_code } => Ok((ticker.clone(), class_code.clone())),
        _ => Err(Error::Config(ConfigError::Message(
            "the native id of an instrument must be TickerClass (ticker and board) for ALOR".into(),
        ))),
    }
}

/// The exchange an instrument trades on: `SPBX` for the board `SPBX`, `MOEX` for every other board.
pub fn exchange_of_board(board: &str) -> &'static str {
    if board.eq_ignore_ascii_case("SPBX") {
        "SPBX"
    } else {
        "MOEX"
    }
}

/// The id of an instrument by ticker and board.
pub fn instrument_id(ticker: &str, board: &str) -> InstrumentId {
    InstrumentId::new(
        InstrumentKey::new(ticker, board),
        NativeId::TickerClass {
            ticker: ticker.to_string(),
            class_code: board.to_string(),
        },
    )
}

/// The `tf` of the broker for a timeframe: a number of seconds, or `D`, `W`, `M`.
pub fn timeframe_label(tf: Timeframe) -> String {
    match tf {
        Timeframe::D1 => "D".to_string(),
        Timeframe::W1 => "W".to_string(),
        Timeframe::MN => "M".to_string(),
        other => other.seconds().to_string(),
    }
}

/// A [`UnixNanos`] as a date.
pub fn nanos_to_datetime(nanos: UnixNanos) -> DateTime<Utc> {
    let secs = nanos.as_nanos().div_euclid(1_000_000_000);
    let nsec = nanos.as_nanos().rem_euclid(1_000_000_000) as u32;
    Utc.timestamp_opt(secs, nsec)
        .single()
        .unwrap_or_else(Utc::now)
}

/// Unix seconds of a [`UnixNanos`].
pub fn nanos_to_secs(nanos: UnixNanos) -> i64 {
    nanos.as_nanos().div_euclid(1_000_000_000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeframes_are_seconds_or_letters() {
        assert_eq!(timeframe_label(Timeframe::M1), "60");
        assert_eq!(timeframe_label(Timeframe::M10), "600");
        assert_eq!(timeframe_label(Timeframe::H4), "14400");
        assert_eq!(timeframe_label(Timeframe::D1), "D");
        assert_eq!(timeframe_label(Timeframe::MN), "M");
    }

    #[test]
    fn the_exchange_follows_the_board() {
        assert_eq!(exchange_of_board("TQBR"), "MOEX");
        assert_eq!(exchange_of_board("SPBFUT"), "MOEX");
        assert_eq!(exchange_of_board("SPBX"), "SPBX");
    }

    #[test]
    fn an_id_of_another_broker_is_refused() {
        let id = InstrumentId::new(
            InstrumentKey::new("SBER", "MISX"),
            NativeId::SymbolMic {
                symbol: "SBER".into(),
                mic: "MISX".into(),
            },
        );
        assert!(matches!(ticker_board(&id), Err(Error::Config(_))));
    }
}
