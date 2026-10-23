//! Small helpers: instrument ids, intervals, times.

use brokapi_core::{
    ConfigError, Error, InstrumentId, InstrumentKey, NativeId, Timeframe, UnixNanos,
};

/// The id of an instrument: the symbol and the category.
///
/// The key is `BTCUSDT@linear`: the second part names the product category of the exchange, because the same symbol
/// can trade as spot and as a perpetual. The native id is the pair as the exchange spells it.
///
/// # Examples
///
/// ```
/// use brokapi_bybit::util::instrument_id;
///
/// let btc = instrument_id("BTCUSDT", "linear");
/// assert_eq!(btc.key.display(), "BTCUSDT@linear");
/// ```
pub fn instrument_id(symbol: &str, category: &str) -> InstrumentId {
    InstrumentId::new(
        InstrumentKey::new(symbol, category),
        NativeId::SymbolMic {
            symbol: symbol.to_string(),
            mic: category.to_string(),
        },
    )
}

/// The symbol and the category of an instrument id.
///
/// # Errors
///
/// [`Error::Config`] when the native id is not of the kind this connector uses (`SymbolMic`), or the category is not
/// the one the connector works with.
pub fn symbol_category(id: &InstrumentId, category: &str) -> Result<String, Error> {
    match &id.native {
        NativeId::SymbolMic { symbol, mic } if mic == category => Ok(symbol.clone()),
        NativeId::SymbolMic { mic, .. } => Err(Error::Config(ConfigError::Message(format!(
            "the instrument is of the category {mic:?}, this connector works with {category:?}: start another connector for it"
        )))),
        _ => Err(Error::Config(ConfigError::Message(
            "the native id of an instrument must be SymbolMic (symbol and category) for Bybit"
                .into(),
        ))),
    }
}

/// The interval word of the API for a timeframe, `None` for a timeframe the exchange has no candles for (10 minutes).
///
/// # Examples
///
/// ```
/// use brokapi_bybit::util::interval_label;
/// use brokapi_core::Timeframe;
///
/// assert_eq!(interval_label(Timeframe::H4), Some("240"));
/// assert_eq!(interval_label(Timeframe::D1), Some("D"));
/// assert_eq!(interval_label(Timeframe::M10), None);
/// ```
pub fn interval_label(tf: Timeframe) -> Option<&'static str> {
    Some(match tf {
        Timeframe::M1 => "1",
        Timeframe::M5 => "5",
        Timeframe::M10 => return None,
        Timeframe::M15 => "15",
        Timeframe::M30 => "30",
        Timeframe::H1 => "60",
        Timeframe::H4 => "240",
        Timeframe::D1 => "D",
        Timeframe::W1 => "W",
        Timeframe::MN => "M",
    })
}

/// Milliseconds since the epoch from nanoseconds.
pub fn nanos_to_ms(nanos: UnixNanos) -> i64 {
    nanos.as_nanos().div_euclid(1_000_000)
}

#[cfg(test)]
mod tests {
    use super::*;
    use brokapi_core::NativeId;

    #[test]
    fn an_id_of_another_broker_or_category_is_a_config_error() {
        let ok = instrument_id("BTCUSDT", "linear");
        assert_eq!(symbol_category(&ok, "linear").unwrap(), "BTCUSDT");
        assert!(matches!(
            symbol_category(&ok, "spot"),
            Err(Error::Config(_))
        ));
        let foreign = InstrumentId::new(
            InstrumentKey::new("SBER", "TQBR"),
            NativeId::TickerClass {
                ticker: "SBER".into(),
                class_code: "TQBR".into(),
            },
        );
        assert!(matches!(
            symbol_category(&foreign, "linear"),
            Err(Error::Config(_))
        ));
    }
}
