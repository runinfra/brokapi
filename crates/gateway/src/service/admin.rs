//! What the program does with a running gateway beyond what a bot does: find it, and clear an account.

use brokapi::core::{Error, OrderExecutor, StopOrderExecutor, StopOrderRef};

use crate::RemoteClient;

use super::config::Layout;

/// The name the program gives itself when it talks to a gateway. Names that start with `brokapi-` are the tools', not
/// bots', and are not counted as bots.
pub const TOOL_BOT: &str = "brokapi-cli";

/// Connects to the gateway of this name in a state directory.
pub async fn connect_gateway(
    layout: &Layout,
    name: &str,
    bot: &str,
) -> Result<RemoteClient, Error> {
    RemoteClient::connect_dir(&layout.gateway_dir(name), bot).await
}

/// What `cancel-all` did.
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize)]
pub struct CancelReport {
    /// Orders cancelled.
    pub orders: u32,
    /// Stop orders cancelled.
    pub stops: u32,
    /// What could not be cancelled, and why.
    pub errors: Vec<String>,
}

/// Runs a call again when the rate limit of the broker refuses it. A refusal of the limiter is local, nothing was sent, so
/// repeating is safe. An emergency command waits for its turn rather than give up.
async fn patiently<T, F, Fut>(mut call: F) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, Error>>,
{
    let mut tries = 0;
    loop {
        match call().await {
            Err(Error::RateLimited { retry_after }) if tries < 30 => {
                tries += 1;
                tokio::time::sleep(retry_after.clamp(
                    std::time::Duration::from_millis(5),
                    std::time::Duration::from_secs(5),
                ))
                .await;
            }
            other => return other,
        }
    }
}

/// Cancels every active order and stop order of the account behind a gateway. The ones that cannot be cancelled are
/// in the report, the rest are done anyway.
pub async fn cancel_all(client: &RemoteClient) -> Result<CancelReport, Error> {
    let mut report = CancelReport::default();
    for order in patiently(|| client.active_orders()).await? {
        match patiently(|| client.cancel(order.cid)).await {
            Ok(()) => report.orders += 1,
            Err(e) => report.errors.push(format!("order {}: {e}", order.cid)),
        }
    }
    match patiently(|| client.active_stop_orders()).await {
        Ok(stops) => {
            for stop in stops {
                let id = stop.broker_id.clone();
                match client
                    .cancel_stop(StopOrderRef::Broker(stop.broker_id))
                    .await
                {
                    Ok(()) => report.stops += 1,
                    Err(e) => report.errors.push(format!("stop order {id:?}: {e}")),
                }
            }
        }
        // A broker without stop orders has nothing to cancel.
        Err(Error::Unsupported { .. }) => {}
        Err(e) => report.errors.push(format!("stop orders: {e}")),
    }
    Ok(report)
}
