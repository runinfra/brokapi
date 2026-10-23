//! The client of the service API: what the commands of the program use.

use std::path::Path;
use std::time::Duration;

use tonic::transport::Channel;
use tonic::{Request, Status};

use crate::access::{Address, KEY_HEADER, read_key_in};
use crate::remote::{ConnectOptions, open_channel};

use super::pb;
use super::supervisor::{ADDRESS_FILE, KEY_FILE};

/// Why a call to the service did not work.
#[derive(Debug, thiserror::Error)]
pub enum CallError {
    /// No service answers for this state directory.
    #[error("no service is running for the state directory {0}")]
    NotRunning(String),
    /// The service answered with a refusal.
    #[error("{}", .0.message())]
    Refused(Status),
}

/// A connection to the service of a state directory.
#[derive(Clone)]
pub struct ServiceClient {
    channel: Channel,
    auth: tonic::metadata::MetadataValue<tonic::metadata::Ascii>,
}

impl ServiceClient {
    /// Connects to the service whose state directory is `state_dir`: the address and the key are read from there.
    pub async fn connect(state_dir: &Path) -> Result<Self, CallError> {
        let not_running = || CallError::NotRunning(state_dir.display().to_string());
        let address: Address = std::fs::read_to_string(state_dir.join(ADDRESS_FILE))
            .map_err(|_| not_running())?
            .trim()
            .parse()
            .map_err(|_: String| not_running())?;
        let key = read_key_in(state_dir, KEY_FILE).map_err(|_| not_running())?;
        let channel = tokio::time::timeout(
            Duration::from_secs(3),
            open_channel(&address, ConnectOptions::default()),
        )
        .await
        .map_err(|_| not_running())?
        .map_err(|_| not_running())?;
        let auth = format!("Bearer {key}").parse().map_err(|_| not_running())?;
        Ok(Self { channel, auth })
    }

    fn grpc(&self) -> pb::service_client::ServiceClient<Channel> {
        pb::service_client::ServiceClient::new(self.channel.clone())
    }

    fn request<T>(&self, message: T) -> Request<T> {
        let mut req = Request::new(message);
        req.metadata_mut().insert(KEY_HEADER, self.auth.clone());
        req
    }

    /// What the service is.
    pub async fn info(&self) -> Result<pb::ServiceInfo, CallError> {
        self.grpc()
            .info(self.request(pb::InfoRequest {}))
            .await
            .map(tonic::Response::into_inner)
            .map_err(CallError::Refused)
    }

    /// The gateways (all of them for no names).
    pub async fn list(&self, names: Vec<String>) -> Result<Vec<pb::GatewayInfo>, CallError> {
        self.grpc()
            .list(self.request(pb::ListRequest { names }))
            .await
            .map(|r| r.into_inner().gateways)
            .map_err(CallError::Refused)
    }

    /// Starts a gateway.
    pub async fn start(&self, name: &str) -> Result<pb::GatewayInfo, CallError> {
        self.grpc()
            .start(self.request(pb::GatewayRef { name: name.into() }))
            .await
            .map(tonic::Response::into_inner)
            .map_err(CallError::Refused)
    }

    /// Stops a gateway, with the cancelling of the orders of the account first if asked.
    pub async fn stop(
        &self,
        name: &str,
        cancel_orders: bool,
    ) -> Result<pb::StopResponse, CallError> {
        self.grpc()
            .stop(self.request(pb::StopRequest {
                name: name.into(),
                cancel_orders,
            }))
            .await
            .map(tonic::Response::into_inner)
            .map_err(CallError::Refused)
    }

    /// Stops a gateway and starts it again.
    pub async fn restart(&self, name: &str) -> Result<pb::GatewayInfo, CallError> {
        self.grpc()
            .restart(self.request(pb::GatewayRef { name: name.into() }))
            .await
            .map(tonic::Response::into_inner)
            .map_err(CallError::Refused)
    }

    /// Brings a gateway of the config file in.
    pub async fn add(&self, name: &str) -> Result<pb::GatewayInfo, CallError> {
        self.grpc()
            .add(self.request(pb::GatewayRef { name: name.into() }))
            .await
            .map(tonic::Response::into_inner)
            .map_err(CallError::Refused)
    }

    /// Stops a gateway and forgets it.
    pub async fn remove(&self, name: &str) -> Result<pb::GatewayInfo, CallError> {
        self.grpc()
            .remove(self.request(pb::GatewayRef { name: name.into() }))
            .await
            .map(tonic::Response::into_inner)
            .map_err(CallError::Refused)
    }

    /// Reads the config file again.
    pub async fn reload(&self) -> Result<pb::ReloadResponse, CallError> {
        self.grpc()
            .reload(self.request(pb::ReloadRequest {}))
            .await
            .map(tonic::Response::into_inner)
            .map_err(CallError::Refused)
    }

    /// Asks the service to stop.
    pub async fn shutdown(&self, leave_gateways: bool) -> Result<(), CallError> {
        self.grpc()
            .shutdown(self.request(pb::ShutdownRequest { leave_gateways }))
            .await
            .map(|_| ())
            .map_err(CallError::Refused)
    }
}
