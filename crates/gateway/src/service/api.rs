//! The API of the service (`proto/brokapi/service/v1/service.proto`).

use std::sync::Arc;

use tonic::{Request, Response, Status};

use super::admin;
use super::config::Config;
use super::pb;
use super::supervisor::{GatewayState, Inner, Slot};

pub(crate) struct Api {
    pub(crate) inner: Arc<Inner>,
}

type Reply<T> = Result<Response<T>, Status>;

impl Api {
    async fn slot(&self, name: &str) -> Result<Arc<Slot>, Status> {
        let slots = self.inner.slots.lock().await;
        slots.get(name).cloned().ok_or_else(|| {
            let known: Vec<_> = slots.keys().cloned().collect();
            Status::not_found(format!(
                "no gateway named {name:?} in this service (it has: {}). `brokapi add {name}` brings one in from the config file",
                if known.is_empty() { "none".into() } else { known.join(", ") }
            ))
        })
    }

    /// The config file read again.
    fn reread(&self) -> Result<Config, Status> {
        let path = self
            .inner
            .config
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .path
            .clone();
        Config::load(&path).map_err(|e| Status::failed_precondition(e.to_string()))
    }

    fn adopt_config(&self, config: Config) {
        *self.inner.config.lock().unwrap_or_else(|e| e.into_inner()) = config;
    }

    /// A gateway with what it says of itself right now.
    async fn describe(&self, slot: &Slot) -> pb::GatewayInfo {
        let health = if matches!(slot.state(), GatewayState::Starting | GatewayState::Running) {
            self.inner.fresh_health(&slot.name).await
        } else {
            None
        };
        slot.info(health, &self.inner.layout)
    }
}

fn names(list: &[String]) -> Vec<String> {
    list.to_vec()
}

#[tonic::async_trait]
impl pb::service_server::Service for Api {
    async fn info(&self, _: Request<pb::InfoRequest>) -> Reply<pb::ServiceInfo> {
        let slots = self.inner.slots.lock().await;
        let running = slots
            .values()
            .filter(|s| s.state() == GatewayState::Running)
            .count();
        let config = self
            .inner
            .config
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .path
            .display()
            .to_string();
        let uptime = brokapi::runtime::now().saturating_duration_since(self.inner.started);
        Ok(Response::new(pb::ServiceInfo {
            pid: std::process::id(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            config,
            state_dir: self.inner.layout.root.display().to_string(),
            address: self.inner.address.to_string(),
            started_ns: self.inner.started_unix.saturating_mul(1_000_000_000),
            uptime_ns: crate::metrics::nanos(uptime),
            gateways: slots.len() as u32,
            running: running as u32,
        }))
    }

    async fn list(&self, req: Request<pb::ListRequest>) -> Reply<pb::ListResponse> {
        let wanted = names(&req.get_ref().names);
        let slots: Vec<Arc<Slot>> = if wanted.is_empty() {
            self.inner.slots.lock().await.values().cloned().collect()
        } else {
            let mut out = Vec::new();
            for name in &wanted {
                out.push(self.slot(name).await?);
            }
            out
        };
        let infos = futures_util::future::join_all(slots.iter().map(|s| self.describe(s))).await;
        Ok(Response::new(pb::ListResponse { gateways: infos }))
    }

    async fn start(&self, req: Request<pb::GatewayRef>) -> Reply<pb::GatewayInfo> {
        let slot = self.slot(&req.get_ref().name).await?;
        if !slot.start().await {
            return Err(Status::unavailable("the service is stopping"));
        }
        Ok(Response::new(self.describe(&slot).await))
    }

    async fn stop(&self, req: Request<pb::StopRequest>) -> Reply<pb::StopResponse> {
        let slot = self.slot(&req.get_ref().name).await?;
        let mut response = pb::StopResponse::default();
        if req.get_ref().cancel_orders && slot.state() == GatewayState::Running {
            let client = admin::connect_gateway(&self.inner.layout, &slot.name, admin::TOOL_BOT)
                .await
                .map_err(|e| {
                    Status::unavailable(format!(
                        "cannot reach the gateway to cancel the orders: {e}"
                    ))
                })?;
            let report = admin::cancel_all(&client)
                .await
                .map_err(|e| Status::unavailable(format!("cannot cancel the orders: {e}")))?;
            response.cancelled_orders = report.orders;
            response.cancelled_stops = report.stops;
            response.errors = report.errors;
        } else if req.get_ref().cancel_orders {
            response.errors.push(format!(
                "the gateway is {}: its orders were not cancelled",
                slot.state().as_str()
            ));
        }
        if !slot.stop().await {
            return Err(Status::unavailable("the service is stopping"));
        }
        response.gateway = Some(self.describe(&slot).await);
        Ok(Response::new(response))
    }

    async fn restart(&self, req: Request<pb::GatewayRef>) -> Reply<pb::GatewayInfo> {
        let slot = self.slot(&req.get_ref().name).await?;
        if !slot.restart().await {
            return Err(Status::unavailable("the service is stopping"));
        }
        Ok(Response::new(self.describe(&slot).await))
    }

    async fn add(&self, req: Request<pb::GatewayRef>) -> Reply<pb::GatewayInfo> {
        let name = req.get_ref().name.clone();
        let config = self.reread()?;
        let spec = config.gateway(&name).cloned().ok_or_else(|| {
            Status::not_found(format!(
                "the config file {} has no gateway named {name:?}",
                config.path.display()
            ))
        })?;
        self.adopt_config(config);
        let existing = self.inner.slots.lock().await.get(&name).cloned();
        let slot = match existing {
            Some(slot) => {
                let same = *slot.spec.lock().unwrap_or_else(|e| e.into_inner()) == spec;
                if same {
                    return Err(Status::already_exists(format!(
                        "the gateway {name:?} is already in the service ({}); `brokapi restart {name}` restarts it",
                        slot.state().as_str()
                    )));
                }
                slot.set_spec(spec);
                slot.restart().await;
                slot
            }
            None => {
                let slot = Inner::make_slot(&self.inner, spec, true);
                self.inner
                    .slots
                    .lock()
                    .await
                    .insert(name, Arc::clone(&slot));
                slot
            }
        };
        Ok(Response::new(self.describe(&slot).await))
    }

    async fn remove(&self, req: Request<pb::GatewayRef>) -> Reply<pb::GatewayInfo> {
        let name = req.get_ref().name.clone();
        let slot = self.slot(&name).await?;
        slot.quit_now(false);
        slot.join().await;
        self.inner.slots.lock().await.remove(&name);
        Ok(Response::new(slot.info(None, &self.inner.layout)))
    }

    async fn reload(&self, _: Request<pb::ReloadRequest>) -> Reply<pb::ReloadResponse> {
        let config = self.reread()?;
        self.adopt_config(config.clone());
        let mut out = pb::ReloadResponse::default();
        let current: Vec<(String, Arc<Slot>)> = self
            .inner
            .slots
            .lock()
            .await
            .iter()
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect();
        for (name, slot) in &current {
            match config.gateway(name) {
                None => {
                    slot.quit_now(false);
                    slot.join().await;
                    self.inner.slots.lock().await.remove(name);
                    out.stopped.push(name.clone());
                }
                Some(spec) => {
                    let same = *slot.spec.lock().unwrap_or_else(|e| e.into_inner()) == *spec;
                    if same {
                        out.unchanged.push(name.clone());
                    } else {
                        slot.set_spec(spec.clone());
                        slot.restart().await;
                        out.restarted.push(name.clone());
                    }
                }
            }
        }
        for spec in &config.gateways {
            if current.iter().all(|(n, _)| *n != spec.name) {
                let slot = Inner::make_slot(&self.inner, spec.clone(), spec.autostart);
                self.inner
                    .slots
                    .lock()
                    .await
                    .insert(spec.name.clone(), slot);
                out.started.push(spec.name.clone());
            }
        }
        Ok(Response::new(out))
    }

    async fn shutdown(&self, req: Request<pb::ShutdownRequest>) -> Reply<pb::ShutdownResponse> {
        self.inner
            .shutdown
            .send_replace(Some(req.get_ref().leave_gateways));
        Ok(Response::new(pb::ShutdownResponse {}))
    }
}
