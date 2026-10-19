//! JWT lifecycle: secret -> `AuthService.Auth` -> JWT, refreshed ahead of expiry.
//!
//! One [`TokenManager`] per secret. It is shared (`Arc`) by every connector that uses the secret,
//! so several accounts or roles do not each run their own `Auth`. The refresher task holds only a
//! `Weak` reference and ends when the last `Arc` is dropped.
//!
//! Lifetime comes from `TokenDetails` (`expires_at - created_at`, both server clock, so no local
//! clock skew), falling back to the JWT `exp`/`iat` claims, then to a conservative assumption.
//!
//! # Examples
//!
//! Start a manager against the in-process imitation of the broker, read the token and the accounts
//! that the secret can reach:
//!
//! ```
//! use std::sync::Arc;
//! use brokapi_finam_protocol::client::connect_lazy;
//! use brokapi_finam_protocol::{ApiEndpoint, GrpcTokenApi, TokenManager};
//! # use brokapi_mocks::finam::{MockServer, SECRET};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let server = MockServer::start().await;
//! # let endpoint = ApiEndpoint::new(server.url.clone());
//! # let secret = SECRET;
//! let channel = connect_lazy(&endpoint)?;
//! let api = Arc::new(GrpcTokenApi::new(channel));
//! let tokens = TokenManager::start(api, secret, "my-app").await?;
//!
//! assert_eq!(tokens.generation(), 1);
//! assert!(!tokens.current().is_empty());
//! assert_eq!(tokens.account_ids(), vec!["ACC1", "ACC2"]);
//!
//! // Renew by hand, for example after you saw UNAUTHENTICATED.
//! tokens.refresh().await?;
//! assert_eq!(tokens.generation(), 2);
//! # Ok(())
//! # }
//! ```

use std::fmt;
use std::sync::{Arc, RwLock, Weak};
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use tokio::sync::{Mutex, Notify};
use tokio::time::Instant;
use tonic::transport::Channel;
use tracing::{debug, warn};

use crate::error::FinamError;
use crate::pb::grpc::tradeapi::v1::auth::auth_service_client::AuthServiceClient;
use crate::pb::grpc::tradeapi::v1::auth::{AuthRequest, TokenDetailsRequest};

/// Used only when neither `TokenDetails` nor the JWT claims tell the lifetime.
pub const ASSUMED_TTL: Duration = Duration::from_secs(5 * 60);

/// Refresh retry backoff bounds after a failed refresh.
const RETRY_MIN: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(30);

/// What `TokenDetails` says about a token.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TokenInfo {
    /// Token lifetime (`expires_at - created_at`), if known.
    pub ttl: Option<Duration>,
    /// Accounts reachable with this token.
    pub account_ids: Vec<String>,
    /// Trading is disabled for this session.
    pub readonly: bool,
}

/// Auth transport, abstracted so refresh logic is testable without a network.
///
/// [`GrpcTokenApi`] is the real implementation. Implement the trait yourself to test code that depends
/// on a [`TokenManager`] without a server.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use async_trait::async_trait;
/// use brokapi_finam_protocol::{FinamError, TokenApi, TokenInfo, TokenManager};
///
/// /// Gives numbered tokens and no details.
/// struct Fake(std::sync::atomic::AtomicU32);
///
/// #[async_trait]
/// impl TokenApi for Fake {
///     async fn auth(&self, _secret: &str, _app: &str) -> Result<String, FinamError> {
///         let n = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
///         Ok(format!("token-{n}"))
///     }
///     async fn token_details(&self, _token: &str) -> Result<TokenInfo, FinamError> {
///         Ok(TokenInfo { account_ids: vec!["A1".into()], ..TokenInfo::default() })
///     }
/// }
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let tokens = TokenManager::start(Arc::new(Fake(Default::default())), "secret", "").await?;
/// assert_eq!(&*tokens.current(), "token-1");
/// assert_eq!(tokens.account_ids(), vec!["A1"]);
/// # Ok(())
/// # }
/// ```
#[async_trait]
pub trait TokenApi: Send + Sync {
    /// Exchanges the API secret for a JWT (`AuthService.Auth`).
    ///
    /// `source_app_id` is the application id the broker shows in its records. It may be empty.
    ///
    /// # Errors
    ///
    /// [`FinamError::Status`] when the server refuses the secret or cannot be reached.
    async fn auth(&self, secret: &str, source_app_id: &str) -> Result<String, FinamError>;
    /// Reads the lifetime and the account list of `token` (`AuthService.TokenDetails`).
    ///
    /// The manager does not fail when this call fails: it falls back to the JWT claims for the
    /// lifetime and to an empty account list.
    ///
    /// # Errors
    ///
    /// [`FinamError::Status`] when the server refuses the token or cannot be reached.
    async fn token_details(&self, token: &str) -> Result<TokenInfo, FinamError>;
}

/// [`TokenApi`] over the gRPC `AuthService` of the broker.
///
/// See the [module docs](self) for how it is used with a [`TokenManager`].
pub struct GrpcTokenApi {
    client: AuthServiceClient<Channel>,
}

impl GrpcTokenApi {
    /// Auth transport over `channel`. The channel needs no authorization: `Auth` is the call that
    /// creates the token.
    pub fn new(channel: Channel) -> Self {
        Self {
            client: AuthServiceClient::new(channel),
        }
    }
}

#[async_trait]
impl TokenApi for GrpcTokenApi {
    async fn auth(&self, secret: &str, source_app_id: &str) -> Result<String, FinamError> {
        let resp = self
            .client
            .clone()
            .auth(AuthRequest {
                secret: secret.to_string(),
                source_app_id: source_app_id.to_string(),
            })
            .await?
            .into_inner();
        Ok(resp.token)
    }

    async fn token_details(&self, token: &str) -> Result<TokenInfo, FinamError> {
        let d = self
            .client
            .clone()
            .token_details(TokenDetailsRequest {
                token: token.to_string(),
            })
            .await?
            .into_inner();
        let ttl = match (d.created_at, d.expires_at) {
            (Some(c), Some(e)) => {
                let secs = e.seconds - c.seconds;
                (secs > 0).then(|| Duration::from_secs(secs as u64))
            }
            _ => None,
        };
        Ok(TokenInfo {
            ttl,
            account_ids: d.account_ids,
            readonly: d.readonly,
        })
    }
}

/// Lifetime from the JWT payload (`exp` - `iat`), if the token is a readable JWT.
///
/// The signature is not checked and the token is not validated. `None` when the token is not a JWT,
/// the claims are missing, or `exp` is not after `iat`.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use brokapi_finam_protocol::auth::jwt_ttl;
///
/// // Header {"alg":"none"}, payload {"iat":1000,"exp":1900}.
/// let token = "eyJhbGciOiJub25lIn0.eyJpYXQiOjEwMDAsImV4cCI6MTkwMH0.sig";
/// assert_eq!(jwt_ttl(token), Some(Duration::from_secs(900)));
/// assert_eq!(jwt_ttl("opaque-token"), None);
/// ```
pub fn jwt_ttl(token: &str) -> Option<Duration> {
    let payload = token.split('.').nth(1)?;
    let bytes = URL_SAFE_NO_PAD.decode(payload.trim_end_matches('=')).ok()?;
    let v: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    let exp = v.get("exp")?.as_i64()?;
    let iat = v.get("iat")?.as_i64()?;
    (exp > iat).then(|| Duration::from_secs((exp - iat) as u64))
}

/// How long after issuing to renew a token that lives `ttl`.
///
/// Renews `ttl/5` (clamped to 5s..120s) before expiry, but never later than half of the lifetime.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use brokapi_finam_protocol::auth::refresh_after;
///
/// // A 15 minute token is renewed 2 minutes before it ends.
/// assert_eq!(refresh_after(Duration::from_secs(900)), Duration::from_secs(780));
/// // A very short token is renewed at half of its life.
/// assert_eq!(refresh_after(Duration::from_secs(8)), Duration::from_secs(4));
/// ```
pub fn refresh_after(ttl: Duration) -> Duration {
    let lead = (ttl / 5).clamp(Duration::from_secs(5), Duration::from_secs(120));
    if ttl <= lead * 2 { ttl / 2 } else { ttl - lead }
}

struct State {
    token: Arc<str>,
    info: TokenInfo,
    refresh_at: Instant,
    /// Bumped on every successful refresh.
    generation: u64,
}

/// Holds the current JWT and keeps it fresh. Share via `Arc` between connectors.
///
/// A background task renews the token ahead of expiry (see [`refresh_after`] for the rule). If a renewal
/// fails, the old token stays in use and the task retries with a growing pause (1 s up to 30 s).
/// The task ends when the last `Arc` is dropped.
///
/// The `Debug` output hides the secret.
///
/// See the [module docs](self) for an example.
pub struct TokenManager {
    api: Arc<dyn TokenApi>,
    secret: String,
    source_app_id: String,
    state: RwLock<State>,
    wake: Arc<Notify>,
    refresh_lock: Mutex<()>,
}

impl fmt::Debug for TokenManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenManager")
            .field("secret", &"<redacted>")
            .field("generation", &self.generation())
            .finish()
    }
}

impl TokenManager {
    /// Authenticates once (failure here means a bad secret or no connectivity) and starts the
    /// background refresher.
    ///
    /// Must be called inside a Tokio runtime, because it spawns the refresher task.
    ///
    /// # Errors
    ///
    /// - [`FinamError::Config`]: the secret is empty or only spaces. Nothing is sent.
    /// - [`FinamError::Auth`]: the server answered with an empty token.
    /// - [`FinamError::Status`]: the `Auth` call failed (a wrong secret is `UNAUTHENTICATED`).
    ///
    /// A failure of `TokenDetails` is not an error: the lifetime then comes from the JWT claims, or
    /// from [`ASSUMED_TTL`].
    pub async fn start(
        api: Arc<dyn TokenApi>,
        secret: impl Into<String>,
        source_app_id: impl Into<String>,
    ) -> Result<Arc<Self>, FinamError> {
        let secret = secret.into();
        if secret.trim().is_empty() {
            return Err(FinamError::Config("empty secret".into()));
        }
        let source_app_id = source_app_id.into();
        let fresh = fetch(api.as_ref(), &secret, &source_app_id).await?;
        let manager = Arc::new(Self {
            api,
            secret,
            source_app_id,
            state: RwLock::new(fresh),
            wake: Arc::new(Notify::new()),
            refresh_lock: Mutex::new(()),
        });
        tokio::spawn(refresh_loop(Arc::downgrade(&manager)));
        Ok(manager)
    }

    /// Current JWT (what goes into the `authorization` metadata).
    ///
    /// Cheap: no network. Call it for every request instead of keeping the value.
    pub fn current(&self) -> Arc<str> {
        Arc::clone(&self.state.read().expect("token state poisoned").token)
    }

    /// Details of the current token: lifetime, accounts, read-only flag. No network.
    pub fn info(&self) -> TokenInfo {
        self.state
            .read()
            .expect("token state poisoned")
            .info
            .clone()
    }

    /// Accounts available to this secret (`TokenDetails.account_ids`).
    ///
    /// Empty when `TokenDetails` failed or listed none.
    pub fn account_ids(&self) -> Vec<String> {
        self.info().account_ids
    }

    /// Number of successful issues so far (1 after [`TokenManager::start`]).
    ///
    /// Changes every time the token is replaced, so it tells a reader whether its copy is stale.
    pub fn generation(&self) -> u64 {
        self.state.read().expect("token state poisoned").generation
    }

    /// Asks the refresher to renew now, e.g. after `UNAUTHENTICATED`. Non-blocking.
    ///
    /// The renewal runs in the background. Use [`TokenManager::refresh`] to wait for it.
    pub fn request_refresh(&self) {
        self.wake.notify_one();
    }

    /// Renews the token immediately.
    ///
    /// If another task renewed the token while this one waited for the lock, nothing more is sent
    /// and the call returns `Ok`. So parallel callers cause one `Auth`, not many.
    ///
    /// # Errors
    ///
    /// The same as [`TokenManager::start`] without the empty-secret check. The old token stays in
    /// use when the renewal fails.
    pub async fn refresh(&self) -> Result<(), FinamError> {
        let seen = self.generation();
        let _guard = self.refresh_lock.lock().await;
        if self.generation() != seen {
            // Someone else refreshed while we waited for the lock.
            return Ok(());
        }
        let mut fresh = fetch(self.api.as_ref(), &self.secret, &self.source_app_id).await?;
        let mut st = self.state.write().expect("token state poisoned");
        fresh.generation = st.generation + 1;
        *st = fresh;
        Ok(())
    }

    fn until_refresh(&self) -> Duration {
        let at = self.state.read().expect("token state poisoned").refresh_at;
        at.saturating_duration_since(Instant::now())
    }
}

async fn fetch(api: &dyn TokenApi, secret: &str, source_app_id: &str) -> Result<State, FinamError> {
    let started = Instant::now();
    let token = api.auth(secret, source_app_id).await?;
    if token.is_empty() {
        return Err(FinamError::Auth("server returned an empty token".into()));
    }
    let mut info = match api.token_details(&token).await {
        Ok(info) => info,
        Err(e) => {
            // The token itself is good; only the lifetime/account list is missing.
            warn!(error = %e, "TokenDetails failed, falling back to JWT claims");
            TokenInfo::default()
        }
    };
    if info.ttl.is_none() {
        info.ttl = jwt_ttl(&token);
    }
    let ttl = info.ttl.unwrap_or(ASSUMED_TTL);
    debug!(?ttl, "finam token issued");
    Ok(State {
        token: Arc::from(token),
        info,
        // Counted from the start of the Auth call: the server issued it somewhere inside it.
        refresh_at: started + refresh_after(ttl),
        generation: 1,
    })
}

async fn refresh_loop(weak: Weak<TokenManager>) {
    let mut backoff = RETRY_MIN;
    loop {
        // Wait without holding a strong reference, so dropping the last Arc ends this task.
        let (wait, wake) = {
            let Some(m) = weak.upgrade() else { return };
            (m.until_refresh(), Arc::clone(&m.wake))
        };
        if !wait.is_zero() {
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = wake.notified() => {}
            }
        }
        let Some(m) = weak.upgrade() else { return };
        match m.refresh().await {
            Ok(()) => backoff = RETRY_MIN,
            Err(e) => {
                warn!(error = %e, retry_in = ?backoff, "finam token refresh failed");
                drop(m);
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(RETRY_MAX);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    #[test]
    fn refresh_policy() {
        // 15 min token: renew 2 min before expiry.
        assert_eq!(
            refresh_after(Duration::from_secs(900)),
            Duration::from_secs(780)
        );
        // 60 s token: lead 12 s.
        assert_eq!(
            refresh_after(Duration::from_secs(60)),
            Duration::from_secs(48)
        );
        // Very short token: half of the lifetime.
        assert_eq!(
            refresh_after(Duration::from_secs(8)),
            Duration::from_secs(4)
        );
        // Long token: lead is capped at 120 s.
        assert_eq!(
            refresh_after(Duration::from_secs(86_400)),
            Duration::from_secs(86_280)
        );
    }

    fn jwt(iat: i64, exp: i64) -> String {
        let h = URL_SAFE_NO_PAD.encode(r#"{"alg":"HS256"}"#);
        let p = URL_SAFE_NO_PAD.encode(format!(r#"{{"iat":{iat},"exp":{exp}}}"#));
        format!("{h}.{p}.sig")
    }

    #[test]
    fn jwt_claims_give_ttl() {
        assert_eq!(jwt_ttl(&jwt(1000, 1900)), Some(Duration::from_secs(900)));
        assert_eq!(jwt_ttl("not-a-jwt"), None);
        assert_eq!(jwt_ttl(&jwt(2000, 1000)), None);
    }

    /// Fake auth server: numbered tokens, scripted lifetime and failures.
    struct FakeApi {
        issued: AtomicU64,
        ttl: Option<Duration>,
        details_fail: bool,
        fail_next_auths: StdMutex<u32>,
    }

    impl FakeApi {
        fn new(ttl: Option<Duration>) -> Arc<Self> {
            Arc::new(Self {
                issued: AtomicU64::new(0),
                ttl,
                details_fail: false,
                fail_next_auths: StdMutex::new(0),
            })
        }
    }

    #[async_trait]
    impl TokenApi for FakeApi {
        async fn auth(&self, _s: &str, _a: &str) -> Result<String, FinamError> {
            tokio::task::yield_now().await;
            {
                let mut f = self.fail_next_auths.lock().unwrap();
                if *f > 0 {
                    *f -= 1;
                    return Err(FinamError::Status {
                        code: tonic::Code::Unavailable,
                        message: "down".into(),
                    });
                }
            }
            let n = self.issued.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(format!("jwt-{n}"))
        }

        async fn token_details(&self, _t: &str) -> Result<TokenInfo, FinamError> {
            if self.details_fail {
                return Err(FinamError::Status {
                    code: tonic::Code::Internal,
                    message: "x".into(),
                });
            }
            Ok(TokenInfo {
                ttl: self.ttl,
                account_ids: vec!["A1".into(), "A2".into()],
                readonly: false,
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn token_is_renewed_before_expiry_not_after() {
        let api = FakeApi::new(Some(Duration::from_secs(900)));
        let m = TokenManager::start(api.clone(), "secret", "")
            .await
            .unwrap();
        assert_eq!(&*m.current(), "jwt-1");
        assert_eq!(m.account_ids(), vec!["A1", "A2"]);

        // Just before the renewal point the old token is still served.
        tokio::time::sleep(Duration::from_secs(770)).await;
        assert_eq!(&*m.current(), "jwt-1");

        // Past it (780 s) and still before expiry (900 s) the token has been replaced.
        tokio::time::sleep(Duration::from_secs(20)).await;
        assert_eq!(&*m.current(), "jwt-2");
        assert_eq!(m.generation(), 2);

        // And keeps being renewed.
        tokio::time::sleep(Duration::from_secs(780)).await;
        assert_eq!(&*m.current(), "jwt-3");
    }

    #[tokio::test(start_paused = true)]
    async fn failed_refresh_is_retried_and_old_token_kept() {
        let api = FakeApi::new(Some(Duration::from_secs(600)));
        let m = TokenManager::start(api.clone(), "secret", "")
            .await
            .unwrap();
        *api.fail_next_auths.lock().unwrap() = 2;
        // Renewal point at 480 s; first two attempts fail (1 s, 2 s backoff), third succeeds.
        tokio::time::sleep(Duration::from_secs(482)).await;
        assert_eq!(&*m.current(), "jwt-1", "old token kept while refresh fails");
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert_eq!(&*m.current(), "jwt-2");
    }

    #[tokio::test(start_paused = true)]
    async fn lifetime_falls_back_to_jwt_claims_then_assumption() {
        // TokenDetails fails, token is not a JWT: assumed 5 min -> renewal at 240 s.
        let api = Arc::new(FakeApi {
            issued: AtomicU64::new(0),
            ttl: None,
            details_fail: true,
            fail_next_auths: StdMutex::new(0),
        });
        let m = TokenManager::start(api, "secret", "").await.unwrap();
        assert!(m.account_ids().is_empty());
        tokio::time::sleep(Duration::from_secs(245)).await;
        assert_eq!(m.generation(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn request_refresh_renews_immediately() {
        let api = FakeApi::new(Some(Duration::from_secs(900)));
        let m = TokenManager::start(api, "secret", "").await.unwrap();
        m.request_refresh();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(&*m.current(), "jwt-2");
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_refresh_calls_issue_one_token() {
        let api = FakeApi::new(Some(Duration::from_secs(900)));
        let m = TokenManager::start(api.clone(), "secret", "")
            .await
            .unwrap();
        let (a, b) = tokio::join!(m.refresh(), m.refresh());
        a.unwrap();
        b.unwrap();
        assert_eq!(api.issued.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn refresher_stops_when_manager_is_dropped() {
        let api = FakeApi::new(Some(Duration::from_secs(60)));
        let m = TokenManager::start(api.clone(), "secret", "")
            .await
            .unwrap();
        drop(m);
        tokio::time::sleep(Duration::from_secs(600)).await;
        assert_eq!(
            api.issued.load(Ordering::SeqCst),
            1,
            "no refresh after drop"
        );
    }

    #[tokio::test]
    async fn empty_secret_is_a_config_error() {
        let api = FakeApi::new(None);
        let err = TokenManager::start(api, "  ", "").await.unwrap_err();
        assert!(matches!(err, FinamError::Config(_)));
    }

    #[tokio::test]
    async fn debug_hides_the_secret() {
        let m = TokenManager::start(FakeApi::new(None), "hunter2-secret", "")
            .await
            .unwrap();
        let shown = format!("{m:?}");
        assert!(!shown.contains("hunter2"), "{shown}");
    }
}
