//! The supervisor of a gRPC stream: what it tells the connector when a stream cannot be opened, and how often it tries.
//! The opening function is the test's own, so no broker is needed for the stream itself; the token manager is the one of
//! the imitation.

mod mock_server;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use brokapi_core::SubscriptionId;
use brokapi_finam::core::StreamEvent;
use brokapi_finam::stream::{OpenFn, StreamParams, spawn_stream};
use brokapi_finam_protocol::TokenManager;
use brokapi_mocks::time::{jump, wait_until};
use brokapi_runtime::{BrokerLimiter, BrokerLimits};
use futures_util::FutureExt;
use mock_server::{MockServer, api, limiter};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tonic::{Code, Status};

#[derive(Debug, PartialEq)]
enum Seen {
    Connected,
    Disconnected(String),
    Failed(String),
}

struct Rig {
    rx: mpsc::Receiver<Seen>,
    opens: Arc<AtomicU32>,
    tokens: Arc<TokenManager>,
    first: oneshot::Receiver<Result<(), brokapi_finam_protocol::FinamError>>,
    handle: brokapi_finam::stream::StreamHandle,
    cancel: CancellationToken,
    _server: MockServer,
}

/// A supervisor whose every attempt to open ends with `answer`.
async fn rig_with(
    answer: impl Fn() -> Option<Status> + Send + Sync + 'static,
    open_timeout: Duration,
    limits: Option<Arc<BrokerLimiter>>,
) -> Rig {
    let server = MockServer::start().await;
    let tokens = api(&server).shared_tokens().await.unwrap();
    let (tx, rx) = mpsc::channel(64);
    let (first_tx, first) = oneshot::channel();
    let opens = Arc::new(AtomicU32::new(0));
    let counted = Arc::clone(&opens);
    let open: OpenFn<()> = Arc::new(move || {
        counted.fetch_add(1, Ordering::SeqCst);
        match answer() {
            Some(status) => async move { Err(status) }.boxed(),
            None => futures_util::future::pending().boxed(),
        }
    });
    let cancel = CancellationToken::new();
    let handle = spawn_stream(
        StreamParams {
            sub: SubscriptionId::new(1),
            method: "SubscribeOrders",
            limiter: limits.unwrap_or_else(limiter),
            tokens: Arc::clone(&tokens),
            open,
            open_timeout,
            wrap: Arc::new(|ev: StreamEvent<()>| match ev {
                StreamEvent::Connected => Seen::Connected,
                StreamEvent::Disconnected(why) => Seen::Disconnected(why),
                StreamEvent::Failed(why) => Seen::Failed(why),
                StreamEvent::Msg(()) => unreachable!("no message is ever made"),
            }),
            tx,
            first: Some(first_tx),
        },
        cancel.clone(),
    );
    Rig {
        rx,
        opens,
        tokens,
        first,
        handle,
        cancel,
        _server: server,
    }
}

async fn recv(rx: &mut mpsc::Receiver<Seen>) -> Seen {
    rx.recv().await.expect("the supervisor tells what happened")
}

#[tokio::test]
async fn a_stream_that_cannot_be_reached_is_reported_once_and_retried_with_growing_pauses() {
    let mut rig = rig_with(
        || Some(Status::unavailable("broker is away")),
        Duration::from_secs(5),
        None,
    )
    .await;
    assert!(
        matches!(recv(&mut rig.rx).await, Seen::Disconnected(why) if why.contains("broker is away"))
    );
    wait_until("the first attempt", || {
        rig.opens.load(Ordering::SeqCst) >= 1
    })
    .await;
    // The pauses are 0.5 s, 1 s, 2 s: a jump over each makes the next attempt.
    for (n, pause) in [500u64, 1000, 2000].into_iter().enumerate() {
        jump(Duration::from_millis(pause + 10)).await;
        let want = n as u32 + 2;
        wait_until("another attempt", || {
            rig.opens.load(Ordering::SeqCst) >= want
        })
        .await;
    }
    // Nothing more was reported: the same outage is told once.
    assert!(rig.rx.try_recv().is_err());
    rig.cancel.cancel();
    assert!(rig.handle.stop(Duration::from_secs(1)).await);
}

#[tokio::test]
async fn a_refusal_of_the_broker_reaches_the_caller_of_subscribe_and_the_stream_is_retried_slowly()
{
    let mut rig = rig_with(
        || Some(Status::permission_denied("no rights")),
        Duration::from_secs(5),
        None,
    )
    .await;
    let outcome = (&mut rig.first).await.unwrap();
    assert!(outcome.is_err(), "{outcome:?}");
    assert!(matches!(recv(&mut rig.rx).await, Seen::Failed(why) if why.contains("no rights")));
    // Half a minute is the pause of a refusal.
    jump(Duration::from_secs(31)).await;
    wait_until("a slow retry", || rig.opens.load(Ordering::SeqCst) >= 2).await;
    rig.handle.abort();
}

#[tokio::test]
async fn an_unauthenticated_answer_asks_for_a_new_token() {
    let rig = rig_with(
        || Some(Status::unauthenticated("token lapsed")),
        Duration::from_secs(5),
        None,
    )
    .await;
    let before = rig.tokens.generation();
    wait_until("a new token is made", || rig.tokens.generation() > before).await;
    rig.handle.abort();
}

#[tokio::test]
async fn an_open_that_never_answers_is_a_failure_after_its_timeout() {
    let mut rig = rig_with(|| None, Duration::from_secs(3), None).await;
    wait_until("the first attempt", || {
        rig.opens.load(Ordering::SeqCst) >= 1
    })
    .await;
    jump(Duration::from_secs(4)).await;
    match recv(&mut rig.rx).await {
        Seen::Disconnected(why) => assert!(why.contains("not opened"), "{why}"),
        other => panic!("{other:?}"),
    }
    rig.handle.abort();
}

#[tokio::test]
async fn a_spent_quota_delays_the_open_instead_of_failing_it() {
    let limits = BrokerLimits::from_toml_str(
        r#"
        [finam.buckets.tiny]
        rate = 1
        per = "10s"
        burst = 1
        [finam.route]
        "*" = ["tiny"]
        [finam.conns]
        max_conns = 1
        max_subs_per_conn = 100
        supports_multiplex = true
        "#,
    )
    .unwrap();
    let limiter = BrokerLimiter::new(&limits);
    // The only token is spent before the supervisor starts.
    limiter
        .acquire("SubscribeOrders", brokapi_runtime::now())
        .unwrap();
    let rig = rig_with(
        || Some(Status::unavailable("x")),
        Duration::from_secs(5),
        Some(limiter),
    )
    .await;
    // While the quota is spent, the supervisor does not call the open function.
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    assert_eq!(rig.opens.load(Ordering::SeqCst), 0);
    jump(Duration::from_secs(11)).await;
    wait_until("the open after the wait", || {
        rig.opens.load(Ordering::SeqCst) >= 1
    })
    .await;
    rig.handle.abort();
}

#[tokio::test]
async fn a_supervisor_whose_reader_is_gone_ends_by_itself() {
    let rig = rig_with(
        || Some(Status::new(Code::Unavailable, "x")),
        Duration::from_secs(5),
        None,
    )
    .await;
    let Rig { rx, handle, .. } = rig;
    drop(rx);
    // The report of the outage has nowhere to go: the task ends, and `stop` finds it ended.
    assert!(handle.stop(Duration::from_secs(5)).await);
}
