//! Faults on real loopback sockets, through the public data and trading sessions.

use std::future::Future;
use std::time::Duration;

use brokapi_alor_protocol::config::AlorConfig;
use brokapi_alor_protocol::error::AlorApiError;
use brokapi_alor_protocol::http::client::AlorHttpClient;
use brokapi_alor_protocol::ws::cws::{CwsSession, opcodes};
use brokapi_alor_protocol::ws::models::{WsIncoming, WsSubscribe};
use brokapi_alor_protocol::ws::session::{WsOptions, WsSession};
use brokapi_mocks::time::jump;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

type Socket = WebSocketStream<TcpStream>;
const LIMIT: Duration = Duration::from_secs(5);

// The HTTP token issuer stays alive for the whole test. Socket scripts are joined so a server-side
// assertion cannot silently turn into a passing client-side error test.
struct Wire {
    http: AlorHttpClient,
    listener: TcpListener,
    _auth: mockito::ServerGuard,
}

impl Wire {
    async fn start() -> Self {
        let mut auth = mockito::Server::new_async().await;
        auth.mock("POST", "/refresh")
            .with_body(r#"{"AccessToken":"wire-token"}"#)
            .create_async()
            .await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut cfg = AlorConfig::for_mock("refresh", auth.url());
        cfg.ws_base_url = format!("ws://{}", listener.local_addr().unwrap());
        Self {
            http: AlorHttpClient::new(cfg).unwrap(),
            listener,
            _auth: auth,
        }
    }
}

fn serve_on<F, Fut>(listener: TcpListener, script: F) -> JoinHandle<()>
where
    F: FnOnce(Socket) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        let (stream, _) = timeout(LIMIT, listener.accept()).await.unwrap().unwrap();
        let socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        script(socket).await;
    })
}

async fn request(socket: &mut Socket) -> Value {
    loop {
        match timeout(LIMIT, socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
        {
            Message::Text(text) => return serde_json::from_str(&text).unwrap(),
            Message::Ping(data) => socket.send(Message::Pong(data)).await.unwrap(),
            other => panic!("expected a request, got {other:?}"),
        }
    }
}

async fn reply(socket: &mut Socket, req: &Value, code: u16) {
    socket
        .send(Message::Text(
            json!({"requestGuid": req["guid"], "httpCode": code,
                "message": "wire reply", "orderNumber": "order-17"})
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
}

async fn authorize(socket: &mut Socket) {
    let req = request(socket).await;
    assert_eq!(req["opcode"], "authorize");
    assert_eq!(req["token"], "wire-token");
    reply(socket, &req, 200).await;
}

async fn join(task: JoinHandle<()>) {
    timeout(LIMIT, task).await.unwrap().unwrap();
}

fn closed(result: Result<brokapi_alor_protocol::ws::cws::CwsReply, AlorApiError>) {
    match result {
        Err(AlorApiError::Ws {
            code: None,
            message,
        }) => {
            assert!(message.contains("closed before the answer"), "{message}");
        }
        other => panic!("a lost answer must be a transport error: {other:?}"),
    }
}

#[tokio::test]
async fn a_command_ignores_unrelated_frames_and_waits_for_its_own_reply() {
    let wire = Wire::start().await;
    let task = serve_on(wire.listener, |mut socket| async move {
        authorize(&mut socket).await;
        let req = request(&mut socket).await;
        assert_eq!(req["opcode"], opcodes::CREATE_LIMIT);
        assert_eq!(req["quantity"], 2);
        for frame in [
            Message::Text("not json".into()),
            Message::Binary(vec![0, 1].into()),
            Message::Text(
                json!({"requestGuid":"another-command","httpCode":400})
                    .to_string()
                    .into(),
            ),
            Message::Ping(vec![7, 8].into()),
        ] {
            socket.send(frame).await.unwrap();
        }
        // A ping must be answered even while waiting for the command's answer.
        assert_eq!(
            timeout(LIMIT, socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            Message::Pong(vec![7, 8].into())
        );
        reply(&mut socket, &req, 200).await;
    });
    let cws = CwsSession::connect(wire.http, LIMIT).await.unwrap();
    let answer = cws
        .command(opcodes::CREATE_LIMIT, json!({"quantity":2}))
        .await
        .unwrap();
    assert_eq!(answer.code, 200);
    assert_eq!(answer.order_number.as_deref(), Some("order-17"));
    assert_eq!(answer.message, "wire reply");
    assert_eq!(answer.raw["requestGuid"], answer.guid);
    join(task).await;
}

#[tokio::test]
async fn an_authorization_connection_closed_without_a_reply_is_an_error() {
    let wire = Wire::start().await;
    let task = serve_on(wire.listener, |mut socket| async move {
        assert_eq!(request(&mut socket).await["opcode"], "authorize");
        socket.send(Message::Close(None)).await.unwrap();
    });
    match CwsSession::connect(wire.http, LIMIT).await {
        Err(AlorApiError::Ws {
            code: None,
            message,
        }) => {
            assert!(message.contains("closed before the answer"), "{message}");
        }
        _ => panic!("a closed authorization channel must not connect"),
    }
    join(task).await;
}

async fn command_loses_its_answer(close_frame: bool) {
    let wire = Wire::start().await;
    let task = serve_on(wire.listener, move |mut socket| async move {
        authorize(&mut socket).await;
        let req = request(&mut socket).await;
        assert_eq!(req["opcode"], opcodes::CREATE_LIMIT);
        if close_frame {
            socket.send(Message::Close(None)).await.unwrap();
        }
        // Dropping without Close produces an abrupt EOF instead of a WebSocket close handshake.
    });
    let cws = CwsSession::connect(wire.http, LIMIT).await.unwrap();
    closed(
        cws.command(opcodes::CREATE_LIMIT, json!({"quantity":1}))
            .await,
    );
    join(task).await;
}

#[tokio::test]
async fn a_close_before_the_command_answer_is_an_unknown_outcome() {
    command_loses_its_answer(true).await;
}

#[tokio::test]
async fn an_abrupt_eof_before_the_command_answer_is_an_unknown_outcome() {
    command_loses_its_answer(false).await;
}

#[tokio::test]
async fn a_silent_command_times_out_without_being_sent_again() {
    let wire = Wire::start().await;
    let (seen_tx, seen_rx) = oneshot::channel();
    let task = serve_on(wire.listener, |mut socket| async move {
        authorize(&mut socket).await;
        assert_eq!(request(&mut socket).await["opcode"], opcodes::CREATE_LIMIT);
        seen_tx.send(()).unwrap();
        // The client drops after the timeout. Any text here would be a repeated trading command.
        match socket.next().await {
            None | Some(Err(_)) | Some(Ok(Message::Close(_))) => {}
            other => panic!("the unanswered command was sent again: {other:?}"),
        }
    });
    let cws = CwsSession::connect(wire.http, Duration::from_secs(60))
        .await
        .unwrap();
    let command = tokio::spawn(async move {
        cws.command(opcodes::CREATE_LIMIT, json!({"quantity":1}))
            .await
    });
    timeout(LIMIT, seen_rx).await.unwrap().unwrap();
    // The command is already on the server. Only now advance its answer deadline.
    jump(Duration::from_secs(61)).await;
    match timeout(LIMIT, command).await.unwrap().unwrap() {
        Err(AlorApiError::Ws {
            code: None,
            message,
        }) => assert_eq!(message, "no answer in time"),
        other => panic!("expected the answer deadline, got {other:?}"),
    }
    join(task).await;
}

async fn data_subscription_survives_fault(invalid_frame: bool) {
    let wire = Wire::start().await;
    let task = tokio::spawn(async move {
        let mut requests = Vec::new();
        for attempt in 0..2 {
            let (stream, _) = timeout(LIMIT, wire.listener.accept())
                .await
                .unwrap()
                .unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let req = request(&mut socket).await;
            assert_eq!(req["opcode"], "OrderBookGetAndSubscribe");
            assert_eq!(req["token"], "wire-token");
            requests.push(req.clone());
            if attempt == 0 {
                if invalid_frame {
                    socket.get_mut().write_all(&[0x83, 0]).await.unwrap();
                }
                continue;
            }
            assert_eq!(requests[0], requests[1], "restore the same subscription");
            socket
                .send(Message::Text(
                    json!({"guid":req["guid"], "data":{"bids":[],"asks":[]}})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            // Stay connected until the test drops the session. A queued copy of the restored
            // subscription must not be sent a second time on this connection.
            match timeout(LIMIT, socket.next()).await.unwrap() {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => {}
                other => panic!("the restored subscription was sent twice: {other:?}"),
            }
        }
    });
    let (tx, rx) = kanal::bounded_async(16);
    let session = WsSession::spawn_with(
        wire.http,
        tx,
        WsOptions {
            announce_connects: true,
            reconnect_min: Duration::from_millis(10),
            reconnect_max: Duration::from_millis(20),
        },
    );
    session
        .subscribe(WsSubscribe::order_book(
            "book-1",
            "MOEX",
            "SBER",
            Some("TQBR"),
            10,
        ))
        .await
        .unwrap();
    for _ in 0..2 {
        assert!(matches!(
            timeout(LIMIT, rx.recv()).await.unwrap().unwrap(),
            WsIncoming::Reconnected
        ));
    }
    match timeout(LIMIT, rx.recv()).await.unwrap().unwrap() {
        WsIncoming::Data { guid, data } => {
            assert_eq!(guid, "book-1");
            assert_eq!(data, json!({"bids":[],"asks":[]}));
        }
        other => panic!("expected data from the restored subscription, got {other:?}"),
    }
    assert!(session.is_connected());
    drop(session);
    join(task).await;
}

#[tokio::test]
async fn an_abrupt_eof_restores_the_data_subscription_and_announces_the_gap() {
    data_subscription_survives_fault(false).await;
}

#[tokio::test]
async fn an_invalid_frame_restores_the_data_subscription_and_announces_the_gap() {
    data_subscription_survives_fault(true).await;
}
