use super::*;
use brokapi_mocks::io::FaultyWrite;
use tokio::io::AsyncReadExt;
use tokio_tungstenite::tungstenite::protocol::Role;

async fn command_write_is_not_retried(after: usize) {
    let (client, mut peer) = tokio::io::duplex(4096);
    let (client, fault) = FaultyWrite::new(client);
    let stream = Mutex::new(WebSocketStream::from_raw_socket(client, Role::Client, None).await);
    fault.fail_after(after);
    let error = CwsSession::exchange_on(
        &stream,
        Duration::from_secs(5),
        json!({"opcode": opcodes::CREATE_LIMIT, "quantity": 1, "price": 100}),
    )
    .await
    .unwrap_err();
    match error {
        AlorApiError::Ws {
            code: None,
            message,
        } => {
            assert!(message.starts_with("write:"), "{message}");
            assert!(message.contains("simulated write failure"), "{message}");
        }
        other => panic!("a failed command write is an unknown outcome: {other:?}"),
    }
    assert_eq!(fault.written(), after);
    assert_eq!(fault.failures(), 1, "no blind retry of a trading command");
    drop(stream);
    let mut received = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), peer.read_to_end(&mut received))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.len(), after);
    if after != 0 {
        assert_eq!(
            received[0] & 0x0f,
            1,
            "a partial text frame, not another command"
        );
    }
}

#[tokio::test]
async fn a_command_write_failure_before_the_first_byte_is_reported_without_a_retry() {
    command_write_is_not_retried(0).await;
}

#[tokio::test]
async fn a_command_write_failure_inside_the_header_is_reported_without_a_retry() {
    command_write_is_not_retried(1).await;
}

#[tokio::test]
async fn a_command_write_failure_inside_the_payload_is_reported_without_a_retry() {
    command_write_is_not_retried(12).await;
}
