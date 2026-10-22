//! Edges of the T-Bank protocol client: headers that cannot be carried, the headers of every call, the environment of a
//! program and every service the client offers.

use brokapi_tbank_protocol::client::AuthInterceptor;
use brokapi_tbank_protocol::{ProtocolError, TbankClient, TbankConfig};
use tonic::service::Interceptor;

#[test]
fn a_token_or_a_name_a_header_cannot_carry_is_a_configuration_error_that_does_not_repeat_the_token()
{
    for bad_token in ["line\nbreak", "a\u{7f}b"] {
        match AuthInterceptor::new(bad_token, None).err() {
            Some(ProtocolError::Config(message)) => {
                assert!(!message.contains(bad_token), "{message}")
            }
            other => panic!("{other:?}"),
        }
    }
    assert!(matches!(
        AuthInterceptor::new("t.token", Some("bad\nname")).err(),
        Some(ProtocolError::Config(message)) if message.contains("app_name")
    ));
}

#[test]
fn every_call_carries_the_token_and_the_name_of_the_program_when_it_has_one() {
    let mut plain = AuthInterceptor::new("t.token", None).unwrap();
    let request = plain.call(tonic::Request::new(())).unwrap();
    assert_eq!(
        request.metadata().get("authorization").unwrap(),
        "Bearer t.token"
    );
    assert!(request.metadata().get("x-app-name").is_none());
    assert!(
        request
            .metadata()
            .get("authorization")
            .unwrap()
            .is_sensitive()
    );

    let mut named = AuthInterceptor::new("t.token", Some("me.my-robot")).unwrap();
    let request = named.call(tonic::Request::new(())).unwrap();
    assert_eq!(request.metadata().get("x-app-name").unwrap(), "me.my-robot");
}

/// The only test of this file that reads the environment: the variables are process-wide.
#[test]
fn the_environment_names_the_token_the_contour_the_endpoint_and_the_account() {
    // Nothing is set: no token, and the call does not fall back to a default.
    assert!(matches!(
        TbankConfig::from_env(),
        Err(ProtocolError::Config(_))
    ));

    let dir = std::env::temp_dir().join(format!("brokapi-tbank-env-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join(".env");
    std::fs::write(
        &file,
        "TBANK_TOKEN=t.from-file\nTBANK_SANDBOX=yes\nTBANK_ACCOUNT_ID=acc-1\n",
    )
    .unwrap();
    let cfg = TbankConfig::from_env_file(&file).unwrap();
    assert_eq!(cfg.account_id.as_deref(), Some("acc-1"));
    assert_eq!(cfg.endpoint, TbankConfig::sandbox("x").endpoint);
    // `from_env` reads the same variables, which are set by now.
    assert_eq!(
        TbankConfig::from_env().unwrap().account_id.as_deref(),
        Some("acc-1")
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn the_client_offers_every_service_of_the_broker_on_one_channel() {
    let cfg = TbankConfig::sandbox("t.token").with_app_name("me.my-robot");
    assert_eq!(cfg.app_name.as_deref(), Some("me.my-robot"));
    let client = TbankClient::new(&cfg).unwrap();
    // Each accessor makes a client of its own service on the shared channel; the channel connects lazily.
    let _ = (
        client.grpc(),
        client.market_data(),
        client.market_data_stream(),
        client.orders(),
        client.orders_stream(),
        client.operations(),
        client.operations_stream(),
        client.users(),
        client.instruments(),
        client.sandbox(),
        client.stop_orders(),
        client.signals(),
    );
}
