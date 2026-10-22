//! Edges of the Finam protocol client: what the codes of a refusal say, the endpoint that is not an address, the token
//! manager given an empty token, and every service the client offers.

use std::sync::Arc;

use async_trait::async_trait;
use brokapi_finam_protocol::client::connect_lazy;
use brokapi_finam_protocol::{
    ApiEndpoint, FinamApi, FinamError, TokenApi, TokenInfo, TokenManager,
};
use tonic::Code;

#[test]
fn the_codes_of_refusals_are_told_apart_and_a_failure_of_the_setup_has_no_code() {
    let status = |code| FinamError::from(tonic::Status::new(code, "x"));
    assert!(status(Code::ResourceExhausted).is_rate_limited());
    assert!(!status(Code::Unavailable).is_rate_limited());
    assert!(status(Code::Unauthenticated).is_unauthenticated());
    assert_eq!(status(Code::NotFound).code(), Some(Code::NotFound));
    for setup in [
        FinamError::Config("bad".into()),
        FinamError::Auth("no".into()),
    ] {
        assert_eq!(setup.code(), None);
        assert!(!setup.is_rate_limited() && !setup.is_unauthenticated());
    }
}

#[tokio::test]
async fn a_secure_address_gets_a_tls_channel_and_garbage_is_refused() {
    assert!(connect_lazy(&ApiEndpoint::new("https://127.0.0.1:1")).is_ok());
    assert!(matches!(
        connect_lazy(&ApiEndpoint::new("not a url")),
        Err(FinamError::Config(_))
    ));
}

/// A server that logs in and gives an empty token.
struct EmptyToken;

#[async_trait]
impl TokenApi for EmptyToken {
    async fn auth(&self, _: &str, _: &str) -> Result<String, FinamError> {
        Ok(String::new())
    }
    async fn token_details(&self, _: &str) -> Result<TokenInfo, FinamError> {
        Ok(TokenInfo::default())
    }
}

#[tokio::test]
async fn an_empty_token_from_the_server_is_an_authentication_error() {
    match TokenManager::start(Arc::new(EmptyToken), "secret", "").await {
        Err(FinamError::Auth(message)) => assert!(message.contains("empty"), "{message}"),
        Err(other) => panic!("{other:?}"),
        Ok(_) => panic!("an empty token was taken"),
    }
}

#[tokio::test]
async fn the_client_offers_every_service_of_the_broker_on_one_channel() {
    let server = brokapi_mocks::finam::MockServer::start().await;
    let ep = ApiEndpoint::new(server.url.clone());
    let tokens = brokapi_finam_protocol::start_token_manager(&ep, brokapi_mocks::finam::SECRET, "")
        .await
        .unwrap();
    let channel = connect_lazy(&ep).unwrap();
    let api = FinamApi::from_channel(channel, Arc::clone(&tokens), true);
    assert!(Arc::ptr_eq(api.tokens(), &tokens));
    // Each accessor makes a client of its own service on the shared channel.
    let _ = (
        api.grpc(),
        api.orders(),
        api.accounts(),
        api.market_data(),
        api.assets(),
        api.usage_metrics(),
        api.auth(),
        api.reports(),
        api.corporate_actions(),
    );
    // And one of them really talks: the schedule of an instrument.
    let mut assets = api.assets();
    let schedule = assets
        .schedule(tonic::Request::new(
            brokapi_finam_protocol::pb::grpc::tradeapi::v1::assets::ScheduleRequest {
                symbol: "SBER@MISX".into(),
            },
        ))
        .await
        .unwrap();
    assert!(!schedule.into_inner().sessions.is_empty());
}
