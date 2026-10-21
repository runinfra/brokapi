//! Every status of the broker has its public error, for a read and for a command.

use brokapi_core::{Error, NOT_FOUND};
use brokapi_finam::error::{code_name, map_command_err, map_protocol_err, transport, unsupported};
use brokapi_finam_protocol::FinamError;
use tonic::Code;

const ALL: [Code; 17] = [
    Code::Ok,
    Code::Cancelled,
    Code::Unknown,
    Code::InvalidArgument,
    Code::DeadlineExceeded,
    Code::NotFound,
    Code::AlreadyExists,
    Code::PermissionDenied,
    Code::ResourceExhausted,
    Code::FailedPrecondition,
    Code::Aborted,
    Code::OutOfRange,
    Code::Unimplemented,
    Code::Internal,
    Code::Unavailable,
    Code::DataLoss,
    Code::Unauthenticated,
];

fn status(code: Code) -> FinamError {
    FinamError::Status {
        code,
        message: "m".into(),
    }
}

#[test]
fn every_code_has_a_name_and_not_found_has_the_common_one() {
    let names: Vec<_> = ALL.iter().map(|c| code_name(*c)).collect();
    let mut unique = names.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), ALL.len(), "every name is its own: {names:?}");
    assert_eq!(code_name(Code::NotFound), NOT_FOUND);
    assert_eq!(code_name(Code::Unauthenticated), "unauthenticated");
}

#[test]
fn a_read_gets_the_kind_of_error_its_status_deserves() {
    for code in ALL {
        let mapped = map_protocol_err(&status(code));
        match code {
            Code::ResourceExhausted => assert!(matches!(mapped, Error::RateLimited { .. })),
            Code::Unauthenticated => assert!(matches!(mapped, Error::Config(_))),
            Code::PermissionDenied
            | Code::NotFound
            | Code::InvalidArgument
            | Code::FailedPrecondition
            | Code::OutOfRange
            | Code::AlreadyExists
            | Code::Unimplemented => {
                assert!(matches!(mapped, Error::Rejected { .. }), "{code:?}")
            }
            _ => assert!(
                matches!(mapped, Error::Transport(_)),
                "{code:?}: {mapped:?}"
            ),
        }
    }
    assert!(matches!(
        map_protocol_err(&FinamError::Config("c".into())),
        Error::Config(_)
    ));
    assert!(matches!(
        map_protocol_err(&FinamError::Auth("a".into())),
        Error::Config(_)
    ));
}

#[test]
fn a_command_that_may_have_been_applied_is_unknown() {
    for code in ALL {
        let mapped = map_command_err(&status(code));
        let definite = matches!(
            code,
            Code::ResourceExhausted
                | Code::Unauthenticated
                | Code::PermissionDenied
                | Code::NotFound
                | Code::InvalidArgument
                | Code::FailedPrecondition
                | Code::OutOfRange
                | Code::AlreadyExists
                | Code::Unimplemented
        );
        if !definite {
            assert!(matches!(mapped, Error::Unknown), "{code:?}: {mapped:?}");
        }
    }
    // A local failure is not a hidden outcome.
    assert!(matches!(
        map_command_err(&FinamError::Config("c".into())),
        Error::Config(_)
    ));
}

#[test]
fn the_two_little_builders_make_their_errors() {
    assert!(matches!(transport("down"), Error::Transport(_)));
    assert!(
        matches!(unsupported("stop orders"), Error::Unsupported { capability } if capability == "stop orders")
    );
}
