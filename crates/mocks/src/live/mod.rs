//! Answers of the live brokers as reference files, and the check that the imitations answer the same way.
//!
//! The imitations and the connectors were written by one author from one reading of the broker
//! documentation. If that reading is wrong, the code and its test are wrong in the same way and stay
//! green. The way out is to compare the imitation with the real thing. This module holds the pieces:
//!
//! 1. **Record.** A live run calls a fixed list of *probes* (a read of the accounts, of an order book,
//!    of an unknown order, ...) against the broker. [`Recorder`] cleans each answer with [`scrub()`]
//!    (tokens, account and order numbers, names and other personal data never reach the disk) and
//!    writes it to `crates/<broker>/tests/fixtures/live/<probe>.json`.
//! 2. **Compare.** An ordinary test without network runs the same probes against the imitation and
//!    calls [`check`]: every field of the live answer must be there with the same type, the imitation
//!    must not add fields the broker does not send, an error must have the same code. Values are not
//!    compared, only the shape.
//! 3. **Skip.** While there is no reference file, the comparison does nothing and says so
//!    ("no live reference, run the live run first"). Once a file appears, the same test starts checking.
//!
//! How to make the live run and where the files go: the page "Live runs" of the book.

pub mod bcs;
pub mod finam;
pub mod fixture;
pub mod scrub;
pub mod shape;
pub mod tbank;
pub mod tree;

pub use fixture::{Fixture, NoReference, Outcome, Recorder, by_name, check, load_dir};
pub use scrub::scrub;
pub use shape::{Mismatch, Shape, compare};
