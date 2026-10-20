//! Reference files: recording the answers of a live broker and checking the imitations against them.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use super::scrub::{remove_secrets, scrub};
use super::shape::{Shape, compare};

/// What a probe got from a broker.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    /// The broker answered. The value is the answer: the JSON of BCS, or the message of a gRPC broker
    /// turned into JSON by [`tree::of`](super::tree::of).
    Answered(Value),
    /// The broker refused. `code` is the HTTP status (`"http_404"`) or the name of the gRPC code
    /// (`"NotFound"`), whichever the broker speaks. `body` is what came with the refusal, if anything.
    Refused {
        /// The status or the code.
        code: String,
        /// The body of the refusal, when the broker sends one.
        body: Option<Value>,
    },
}

/// One reference file: the cleaned outcome of a probe at a broker.
#[derive(Debug, Clone, PartialEq)]
pub struct Fixture {
    /// `bcs`, `tbank` or `finam`.
    pub broker: String,
    /// The name of the probe, also the name of the file without `.json`.
    pub probe: String,
    /// What the broker said, cleaned.
    pub outcome: Outcome,
}

impl Fixture {
    fn to_json(&self) -> Value {
        match &self.outcome {
            Outcome::Answered(body) => json!({
                "broker": self.broker, "probe": self.probe, "outcome": "answered", "body": body,
            }),
            Outcome::Refused { code, body } => json!({
                "broker": self.broker, "probe": self.probe, "outcome": "refused", "code": code, "body": body,
            }),
        }
    }

    fn from_json(v: &Value) -> Result<Self, String> {
        let text = |k: &str| {
            v[k].as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("no `{k}`"))
        };
        let outcome = match text("outcome")?.as_str() {
            "answered" => Outcome::Answered(v["body"].clone()),
            "refused" => Outcome::Refused {
                code: text("code")?,
                body: Some(v["body"].clone()).filter(|b| !b.is_null()),
            },
            other => return Err(format!("outcome `{other}` is not `answered` or `refused`")),
        };
        Ok(Self {
            broker: text("broker")?,
            probe: text("probe")?,
            outcome,
        })
    }
}

/// Writes the answers of a live run to `<dir>/<probe>.json`, cleaned.
///
/// Cleaning is [`scrub`] plus, as the last step, the removal of the secrets of the run from the text: pass the
/// tokens and the account of the run to [`with_secrets`](Self::with_secrets).
///
/// # Examples
///
/// ```
/// use brokapi_mocks::live::{Outcome, Recorder, load_dir};
/// use serde_json::json;
///
/// let dir = std::env::temp_dir().join(format!("brokapi-doc-recorder-{}", std::process::id()));
/// let recorder = Recorder::new("tbank", &dir).with_secrets(["t.my-real-token"]);
/// recorder.record("get_accounts", &Outcome::Answered(json!({
///     "accounts": [{"id": "2000123456", "type": 1, "status": 2, "name": "Ivan's account"}],
///     "hint": "token t.my-real-token was accepted",
/// })));
///
/// let text = std::fs::read_to_string(dir.join("get_accounts.json"))?;
/// assert!(!text.contains("2000123456") && !text.contains("t.my-real-token") && !text.contains("Ivan"));
/// assert_eq!(load_dir(&dir).len(), 1);
/// # std::fs::remove_dir_all(&dir)?;
/// # Ok::<(), std::io::Error>(())
/// ```
#[derive(Debug, Clone)]
pub struct Recorder {
    broker: String,
    dir: PathBuf,
    secrets: Vec<String>,
}

impl Recorder {
    /// A recorder of `broker` that writes into `dir` (made if it is not there).
    pub fn new(broker: &str, dir: impl AsRef<Path>) -> Self {
        Self {
            broker: broker.to_owned(),
            dir: dir.as_ref().to_owned(),
            secrets: Vec::new(),
        }
    }

    /// The secrets of this run: tokens, the account, anything that must not be in a file. Each is cut
    /// out of the text before it is written.
    #[must_use]
    pub fn with_secrets<S: AsRef<str>>(mut self, secrets: impl IntoIterator<Item = S>) -> Self {
        self.secrets
            .extend(secrets.into_iter().map(|s| s.as_ref().to_owned()));
        self
    }

    /// Cleans `outcome` and writes it. Returns the path of the file.
    ///
    /// # Panics
    ///
    /// When the file cannot be written: a live run that records nothing has failed at its job.
    pub fn record(&self, probe: &str, outcome: &Outcome) -> PathBuf {
        let mut fixture = Fixture {
            broker: self.broker.clone(),
            probe: probe.to_owned(),
            outcome: outcome.clone(),
        };
        match &mut fixture.outcome {
            Outcome::Answered(body) => scrub(body),
            Outcome::Refused { body, .. } => body.iter_mut().for_each(scrub),
        }
        let mut text = serde_json::to_string_pretty(&fixture.to_json()).expect("JSON is text");
        text = remove_secrets(&text, &self.secrets);
        text.push('\n');
        fs::create_dir_all(&self.dir).unwrap_or_else(|e| panic!("cannot make {:?}: {e}", self.dir));
        let path = self.dir.join(format!("{probe}.json"));
        fs::write(&path, text).unwrap_or_else(|e| panic!("cannot write {path:?}: {e}"));
        path
    }
}

/// All reference files of `dir`, by name. A folder that is not there gives an empty list: no live run yet.
///
/// # Panics
///
/// On a file that is not a reference file (damaged by hand): the check must not skip it silently.
#[must_use]
pub fn load_dir(dir: impl AsRef<Path>) -> Vec<Fixture> {
    let Ok(entries) = fs::read_dir(dir.as_ref()) else {
        return Vec::new();
    };
    let mut files: Vec<PathBuf> = entries
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    files.sort();
    files
        .iter()
        .map(|path| {
            let text = fs::read_to_string(path).unwrap_or_else(|e| panic!("{path:?}: {e}"));
            let value: Value =
                serde_json::from_str(&text).unwrap_or_else(|e| panic!("{path:?} is not JSON: {e}"));
            Fixture::from_json(&value).unwrap_or_else(|e| panic!("{path:?}: {e}"))
        })
        .collect()
}

/// The result of [`check`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    /// How many reference files were compared.
    pub compared: usize,
    /// What differs, one line each, with the probe in front.
    pub problems: Vec<String>,
}

/// The text a test prints while there is nothing to compare with. Print it with `println!("{}", NoReference(broker))`.
///
/// # Examples
///
/// ```
/// use brokapi_mocks::live::NoReference;
///
/// assert!(NoReference("tbank").to_string().starts_with("SKIPPED: no live reference for tbank"));
/// ```
pub struct NoReference<'a>(pub &'a str);

impl std::fmt::Display for NoReference<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SKIPPED: no live reference for {} (tests/fixtures/live is empty): run the live run first, \
             see the page \"Live runs\" of the book",
            self.0
        )
    }
}

/// The outcomes of the probes by name. Two probes with one name are a mistake in the list of probes.
///
/// # Panics
///
/// On a repeated name: the second would silently replace the first.
#[must_use]
pub fn by_name(probes: Vec<(String, Outcome)>) -> BTreeMap<String, Outcome> {
    let mut map = BTreeMap::new();
    for (name, outcome) in probes {
        assert!(
            map.insert(name.clone(), outcome).is_none(),
            "two probes are called `{name}`"
        );
    }
    map
}

impl Report {
    /// Fails the test when something differs.
    ///
    /// # Panics
    ///
    /// With the list of differences.
    pub fn assert_ok(&self) {
        assert!(
            self.problems.is_empty(),
            "the mock does not answer like the broker ({} of {} probes differ):\n  {}",
            self.problems.len(),
            self.compared,
            self.problems.join("\n  ")
        );
    }
}

/// Compares the live reference files with what the mock answered to the same probes.
///
/// `mock` maps the name of a probe to the outcome the mock gave. Each file must have a probe of its name.
/// Answered and answered: the shapes are compared ([`compare`]). Refused and refused: the codes must
/// be equal, and the bodies compared when both have one. A refusal on one side and an answer on the other
/// is a difference.
///
/// # Examples
///
/// ```
/// use std::collections::BTreeMap;
/// use brokapi_mocks::live::{Fixture, Outcome, check};
/// use serde_json::json;
///
/// let live = vec![Fixture {
///     broker: "tbank".into(),
///     probe: "get_accounts".into(),
///     outcome: Outcome::Answered(json!({"accounts": [{"id": "x", "status": 2}]})),
/// }];
///
/// let mut mock = BTreeMap::new();
/// mock.insert("get_accounts".to_owned(), Outcome::Answered(json!({"accounts": [{"id": "acc-a", "status": 2}]})));
/// assert!(check(&live, &mock).problems.is_empty());
///
/// // The mock forgot the status.
/// mock.insert("get_accounts".to_owned(), Outcome::Answered(json!({"accounts": [{"id": "acc-a"}]})));
/// assert_eq!(check(&live, &mock).problems.len(), 1);
/// ```
#[must_use]
pub fn check(live: &[Fixture], mock: &BTreeMap<String, Outcome>) -> Report {
    let mut problems = Vec::new();
    for f in live {
        let name = &f.probe;
        let Some(got) = mock.get(name) else {
            problems.push(format!(
                "{name}: the live file has no probe of this name (renamed or removed? delete the file)"
            ));
            continue;
        };
        match (&f.outcome, got) {
            (Outcome::Answered(l), Outcome::Answered(m)) => {
                for mismatch in compare(&Shape::of(l), &Shape::of(m)) {
                    problems.push(format!("{name}: {mismatch}"));
                }
            }
            (Outcome::Refused { code: lc, body: lb }, Outcome::Refused { code: mc, body: mb }) => {
                if lc != mc {
                    problems.push(format!(
                        "{name}: the broker refuses with {lc}, the mock with {mc}"
                    ));
                }
                if let (Some(l), Some(m)) = (lb, mb) {
                    for mismatch in compare(&Shape::of(l), &Shape::of(m)) {
                        problems.push(format!("{name} (the body of the refusal): {mismatch}"));
                    }
                }
            }
            (Outcome::Answered(_), Outcome::Refused { code, .. }) => {
                problems.push(format!(
                    "{name}: the broker answers, the mock refuses with {code}"
                ));
            }
            (Outcome::Refused { code, .. }, Outcome::Answered(_)) => {
                problems.push(format!(
                    "{name}: the broker refuses with {code}, the mock answers"
                ));
            }
        }
    }
    Report {
        compared: live.len(),
        problems,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn answered(v: Value) -> Outcome {
        Outcome::Answered(v)
    }

    fn temp(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "brokapi-fixture-test-{}-{name}",
            std::process::id()
        ))
    }

    #[test]
    fn a_recorded_file_is_clean_and_reads_back_as_the_same_shape() {
        let dir = temp("roundtrip");
        let secret = "t.RealTokenOfTheRunAbCdEf012345";
        let recorder = Recorder::new("tbank", &dir).with_secrets([secret, "2000123456"]);
        let original = json!({
            "accounts": [{"id": "2000123456", "type": 1, "name": "Ivan Petrov's IIS", "status": 2}],
            "orderId": "20261019-TQBR-79000001",
            "price": {"units": 244, "nano": 300_000_000},
            "echo": format!("authorization: Bearer {secret}"),
        });
        let path = recorder.record("get_accounts", &answered(original.clone()));
        let text = fs::read_to_string(&path).unwrap();
        for leaked in [secret, "2000123456", "Ivan", "Petrov", "79000001"] {
            assert!(!text.contains(leaked), "{leaked} is in the file:\n{text}");
        }
        let back = load_dir(&dir);
        assert_eq!(back.len(), 1);
        let Outcome::Answered(body) = &back[0].outcome else {
            panic!("an answer was recorded")
        };
        assert!(compare(&Shape::of(&original), &Shape::of(body)).is_empty());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_refusal_is_recorded_with_its_code_and_a_clean_body() {
        let dir = temp("refusal");
        let recorder = Recorder::new("bcs", &dir);
        recorder.record(
            "get_unknown_order",
            &Outcome::Refused {
                code: "http_404".into(),
                body: Some(
                    json!({"type": "NOT_FOUND", "traceId": "0a1b2c3d4e5f60718293a4b5c6d7e8f9"}),
                ),
            },
        );
        let loaded = load_dir(&dir);
        let Outcome::Refused { code, body } = &loaded[0].outcome else {
            panic!("a refusal was recorded")
        };
        assert_eq!(code, "http_404");
        assert_eq!(body.as_ref().unwrap()["type"], "NOT_FOUND");
        assert_eq!(body.as_ref().unwrap()["traceId"], "<redacted>");
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn no_folder_means_no_files() {
        assert!(load_dir(temp("never-made")).is_empty());
    }

    fn live(probe: &str, outcome: Outcome) -> Vec<Fixture> {
        vec![Fixture {
            broker: "bcs".into(),
            probe: probe.into(),
            outcome,
        }]
    }

    #[test]
    fn a_mock_that_differs_is_found_and_one_that_agrees_passes() {
        let l = live(
            "book",
            answered(json!({"bids": [{"price": "244.30", "qty": 5}]})),
        );
        let agree = BTreeMap::from([(
            "book".to_owned(),
            answered(json!({"bids": [{"price": "1.5", "qty": 1}]})),
        )]);
        assert_eq!(
            check(&l, &agree),
            Report {
                compared: 1,
                problems: vec![]
            }
        );

        let numbers = BTreeMap::from([(
            "book".to_owned(),
            answered(json!({"bids": [{"price": 244.3, "qty": 5}]})),
        )]);
        let report = check(&l, &numbers);
        assert_eq!(report.problems.len(), 1);
        assert!(
            report.problems[0].contains("$.bids[].price"),
            "{:?}",
            report.problems
        );
    }

    #[test]
    fn error_codes_and_outcomes_must_agree() {
        let l = live(
            "unknown",
            Outcome::Refused {
                code: "http_404".into(),
                body: None,
            },
        );
        let same = BTreeMap::from([(
            "unknown".to_owned(),
            Outcome::Refused {
                code: "http_404".into(),
                body: None,
            },
        )]);
        assert!(check(&l, &same).problems.is_empty());

        let other = BTreeMap::from([(
            "unknown".to_owned(),
            Outcome::Refused {
                code: "http_400".into(),
                body: None,
            },
        )]);
        assert!(check(&l, &other).problems[0].contains("http_404"));

        let answers = BTreeMap::from([("unknown".to_owned(), answered(json!({})))]);
        assert!(check(&l, &answers).problems[0].contains("the mock answers"));
    }

    #[test]
    fn a_file_without_a_probe_is_a_problem_not_a_silent_skip() {
        let l = live("renamed", answered(json!({})));
        let report = check(&l, &BTreeMap::new());
        assert!(report.problems[0].contains("renamed"));
    }

    #[test]
    #[should_panic(expected = "does not answer like the broker")]
    fn the_report_fails_the_test_when_something_differs() {
        let l = live("book", answered(json!({"a": 1})));
        check(
            &l,
            &BTreeMap::from([("book".to_owned(), answered(json!({"a": "x"})))]),
        )
        .assert_ok();
    }
}
