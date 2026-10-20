//! The shape of an answer: which fields it has and of which type. Values are not compared.
//!
//! A [`Shape`] is a map from the path of a field (`$.items[].price.units`) to the set of types the field had
//! in the answer. Elements of a list are merged under one path (`[]`), so a list of ten orders gives the
//! union of what its ten elements have. [`compare`] puts the shape of a live answer against the shape of the
//! answer of an imitation.
//!
//! The rules, written for the case of a live reference and a mock:
//!
//! * a field the broker sent and the mock does not: [`Mismatch::MissingInMock`]. The code could read it
//!   in real life and never sees it in a test;
//! * a field the mock sends and the broker does not: [`Mismatch::ExtraInMock`]. The code could rely on
//!   it, and it does not exist;
//! * the same field of a different type (a number where the broker sends a string): [`Mismatch::TypeDiffers`].
//!   `null` fits any type, because an optional field is `null` when there was nothing to say. The type
//!   `number` covers integers and floats alike;
//! * what the live answer did not show is not asked of the mock: a field that was `null` live or a list
//!   that was empty (so its content is unknown), and everything below it. The other way round is a
//!   difference: an empty list in the mock where the broker sent items.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde_json::Value;

/// The type of a value, as far as the comparison tells them apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Kind {
    /// `null`.
    Null,
    /// `true` or `false`.
    Bool,
    /// An integer or a floating point number.
    Number,
    /// A string. A number written as a string (`"244.30"`) is a string.
    String,
    /// A list.
    Array,
    /// An object.
    Object,
}

impl Kind {
    fn of(v: &Value) -> Self {
        match v {
            Value::Null => Self::Null,
            Value::Bool(_) => Self::Bool,
            Value::Number(_) => Self::Number,
            Value::String(_) => Self::String,
            Value::Array(_) => Self::Array,
            Value::Object(_) => Self::Object,
        }
    }
}

impl fmt::Display for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Null => "null",
            Self::Bool => "bool",
            Self::Number => "number",
            Self::String => "string",
            Self::Array => "list",
            Self::Object => "object",
        })
    }
}

/// Fields and types of one answer.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Shape {
    fields: BTreeMap<String, BTreeSet<Kind>>,
}

impl Shape {
    /// The shape of `value`.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_mocks::live::Shape;
    /// use serde_json::json;
    ///
    /// let shape = Shape::of(&json!({"price": "244.30", "levels": [{"qty": 1}, {"qty": 2, "side": "BUY"}]}));
    /// let paths: Vec<&str> = shape.paths().collect();
    /// assert_eq!(paths, ["$", "$.levels", "$.levels[]", "$.levels[].qty", "$.levels[].side", "$.price"]);
    /// ```
    #[must_use]
    pub fn of(value: &Value) -> Self {
        let mut shape = Self::default();
        shape.add("$", value);
        shape
    }

    fn add(&mut self, path: &str, value: &Value) {
        self.fields
            .entry(path.to_owned())
            .or_default()
            .insert(Kind::of(value));
        match value {
            Value::Object(map) => {
                for (key, v) in map {
                    self.add(&format!("{path}.{key}"), v);
                }
            }
            Value::Array(items) => {
                for v in items {
                    self.add(&format!("{path}[]"), v);
                }
            }
            _ => {}
        }
    }

    /// The paths of all fields, sorted.
    pub fn paths(&self) -> impl Iterator<Item = &str> {
        self.fields.keys().map(String::as_str)
    }

    fn kinds(&self, path: &str) -> Option<&BTreeSet<Kind>> {
        self.fields.get(path)
    }

    /// Whether the content under `path` is unknown in this answer: an ancestor was only `null`, or (with
    /// `empty_lists`) a list that held no element.
    fn unknown_below(&self, path: &str, empty_lists: bool) -> bool {
        let mut end = path.len();
        while let Some(cut) = path[..end].rfind(['.', '[']) {
            let ancestor = &path[..cut];
            if let Some(kinds) = self.fields.get(ancestor) {
                if kinds.iter().all(|k| *k == Kind::Null) {
                    return true;
                }
                if empty_lists
                    && kinds.contains(&Kind::Array)
                    && !self.fields.contains_key(&format!("{ancestor}[]"))
                {
                    return true;
                }
            }
            end = cut;
        }
        false
    }
}

/// One difference between a live answer and the answer of a mock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mismatch {
    /// The broker sends the field, the mock does not.
    MissingInMock {
        /// Path of the field.
        path: String,
        /// Types the broker sent.
        live: Vec<Kind>,
    },
    /// The mock sends the field, the broker does not.
    ExtraInMock {
        /// Path of the field.
        path: String,
        /// Types the mock sent.
        mock: Vec<Kind>,
    },
    /// The field is there on both sides with different types.
    TypeDiffers {
        /// Path of the field.
        path: String,
        /// Types the broker sent.
        live: Vec<Kind>,
        /// Types the mock sent.
        mock: Vec<Kind>,
    },
}

fn list(kinds: &[Kind]) -> String {
    kinds
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" or ")
}

impl fmt::Display for Mismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingInMock { path, live } => {
                write!(
                    f,
                    "{path}: the broker sends it ({}), the mock does not",
                    list(live)
                )
            }
            Self::ExtraInMock { path, mock } => {
                write!(
                    f,
                    "{path}: the mock sends it ({}), the broker does not",
                    list(mock)
                )
            }
            Self::TypeDiffers { path, live, mock } => {
                write!(
                    f,
                    "{path}: the broker sends {}, the mock sends {}",
                    list(live),
                    list(mock)
                )
            }
        }
    }
}

fn without_null(kinds: &BTreeSet<Kind>) -> Vec<Kind> {
    kinds.iter().copied().filter(|k| *k != Kind::Null).collect()
}

/// What differs between the shape of a live answer and the shape of the answer of a mock.
///
/// An empty result means the mock answers in the form of the broker.
///
/// # Examples
///
/// ```
/// use brokapi_mocks::live::{Shape, compare};
/// use serde_json::json;
///
/// let live = Shape::of(&json!({"price": "244.30", "qty": 2}));
///
/// // The same form, other values.
/// assert!(compare(&live, &Shape::of(&json!({"price": "1.00", "qty": 7}))).is_empty());
///
/// // A number where the broker writes a string, and a field the broker does not have.
/// let off = compare(&live, &Shape::of(&json!({"price": 244.3, "qty": 2, "extra": true})));
/// assert_eq!(off.len(), 2);
/// ```
#[must_use]
pub fn compare(live: &Shape, mock: &Shape) -> Vec<Mismatch> {
    let mut out = Vec::new();
    for (path, live_kinds) in &live.fields {
        match mock.kinds(path) {
            None => {
                // Live showed only `null` there: nothing is asked of the mock.
                let live_known = without_null(live_kinds);
                if !live_known.is_empty() && !mock.unknown_below(path, false) {
                    out.push(Mismatch::MissingInMock {
                        path: path.clone(),
                        live: live_known,
                    });
                }
            }
            Some(mock_kinds) => {
                let (l, m) = (without_null(live_kinds), without_null(mock_kinds));
                if !l.is_empty() && !m.is_empty() && l != m {
                    out.push(Mismatch::TypeDiffers {
                        path: path.clone(),
                        live: l,
                        mock: m,
                    });
                }
            }
        }
    }
    for (path, mock_kinds) in &mock.fields {
        if live.kinds(path).is_none() && !live.unknown_below(path, true) {
            let known = without_null(mock_kinds);
            if !known.is_empty() {
                out.push(Mismatch::ExtraInMock {
                    path: path.clone(),
                    mock: known,
                });
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn diff(live: Value, mock: Value) -> Vec<Mismatch> {
        compare(&Shape::of(&live), &Shape::of(&mock))
    }

    #[test]
    fn the_same_form_with_other_values_agrees() {
        assert!(
            diff(
                json!({"a": 1, "b": ["x"]}),
                json!({"a": 9, "b": ["y", "z"]})
            )
            .is_empty()
        );
    }

    #[test]
    fn a_missing_field_is_found_with_its_path() {
        let m = diff(
            json!({"order": {"id": "x", "status": 2}}),
            json!({"order": {"id": "x"}}),
        );
        assert_eq!(
            m,
            vec![Mismatch::MissingInMock {
                path: "$.order.status".into(),
                live: vec![Kind::Number]
            }]
        );
    }

    #[test]
    fn an_invented_field_is_found() {
        let m = diff(json!({"a": 1}), json!({"a": 1, "b": "x"}));
        assert_eq!(
            m,
            vec![Mismatch::ExtraInMock {
                path: "$.b".into(),
                mock: vec![Kind::String]
            }]
        );
    }

    #[test]
    fn a_number_written_as_a_string_is_a_different_type() {
        let m = diff(json!({"price": "244.30"}), json!({"price": 244.3}));
        assert_eq!(
            m,
            vec![Mismatch::TypeDiffers {
                path: "$.price".into(),
                live: vec![Kind::String],
                mock: vec![Kind::Number]
            }]
        );
        // integers and floats are one type
        assert!(diff(json!({"q": 1}), json!({"q": 1.5})).is_empty());
    }

    #[test]
    fn null_fits_any_type_and_hides_what_is_under_it() {
        // live had nothing in the field: the mock may fill it
        assert!(
            diff(
                json!({"price": null}),
                json!({"price": {"units": 1, "nano": 0}})
            )
            .is_empty()
        );
        // the mock has nothing there: the live field is not required from this answer
        assert!(diff(json!({"price": {"units": 1}}), json!({"price": null})).is_empty());
    }

    #[test]
    fn an_empty_list_in_the_mock_is_a_difference_when_live_had_items() {
        let m = diff(json!({"items": [{"id": 1}]}), json!({"items": []}));
        let paths: Vec<String> = m
            .iter()
            .map(|x| match x {
                Mismatch::MissingInMock { path, .. } => path.clone(),
                other => panic!("{other}"),
            })
            .collect();
        assert_eq!(paths, ["$.items[]", "$.items[].id"]);
        // and the other way: a live list that was empty says nothing about its items
        assert!(diff(json!({"items": []}), json!({"items": [{"id": 1}]})).is_empty());
    }

    #[test]
    fn elements_of_a_list_are_merged() {
        // the second element has a field the first has not: it counts as a field of the list
        let live = json!({"orders": [{"id": 1}, {"id": 2, "reason": "x"}]});
        assert!(diff(live.clone(), json!({"orders": [{"id": 5, "reason": "y"}]})).is_empty());
        assert_eq!(diff(live, json!({"orders": [{"id": 5}]})).len(), 1);
    }

    #[test]
    fn messages_name_the_path_and_both_types() {
        let m = diff(json!({"a": {"b": "x"}}), json!({"a": {"b": 1}}));
        assert_eq!(
            m[0].to_string(),
            "$.a.b: the broker sends string, the mock sends number"
        );
    }
}
