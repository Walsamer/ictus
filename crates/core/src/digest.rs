//! The identity-digest protocol.
//!
//! The digest that binds an identity to immutable content is part of the
//! interface between Ictus and a consumer such as Tactus, so Ictus produces and
//! owns it as a versioned protocol: the algorithm, the per-subject field sets
//! and the pinned vectors are published once in `docs/IDENTITY_DIGEST.md` and
//! `contracts/digest-vectors.json`, and a consumer implements it independently
//! and asserts those vectors. There is deliberately no shared runtime library.
//!
//! The canonical form is CPython's
//! `json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)`:
//! object keys sorted by code point, no whitespace, every character outside
//! `0x20..=0x7e` escaped as `\uXXXX` (a surrogate pair above `U+FFFF`), numbers
//! in CPython's `repr` form, encoded as UTF-8. The digest is the lowercase hex
//! SHA-256 of those bytes.
//!
//! Two things never happen here: a value outside the protocol's number domain
//! is refused with a typed error naming its JSON path rather than being hashed
//! as the nearest representable value, and a number the writer cannot write back
//! exactly is refused instead of being written approximately.

use serde_json::{Number, Value};

use crate::decision::DecisionProposal;

/// Version of the identity-digest protocol this build writes and accepts.
///
/// A change to the algorithm, to the digested field set or to the vectors needs
/// a new version. A consumer pins the version and fails closed on an unknown
/// one; this build fails closed on a version it does not implement.
pub const DIGEST_SCHEMA_VERSION: u32 = 1;

/// Smallest integer the protocol digests as an integer (`i64::MIN`).
pub const INTEGER_MIN: i128 = -9_223_372_036_854_775_808;

/// Largest integer the protocol digests as an integer (`u64::MAX`).
pub const INTEGER_MAX: i128 = 18_446_744_073_709_551_615;

/// Root of every reported JSON path (`$`, `$.facts[0].value`).
pub const ROOT_PATH: &str = "$";

/// A payload outside the digest protocol.
///
/// The error names the JSON path of the offending value so a producer can fix
/// the payload instead of shipping a digest a consumer cannot reproduce.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DigestError {
    /// An integer outside `-2^63 ..= 2^64-1`: a parser cannot represent it
    /// exactly, and hashing the nearest double would silently disagree with a
    /// consumer that parsed the same literal as an exact integer.
    NumberOutOfDomain { path: String, literal: String },
    /// A number that is not finite. It has no canonical JSON form.
    NonFinite { path: String, literal: String },
    /// A value the writer could not write back exactly.
    NotRoundTrippable { path: String, value: String },
    /// A digest schema version this build does not implement.
    UnsupportedSchemaVersion { found: u32 },
}

impl DigestError {
    /// The JSON path the error is about (`$` for a whole document).
    pub fn path(&self) -> &str {
        match self {
            DigestError::NumberOutOfDomain { path, .. }
            | DigestError::NonFinite { path, .. }
            | DigestError::NotRoundTrippable { path, .. } => path,
            DigestError::UnsupportedSchemaVersion { .. } => ROOT_PATH,
        }
    }

    /// Stable token naming the refusal, matching the vector file's `reason`.
    pub fn reason(&self) -> &'static str {
        match self {
            DigestError::NumberOutOfDomain { .. } => "number-out-of-domain",
            DigestError::NonFinite { .. } => "non-finite",
            DigestError::NotRoundTrippable { .. } => "not-round-trippable",
            DigestError::UnsupportedSchemaVersion { .. } => "unsupported-schema-version",
        }
    }
}

impl std::fmt::Display for DigestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DigestError::NumberOutOfDomain { path, literal } => write!(
                formatter,
                "{path}: integer {literal} is outside the digest domain -2^63..=2^64-1"
            ),
            DigestError::NonFinite { path, literal } => {
                write!(formatter, "{path}: {literal} is not a finite number")
            }
            DigestError::NotRoundTrippable { path, value } => {
                write!(formatter, "{path}: {value} does not round-trip")
            }
            DigestError::UnsupportedSchemaVersion { found } => write!(
                formatter,
                "unsupported identity-digest schema_version {found}; supported is {DIGEST_SCHEMA_VERSION}"
            ),
        }
    }
}

impl std::error::Error for DigestError {}

/// Reject a version this build does not implement rather than reading it as a
/// successor.
pub fn check_schema_version(found: u32) -> Result<(), DigestError> {
    if found == DIGEST_SCHEMA_VERSION {
        Ok(())
    } else {
        Err(DigestError::UnsupportedSchemaVersion { found })
    }
}

/// Canonical JSON of a value: the exact bytes that are hashed.
pub fn canonical_json(value: &Value) -> Result<String, DigestError> {
    let mut canonical = String::new();
    write_value(&mut canonical, value, ROOT_PATH)?;
    Ok(canonical)
}

/// Lowercase hex SHA-256 of the canonical form of `value`.
pub fn digest_value(value: &Value) -> Result<String, DigestError> {
    Ok(sha256_hex(canonical_json(value)?.as_bytes()))
}

/// Lowercase hex SHA-256 of a proposal: the digest an envelope binds as
/// `validation.proposal_digest` and an intent repeats in `policy_context`.
pub fn proposal_digest(proposal: &DecisionProposal) -> Result<String, DigestError> {
    let value = serde_json::to_value(proposal).expect("a DecisionProposal is always serializable");
    digest_value(&value)
}

/// Lowercase hex SHA-256 of `input`.
pub fn sha256_hex(input: &[u8]) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(input);
    format!("{:x}", hasher.finalize())
}

/// Reject any number in a JSON document that the digest protocol cannot carry
/// exactly.
///
/// This is the ingress guard, not a digest: a parser that cannot represent an
/// integer turns it into the nearest double and the information that it was an
/// integer is gone, so the refusal has to happen on the document text. The scan
/// ignores everything inside a string, so a literal that looks like a number in
/// a message is untouched.
pub fn check_number_domain(document: &str) -> Result<(), DigestError> {
    let bytes = document.as_bytes();
    let mut frames: Vec<Frame> = Vec::new();
    // Everything before this byte is already accounted for (a string or a
    // number token that was scanned in one step).
    let mut skip_until = 0;
    for (at, byte) in bytes.iter().enumerate() {
        if at < skip_until {
            continue;
        }
        match *byte {
            b'"' => {
                if let Some(end) = string_end(bytes, at) {
                    if let Some(frame) = frames.last_mut() {
                        capture_key(frame, &document[at + 1..end - 1]);
                    }
                    skip_until = end;
                } else {
                    // An unterminated string: the parser rejects the document,
                    // so the domain guard has nothing left to report.
                    skip_until = bytes.len();
                }
            }
            b'{' => frames.push(Frame::object()),
            b'[' => frames.push(Frame::array()),
            b'}' | b']' => {
                frames.pop();
            }
            b',' => {
                if let Some(frame) = frames.last_mut() {
                    ended_value(frame);
                }
            }
            b'-' | b'0'..=b'9' => {
                let end = number_end(bytes, at);
                check_number_token(&document[at..end], &path_of(&frames))?;
                skip_until = end;
            }
            _ => {}
        }
    }
    Ok(())
}

/// One open container while scanning a document.
#[derive(Debug, PartialEq, Eq)]
struct Frame {
    object: bool,
    /// The key this object's current value is under.
    key: Option<String>,
    /// True while the next string in this object is a key rather than a value.
    expecting_key: bool,
    /// The current element index of an array.
    index: usize,
}

impl Frame {
    fn object() -> Self {
        Self {
            object: true,
            key: None,
            expecting_key: true,
            index: 0,
        }
    }

    fn array() -> Self {
        Self {
            object: false,
            key: None,
            expecting_key: false,
            index: 0,
        }
    }
}

/// Record a string token as this object's current key, when it is one.
fn capture_key(frame: &mut Frame, text: &str) {
    if frame.expecting_key {
        frame.key = Some(text.to_string());
        frame.expecting_key = false;
    }
}

/// Account for a value ending at a comma: the next string is a key in an
/// object, and an array moves on to its next element.
fn ended_value(frame: &mut Frame) {
    if frame.object {
        frame.key = None;
        frame.expecting_key = true;
    } else {
        frame.index += 1;
    }
}

fn is_number_byte(byte: u8) -> bool {
    matches!(byte, b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9')
}

/// Index just past the closing quote of the string starting at `start`, or
/// `None` when the document has no closing quote.
fn string_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut escaped = false;
    for (offset, byte) in bytes.iter().enumerate().skip(start.saturating_add(1)) {
        if escaped {
            escaped = false;
            continue;
        }
        match *byte {
            b'\\' => escaped = true,
            b'"' => return Some(offset.saturating_add(1)),
            _ => {}
        }
    }
    None
}

/// Index just past the number token starting at `start`.
fn number_end(bytes: &[u8], start: usize) -> usize {
    start.saturating_add(
        bytes[start..]
            .iter()
            .take_while(|byte| is_number_byte(**byte))
            .count(),
    )
}

fn path_of(frames: &[Frame]) -> String {
    let mut path = String::from(ROOT_PATH);
    for frame in frames {
        if frame.object {
            path.push('.');
            path.push_str(frame.key.as_deref().unwrap_or_default());
        } else {
            path.push_str(&format!("[{}]", frame.index));
        }
    }
    path
}

fn check_number_token(literal: &str, path: &str) -> Result<(), DigestError> {
    if literal.contains(['.', 'e', 'E']) {
        let finite = literal
            .parse::<f64>()
            .map(|value| value.is_finite())
            .unwrap_or(false);
        if finite {
            return Ok(());
        }
        return Err(DigestError::NonFinite {
            path: path.to_string(),
            literal: literal.to_string(),
        });
    }
    let in_domain = match literal.parse::<i128>() {
        Ok(value) => (INTEGER_MIN..=INTEGER_MAX).contains(&value),
        Err(_) => false,
    };
    if in_domain {
        Ok(())
    } else {
        Err(DigestError::NumberOutOfDomain {
            path: path.to_string(),
            literal: literal.to_string(),
        })
    }
}

fn write_value(canonical: &mut String, value: &Value, path: &str) -> Result<(), DigestError> {
    match value {
        Value::Null => canonical.push_str("null"),
        Value::Bool(true) => canonical.push_str("true"),
        Value::Bool(false) => canonical.push_str("false"),
        Value::Number(number) => write_number(canonical, number, path)?,
        Value::String(text) => write_string(canonical, text),
        Value::Array(items) => {
            canonical.push('[');
            for (index, entry) in items.iter().enumerate() {
                if index > 0 {
                    canonical.push(',');
                }
                write_value(canonical, entry, &format!("{path}[{index}]"))?;
            }
            canonical.push(']');
        }
        Value::Object(object) => {
            canonical.push('{');
            for (index, (key, entry)) in object.iter().enumerate() {
                if index > 0 {
                    canonical.push(',');
                }
                write_string(canonical, key);
                canonical.push(':');
                write_value(canonical, entry, &format!("{path}.{key}"))?;
            }
            canonical.push('}');
        }
    }
    Ok(())
}

fn write_number(canonical: &mut String, number: &Number, path: &str) -> Result<(), DigestError> {
    if let Some(value) = number.as_i64() {
        canonical.push_str(&value.to_string());
        return Ok(());
    }
    if let Some(value) = number.as_u64() {
        canonical.push_str(&value.to_string());
        return Ok(());
    }
    let value = number
        .as_f64()
        .expect("a serde_json number is an integer or a double");
    let text = format_f64(value);
    verify_roundtrip(&text, value.to_bits(), path)?;
    canonical.push_str(&text);
    Ok(())
}

/// Verify that `text` parses back to exactly `bits`.
fn verify_roundtrip(text: &str, bits: u64, path: &str) -> Result<(), DigestError> {
    let reparsed = text
        .parse::<f64>()
        .map(|value| value.to_bits())
        .unwrap_or(u64::MAX);
    if reparsed == bits {
        Ok(())
    } else {
        Err(DigestError::NotRoundTrippable {
            path: path.to_string(),
            value: text.to_string(),
        })
    }
}

/// How many digits sit before the decimal point in the positional form.
///
/// Only the positional branch calls this, and there the value is always
/// `1 ..= 16`; a panic here rather than a wrapped conversion means a mangled
/// branch fails a test instead of trying to allocate `usize::MAX` bytes.
fn digits_before_point(decimals: i32) -> usize {
    usize::try_from(decimals).expect("the positional branch only sees 1..=16 digits")
}

/// CPython `float.__repr__`: the shortest string that round-trips, written
/// positionally when the decimal point sits at exponent `-4 ..= 16` and with a
/// signed exponent of at least two digits otherwise.
fn format_f64(value: f64) -> String {
    let exponential = format!("{value:e}");
    let (mantissa, exponent) = exponential
        .split_once('e')
        .expect("Rust's LowerExp always writes an exponent");
    let exponent: i32 = exponent
        .parse()
        .expect("Rust's LowerExp writes an integer exponent");
    let (sign, mantissa) = match mantissa.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", mantissa),
    };
    let digits: String = mantissa
        .chars()
        .filter(|character| *character != '.')
        .collect();
    // value == 0.<digits> * 10^decimals
    let decimals = exponent + 1;
    let mut text = String::from(sign);
    if decimals > -4 && decimals <= 16 {
        if decimals <= 0 {
            text.push_str("0.");
            for _ in 0..-decimals {
                text.push('0');
            }
            text.push_str(&digits);
        } else if digits_before_point(decimals) >= digits.len() {
            text.push_str(&digits);
            for _ in digits.len()..digits_before_point(decimals) {
                text.push('0');
            }
            text.push_str(".0");
        } else {
            let split = digits_before_point(decimals);
            text.push_str(&digits[..split]);
            text.push('.');
            text.push_str(&digits[split..]);
        }
    } else {
        text.push_str(&digits[..1]);
        let rest = &digits[1..];
        if !rest.is_empty() {
            text.push('.');
            text.push_str(rest);
        }
        let written = decimals - 1;
        text.push('e');
        text.push_str(&format!("{written:+03}"));
    }
    text
}

/// Write a JSON string in the canonical form: only `"` and `\` are written as
/// themselves, `0x20..=0x7e` stays literal, everything else is `\uXXXX` with a
/// surrogate pair above `U+FFFF`.
fn write_string(canonical: &mut String, text: &str) {
    canonical.push('"');
    for character in text.chars() {
        match character {
            '"' => canonical.push_str("\\\""),
            '\\' => canonical.push_str("\\\\"),
            '\u{08}' => canonical.push_str("\\b"),
            '\u{0c}' => canonical.push_str("\\f"),
            '\n' => canonical.push_str("\\n"),
            '\r' => canonical.push_str("\\r"),
            '\t' => canonical.push_str("\\t"),
            other => {
                let code = other as u32;
                if (0x20..=0x7e).contains(&code) {
                    canonical.push(other);
                } else if code >= 0x1_0000 {
                    let adjusted = code - 0x1_0000;
                    push_escape(canonical, 0xd800 + (adjusted >> 10));
                    push_escape(canonical, 0xdc00 + (adjusted & 0x3ff));
                } else {
                    push_escape(canonical, code);
                }
            }
        }
    }
    canonical.push('"');
}

fn push_escape(canonical: &mut String, code: u32) {
    canonical.push_str(&format!("\\u{code:04x}"));
}

#[cfg(test)]
mod tests {
    use super::{
        canonical_json, capture_key, check_number_domain, check_schema_version, digest_value,
        ended_value, format_f64, path_of, verify_roundtrip, DigestError, Frame,
        DIGEST_SCHEMA_VERSION,
    };
    use serde_json::json;

    fn canonical(value: &serde_json::Value) -> String {
        canonical_json(value).expect("in-domain values always digest")
    }

    #[test]
    fn canonical_form_sorts_keys_and_drops_whitespace() {
        let value: serde_json::Value =
            serde_json::from_str(r#"{ "b" : 2 , "a" : [ 1 , { "d" : 4 , "c" : 3 } ] }"#).unwrap();
        assert_eq!(canonical(&value), r#"{"a":[1,{"c":3,"d":4}],"b":2}"#);
        // Insertion order must not survive: two documents with the same pairs in
        // a different order canonicalize to the same bytes.
        let other: serde_json::Value =
            serde_json::from_str(r#"{"a":[1,{"c":3,"d":4}],"b":2}"#).unwrap();
        assert_eq!(canonical(&other), canonical(&value));
    }

    #[test]
    fn canonical_form_writes_every_literal_and_empty_container() {
        let value = json!({"n": null, "t": true, "f": false, "o": {}, "a": [], "s": ""});
        assert_eq!(
            canonical(&value),
            r#"{"a":[],"f":false,"n":null,"o":{},"s":"","t":true}"#
        );
    }

    #[test]
    fn canonical_form_escapes_the_way_python_does() {
        // Short escapes, the two quote characters, a slash left alone, control
        // characters as \u00xx, and DEL escaped because it is outside 0x20..=0x7e.
        let value = json!({"v": "a\"b\\c/d\n\t\r\u{8}\u{c}\u{0}\u{1f}\u{7f}"});
        assert_eq!(
            canonical(&value),
            "{\"v\":\"a\\\"b\\\\c/d\\n\\t\\r\\b\\f\\u0000\\u001f\\u007f\"}"
        );
    }

    #[test]
    fn canonical_form_escapes_non_ascii_and_surrogate_pairs() {
        let value = json!({"v": "héllo 😀 日本語"});
        assert_eq!(
            canonical(&value),
            "{\"v\":\"h\\u00e9llo \\ud83d\\ude00 \\u65e5\\u672c\\u8a9e\"}"
        );
        // A key is escaped the same way it is written, so the object stays sorted
        // by code point rather than by its escaped spelling.
        let keys = json!({"é": 1, "\u{fffd}": 2, "😀": 3});
        assert_eq!(
            canonical(&keys),
            "{\"\\u00e9\":1,\"\\ufffd\":2,\"\\ud83d\\ude00\":3}"
        );
    }

    #[test]
    fn numbers_follow_the_cpython_form() {
        for (value, expected) in [
            (json!(0), "0"),
            (json!(1), "1"),
            (json!(-1), "-1"),
            (json!(1.0), "1.0"),
            (json!(-0.0), "-0.0"),
            (json!(0.1), "0.1"),
            (json!(100.0), "100.0"),
            (json!(1.5), "1.5"),
            (json!(1e15), "1000000000000000.0"),
            (json!(1e16), "1e+16"),
            (json!(1e17), "1e+17"),
            (json!(1e21), "1e+21"),
            (json!(1e300), "1e+300"),
            (json!(1e-4), "0.0001"),
            (json!(1e-5), "1e-05"),
            (json!(2.5e-10), "2.5e-10"),
            (json!(5e-324), "5e-324"),
            (json!(f64::MAX), "1.7976931348623157e+308"),
            (json!(0.30000000000000004), "0.30000000000000004"),
            (json!(123456789012345678.0), "1.2345678901234568e+17"),
            (json!(1e100), "1e+100"),
            (json!(1.2345678901234567e-7), "1.2345678901234566e-07"),
            (json!(9007199254740993.0), "9007199254740992.0"),
            (json!(i64::MIN), "-9223372036854775808"),
            (json!(u64::MAX), "18446744073709551615"),
        ] {
            assert_eq!(canonical(&value), expected, "for {value}");
        }
    }

    #[test]
    fn an_integer_and_a_double_of_the_same_value_digest_differently() {
        assert_ne!(
            digest_value(&json!({"v": 1})).unwrap(),
            digest_value(&json!({"v": 1.0})).unwrap()
        );
    }

    #[test]
    fn format_f64_writes_the_positional_and_exponent_forms() {
        assert_eq!(format_f64(0.0), "0.0");
        assert_eq!(format_f64(-0.0), "-0.0");
        assert_eq!(format_f64(1e-4), "0.0001");
        assert_eq!(format_f64(1e-5), "1e-05");
        assert_eq!(format_f64(1e16), "1e+16");
        assert_eq!(format_f64(1e15), "1000000000000000.0");
    }

    #[test]
    fn a_value_that_does_not_round_trip_is_refused() {
        let error = verify_roundtrip("1.0", 2.0_f64.to_bits(), "$.a").unwrap_err();
        assert_eq!(
            error,
            DigestError::NotRoundTrippable {
                path: "$.a".to_string(),
                value: "1.0".to_string()
            }
        );
        assert_eq!(error.reason(), "not-round-trippable");
        assert_eq!(error.path(), "$.a");
        assert!(error.to_string().contains("1.0 does not round-trip"));
        // Text that is not a number at all is refused the same way.
        assert!(verify_roundtrip("nan", 1, "$").is_err());
        assert!(verify_roundtrip("1.0", 1.0_f64.to_bits(), "$").is_ok());
    }

    #[test]
    fn an_out_of_domain_number_names_its_path() {
        for document in [
            r#"{"facts":[{"value":18446744073709551616}]}"#,
            r#"{"facts":[{"value":-9223372036854775809}]}"#,
            r#"{"a":{"b":[1,2,123456789012345678901234567890]}}"#,
        ] {
            let error = check_number_domain(document).unwrap_err();
            assert_eq!(error.reason(), "number-out-of-domain");
            assert!(error.path().starts_with('$'));
        }
        let error =
            check_number_domain(r#"{"facts":[{"key":"big","value":18446744073709551616}]}"#)
                .unwrap_err();
        assert_eq!(error.path(), "$.facts[0].value");
        assert!(error.to_string().contains("$.facts[0].value"));
    }

    #[test]
    fn the_number_domain_accepts_its_own_edges_and_ignores_strings() {
        for document in [
            r#"{"a":-9223372036854775808}"#,
            r#"{"a":18446744073709551615}"#,
            r#"{"a":0}"#,
            r#"{"a":-0.0}"#,
            r#"{"a":1.5e300}"#,
            r#"{"a":[]}"#,
            r#"{"a":{}}"#,
            r#"{"a":"18446744073709551616"}"#,
            r#"{"a":"\"18446744073709551616\""}"#,
            r#"["18446744073709551616"]"#,
            r#""18446744073709551616""#,
            r#"{ "a" : 1 , "b" : [ 2 ] }"#,
            r#","#,
            r#"{"a":[{"b":1},{"b":2}]}"#,
            r#"{"a":1,"b":2}"#,
            r#"5"#,
            r#"{}"#,
        ] {
            assert!(check_number_domain(document).is_ok(), "for {document}");
        }
    }

    #[test]
    fn a_non_finite_literal_is_refused() {
        let error = check_number_domain(r#"{"a":1e400}"#).unwrap_err();
        assert_eq!(
            error,
            DigestError::NonFinite {
                path: "$.a".to_string(),
                literal: "1e400".to_string()
            }
        );
        assert_eq!(error.path(), "$.a");
        assert_eq!(error.reason(), "non-finite");
        assert!(error
            .to_string()
            .contains("$.a: 1e400 is not a finite number"));
    }

    #[test]
    fn a_malformed_document_still_names_its_path() {
        // The scan is a guard over the text, not a validator: it reports what it
        // sees, and the parser rejects the document afterwards.
        let error = check_number_domain(r#"{18446744073709551616:1}"#).unwrap_err();
        assert_eq!(error.path(), "$.");
        // An unterminated string leaves nothing to report.
        assert!(check_number_domain(r#"{"a": "unterminated"#).is_ok());
        assert!(check_number_domain(r#"{"a"#).is_ok());
    }

    #[test]
    fn a_key_is_captured_only_while_one_is_expected() {
        let mut object = Frame::object();
        capture_key(&mut object, "facts");
        assert_eq!(object.key.as_deref(), Some("facts"));
        assert!(!object.expecting_key);
        // A value string after the key leaves the key alone.
        capture_key(&mut object, "\"quoted value\"");
        assert_eq!(object.key.as_deref(), Some("facts"));
        // An array element is never a key.
        let mut array = Frame::array();
        capture_key(&mut array, "0");
        assert_eq!(array.key, None);
        assert!(!array.expecting_key);
    }

    #[test]
    fn a_comma_resets_an_object_and_advances_an_array() {
        let mut object = Frame::object();
        capture_key(&mut object, "a");
        ended_value(&mut object);
        assert_eq!(object.key, None);
        assert!(object.expecting_key);
        // Two elements move an array on by two, so a stuck index cannot pass.
        let mut array = Frame::array();
        ended_value(&mut array);
        ended_value(&mut array);
        assert_eq!(array.index, 2);
    }

    #[test]
    fn a_path_names_every_open_container() {
        assert_eq!(path_of(&[]), "$");
        let mut object = Frame::object();
        capture_key(&mut object, "facts");
        let mut array = Frame::array();
        ended_value(&mut array);
        let mut inner = Frame::object();
        capture_key(&mut inner, "value");
        assert_eq!(path_of(&[object, array, inner]), "$.facts[1].value");
        // An object at a value position that never had a key captured still
        // produces a usable path rather than a panic.
        assert_eq!(path_of(&[Frame::object()]), "$.");
    }

    #[test]
    fn a_string_ends_after_its_closing_quote() {
        assert_eq!(super::string_end(br#"{"a":1}"#, 1), Some(4));
        // An escaped quote does not end the string, and the escape itself is
        // skipped rather than read as a quote.
        assert_eq!(super::string_end(br#""a\"b""#, 0), Some(6));
        assert_eq!(super::string_end(br#""unterminated"#, 0), None);
    }

    #[test]
    fn a_number_token_runs_to_its_first_non_number_byte() {
        assert_eq!(super::number_end(b"12,3", 0), 2);
        assert_eq!(super::number_end(b"-1.5e+300]", 0), 9);
        assert_eq!(super::number_end(b"x", 0), 0);
    }

    #[test]
    fn a_malformed_number_token_is_refused() {
        // Not a valid document either, but the guard must not read the token and
        // then decide it is a finite double because the parse failed quietly.
        let error = check_number_domain(r#"{"a":1e}"#).unwrap_err();
        assert_eq!(error.reason(), "non-finite");
        assert_eq!(error.path(), "$.a");
    }

    #[test]
    fn an_unknown_schema_version_fails_closed() {
        assert!(check_schema_version(DIGEST_SCHEMA_VERSION).is_ok());
        let error = check_schema_version(DIGEST_SCHEMA_VERSION + 1).unwrap_err();
        assert_eq!(
            error,
            DigestError::UnsupportedSchemaVersion {
                found: DIGEST_SCHEMA_VERSION + 1
            }
        );
        assert_eq!(error.reason(), "unsupported-schema-version");
        assert_eq!(error.path(), super::ROOT_PATH);
        assert!(error.to_string().contains("supported is 1"));
    }

    #[test]
    fn the_digest_is_sha256_of_the_canonical_bytes() {
        // Pinned: sha256 of {"a":1} — the canonical form of a one-pair object.
        assert_eq!(
            digest_value(&json!({"a": 1})).unwrap(),
            "015abd7f5cc57a2dd94b7590f04ad8084273905ee33ec5cebeae62276a97f862"
        );
    }

    #[test]
    fn the_hash_is_standard_sha256() {
        // Standard FIPS 180-4 vectors, plus padding-boundary and multi-block
        // inputs, so the shared wrapper is pinned to the standard primitive
        // every other implementation of the protocol uses.
        for (input, expected) in [
            (
                &b""[..],
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ),
            (
                &b"abc"[..],
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            ),
            (
                &b"a"[..],
                "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb",
            ),
            (
                &b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"[..],
                "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1",
            ),
        ] {
            assert_eq!(super::sha256_hex(input), expected);
        }
        let padding: [(Vec<u8>, &str); 5] = [
            (
                vec![b'a'; 55],
                "9f4390f8d30c2dd92ec9f095b65e2b9ae9b0a925a5258e241c9f1e910f734318",
            ),
            (
                vec![b'a'; 56],
                "b35439a4ac6f0948b6d6f9e3c6af0f5f590ce20f1bde7090ef7970686ec6738a",
            ),
            (
                vec![b'a'; 64],
                "ffe054fe7ae0cb6dc65c3af9b61d5209f439851db43d0ba5997337df154668eb",
            ),
            (
                (0u8..64).collect(),
                "fdeab9acf3710362bd2658cdc9a29e8f9c757fcf9811603a8c447cd1d9151108",
            ),
            (
                (1u8..66).collect(),
                "47e9b736f9001fab14640d9c915574c20669621520371f05a4b50906020dc49f",
            ),
        ];
        for (input, expected) in padding {
            assert_eq!(super::sha256_hex(&input), expected);
        }
    }

    #[test]
    fn the_integer_domain_edges_are_inclusive() {
        assert!(check_number_domain(r#"{"a":9223372036854775807}"#).is_ok());
        assert!(check_number_domain(r#"{"a":-9223372036854775808}"#).is_ok());
        assert!(check_number_domain(r#"{"a":18446744073709551615}"#).is_ok());
        assert!(check_number_domain(r#"{"a":18446744073709551616}"#).is_err());
        assert!(check_number_domain(r#"{"a":-9223372036854775809}"#).is_err());
        // A literal too long for any integer type is out of domain too.
        assert!(
            check_number_domain(r#"{"a":999999999999999999999999999999999999999999}"#).is_err()
        );
    }
}
