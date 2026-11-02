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
/// positionally when the scientific exponent `e` (value = `d.ddd * 10^e`)
/// satisfies `-4 <= e < 16`, and with a signed exponent of at least two digits
/// otherwise. `decimals` is `e + 1`, the number of digits before the decimal
/// point.
fn format_f64(value: f64) -> String {
    let (signed, decimals) = shortest_decimal(value);
    let (sign, digits) = match signed.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", signed.as_str()),
    };
    let mut text = String::from(sign);
    if decimals > -4 && decimals <= 16 {
        if decimals <= 0 {
            text.push_str("0.");
            for _ in 0..-decimals {
                text.push('0');
            }
            text.push_str(digits);
        } else if digits_before_point(decimals) >= digits.len() {
            text.push_str(digits);
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

/// The shortest round-trip decimal of `value` and its decimal-point position:
/// `value == 0.<digits> * 10^decimals`, with a signed digit string that carries
/// no leading zeros. Trailing zeros are kept when they are the decimal part: an
/// integral double is written `1.0`, and the formatter below turns that digit
/// into the `.0`.
///
/// The digits come from `ryu`, which resolves the choice between two equally
/// short spellings of the same double the way CPython's `repr` does. Rust's own
/// `{:e}` (Grisu) resolves that tie differently: `-847472097840887.2` formats as
/// `-847472097840887.3`, which is just as short and round-trips to the same
/// double, so a digest computed from it would disagree with every consumer.
fn shortest_decimal(value: f64) -> (String, i32) {
    let mut buffer = ryu::Buffer::new();
    let text = buffer.format_finite(value);
    let (negative, rest) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (mantissa, exponent) = match rest.split_once('e') {
        Some((mantissa, exponent)) => (
            mantissa,
            exponent
                .parse::<i32>()
                .expect("ryu writes an integer exponent"),
        ),
        None => (rest, 0),
    };
    let (integer, fraction) = match mantissa.split_once('.') {
        Some((integer, fraction)) => (integer, fraction),
        None => (mantissa, ""),
    };
    let written = format!("{integer}{fraction}");
    // Leading zeros are not significant digits; each one moves the decimal
    // point left. A zero or a value below one is the only source of them.
    let significant = written.trim_start_matches('0');
    let mut digits = significant.to_string();
    let mut decimals = i32::try_from(integer.len()).expect("a double's integer part fits")
        + exponent
        - i32::try_from(written.len() - significant.len()).expect("a double's digits fit");
    // Zero has no significant digit of its own: write the one that is there and
    // put the decimal point after it.
    if digits.is_empty() {
        digits.push('0');
        decimals = 1;
    }
    if negative {
        digits.insert(0, '-');
    }
    (digits, decimals)
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
    fn the_shortest_spelling_is_the_one_cpython_picks() {
        // Two spellings of the same double can both round-trip and be equally
        // short; Rust's `{:e}` picks ...873 here and CPython picks ...872. The
        // digits come from ryu for exactly this reason.
        assert_eq!(format_f64(-847472097840887.2), "-847472097840887.2");
        assert_eq!(format_f64(847472097840887.2), "847472097840887.2");
        assert_eq!(format_f64(1.0 / 3.0), "0.3333333333333333");
    }

    #[test]
    fn numbers_match_cpython_repr_on_a_generated_corpus() {
        // Every expected string is CPython's `repr` of that double, generated
        // once from the bit pattern: the corpus includes subnormals, the
        // extremes, tie cases and 220 pseudo-random finite doubles. A formatter
        // that only round-trips would pass the vectors and fail here.
        for (bits, expected) in [
            (0x0000000000000000, "0.0"),
            (0x8000000000000000, "-0.0"),
            (0x3ff0000000000000, "1.0"),
            (0xbff0000000000000, "-1.0"),
            (0x3ee4f8b588e368f1, "1e-05"),
            (0x3fb999999999999a, "0.1"),
            (0x4059000000000000, "100.0"),
            (0x4341c37937e08000, "1e+16"),
            (0x430c6bf526340000, "1000000000000000.0"),
            (0x0000000000000001, "5e-324"),
            (0x7fefffffffffffff, "1.7976931348623157e+308"),
            (0xc308162bcaca77ba, "-847472097840887.2"),
            (0x4308162bcaca77ba, "847472097840887.2"),
            (0x4340000000000000, "9007199254740992.0"),
            (0x3fd5555555555555, "0.3333333333333333"),
            (0x3fd3333333333334, "0.30000000000000004"),
            (0x7e37e43c8800759c, "1e+300"),
            (0xdc1b77ae0bf34dad, "-4.9911105725155504e+135"),
            (0x64f0eeb9026e6076, "1.715373926431966e+178"),
            (0x7b07ce91e5906136, "4.4251604027143375e+284"),
            (0x305f050c368dcc74, "1.0715660391465826e-75"),
            (0x2ceb16e0a1c54aec, "2.5973481493288907e-92"),
            (0x97101dce4e7bfb79, "-1.3475090132806154e-197"),
            (0x9ad2e144d6e8f2cf, "-1.81996730402717e-179"),
            (0xd9aa792e1af470ea, "-8.750186241947517e+123"),
            (0xddaa4e85b0d6e28b, "-1.603964615428183e+143"),
            (0x8f8ea9d349428d8e, "-9.643915712060552e-234"),
            (0x08f474ffb8e8ab15, "1.5860846119992697e-265"),
            (0x2ead854756d71f03, "7.597954524892738e-84"),
            (0x55bc79f8ada711fd, "1.0204796945730538e+105"),
            (0x0e1fc49bd63b809e, "1.1910593176322935e-240"),
            (0xb92199e83f5a101f, "-1.694933913128696e-33"),
            (0xc5765079fc5d43ff, "-4.3162252145519944e+26"),
            (0x353cfc387dfae6b8, "3.0261999441573203e-52"),
            (0xa32edabf5585bd75, "-3.238696681491661e-139"),
            (0xfc5639b16b714b4f, "-8.663725837306241e+290"),
            (0x92fb2dcfc8ae9a19, "-3.0797526843217145e-217"),
            (0x544b0ec76d00616d, "1.15589923121874e+98"),
            (0xbcbb9b7e9a319aef, "-3.831296467316889e-16"),
            (0x0f1a50d59c0aa21a, "6.466053826432699e-236"),
            (0x80ae2120826571de, "-2.1452846540129615e-305"),
            (0x0e1ecd02ed7c0cbd, "1.1547975125429853e-240"),
            (0x0d0981e8c1fa7be4, "7.296267179458751e-246"),
            (0xdc86b3d3cdbdf613, "-5.2803264785067434e+137"),
            (0x6d0844c269e7693f, "1.6732138965686944e+217"),
            (0x3681da7f6993082d, "3.909076791246025e-46"),
            (0x5b928e2c987d857d, "1.3170664051174186e+133"),
            (0x6c716e1e6ced8137, "2.347135155778617e+214"),
            (0xccb870213498f7f5, "-3.92703626520675e+61"),
            (0x9f07b27a78869b5a, "-3.3710794864173703e-159"),
            (0xb0ba91e47f6200ec, "-5.874229071700385e-74"),
            (0xc72b4c36d0db7fed, "-7.086891063466728e+34"),
            (0x35f305b0c0fc9252, "8.134710481112139e-49"),
            (0x7ac78fb373ffbff6, "2.7371952383912e+283"),
            (0x8e39b81de71b7d09, "-3.857101171397437e-240"),
            (0x59a69ba126cd29b3, "7.472552783215012e+123"),
            (0xbfcbffff0cbcbf20, "-0.21874988672197393"),
            (0x2d1ce28856d20e5e, "2.215603731797175e-91"),
            (0x572a15ed48b3fdc2, "7.841697833625344e+111"),
            (0x32b911499417aab9, "2.3803041954013153e-64"),
            (0x17a8ec88e0da0bac, "1.0669643833474769e-194"),
            (0x0cc88eab2f9ad4bb, "4.3902997915267284e-247"),
            (0xbc3a5b419a43afd2, "-1.4287870889761718e-18"),
            (0x6f0f3414c47c9c0d, "9.239942182940311e+226"),
            (0xe71a7567edb8c675, "-4.6049449625872434e+188"),
            (0x63674cf841ee8ab9, "7.034866163903114e+170"),
            (0xe375837da840d9ec, "-1.2990664698521692e+171"),
            (0x578eb22e1ea5a35f, "5.905686629582816e+113"),
            (0x4efa582ee029dfd9, "2.9091650704676072e+72"),
            (0xf3b363e938295a26, "-2.1692042628035588e+249"),
            (0x2e3a4ef3496f4112, "5.2900001119406624e-86"),
            (0xb809d78e2a2f9b10, "-9.49285634690637e-39"),
            (0x46217f104eb30026, "6.93101536724741e+29"),
            (0x8947b5fefda6afa6, "-5.882775229507853e-264"),
            (0x4a3ae29b2af0cb79, "3.9292734840969675e+49"),
            (0xbfd0273b10a6d4af, "-0.2523944532980051"),
            (0xed0a00eb302b52c6, "-1.7928344098460697e+217"),
            (0x3c0a45ad6d0175e3, "1.7802719962921167e-19"),
            (0x5bec25a986aa6fc8, "6.393254796400403e+134"),
            (0x904bc2dda0ebc917, "-3.576279648101772e-230"),
            (0x459ae6d82ef0bb45, "2.0814210227308495e+27"),
            (0xd8f30b16630d2b73, "-3.0734116139155754e+120"),
            (0x7110b72632258de5, "4.251812196359382e+236"),
            (0x543b14bc79e21fbe, "5.7844666878803994e+97"),
            (0xb3afb674d263f401, "-9.867437071726851e-60"),
            (0x992a5514fae813a9, "-1.891221305366931e-187"),
            (0x65950d0578f009ce, "2.18377227571136e+181"),
            (0x5703572bfb805a5d, "1.4535047277321816e+111"),
            (0xf58b6a5f9c786da9, "-1.6465799069278203e+258"),
            (0xa837793e9e8ad732, "-5.957466131998648e-115"),
            (0x50eabc7287dc4e1c, "6.340262899598194e+81"),
            (0x4dca1d57ff03f180, "5.500381791581099e+66"),
            (0x4899ff11c2f79663, "5.661506902655086e+41"),
            (0x16166c76ad45818f, "2.860815981281152e-202"),
            (0xcb74ca30b3d6894c, "-3.186028444038842e+55"),
            (0x217355871886f75e, "1.5120492207884386e-147"),
            (0x28a1e2393381ed30, "5.809632407259761e-113"),
            (0x074c31b6d9efa2ea, "1.6286721492574822e-273"),
            (0xbec4688d35b7872f, "-2.4328714240831068e-06"),
            (0xf8fd0016351ac3e1, "-6.275367084072634e+274"),
            (0x2a3e36405bf02e26, "3.29321318659287e-105"),
            (0x4fa794552d7c87fa, "5.332638378877033e+75"),
            (0x6b65fa68c812c075, "2.2579710060776677e+209"),
            (0xb0d838d4f45658b5, "-2.1420571626728534e-73"),
            (0xf088256e71b67944, "-1.1995980149915995e+234"),
            (0xb4d1bf49868fc4b6, "-2.89514964136048e-54"),
            (0x7f30634d239b36bf, "4.495301479005006e+304"),
            (0x9adf073ca7e24f12, "-2.9910257010919965e-179"),
            (0xbf023d5f71c40f0c, "-3.478953860461099e-05"),
            (0xf8daff4ab7e1c412, "-1.4604790788453035e+274"),
            (0x3bb60a1f6449431a, "4.667054448216033e-21"),
            (0xe1b6cc3fce8a571c, "-5.128277780879042e+162"),
            (0x974be547500504b2, "-1.865902080785903e-196"),
            (0x788637bdbb04623b, "3.756003170106989e+272"),
            (0x78b479878553843f, "2.769071305261943e+273"),
            (0xf614dc4037d1ccf7, "-6.414723702314457e+260"),
            (0xab5917a81f0fb2ae, "-7.17007679825171e-100"),
            (0x107db970f61ec64b, "3.063346448760151e-229"),
            (0xa7e591e85a840907, "-1.7107275877582857e-116"),
            (0x42e87faaebb9a0d5, "215492859907334.66"),
            (0x7cb7d0a0e9b446d4, "5.941355978696207e+292"),
            (0x93b411557b1e1b59, "-9.314100426966256e-214"),
            (0x17282941775bd12f, "4.040271440024106e-197"),
            (0x1d6eb56cc3fecd4d, "6.5095912305809035e-167"),
            (0x9cc6d38ebb4dc397, "-4.725304702312625e-170"),
            (0x93e7f09c76995dd0, "-8.889046505961924e-213"),
            (0x40481181f54f1b6b, "48.13677851069118"),
            (0xfc10536a39343f9d, "-3.977510832916913e+289"),
            (0x2b59a11006fe10a2, "7.323449055534529e-100"),
            (0xb25de04b53658403, "-4.43267188156626e-66"),
            (0x482f7b77f3b42fcb, "5.3564356221290984e+39"),
            (0xaa19ef48de09d554, "-7.067451838786451e-106"),
            (0xbc1efbb4d09613fe, "-4.199010356643058e-19"),
            (0x01640cfd5e7e0059, "5.847695999398785e-302"),
            (0x2103a6ef6f7bca19, "1.2007177480205567e-149"),
            (0xba59dbbf218e9bcd, "-1.3055148249443392e-27"),
            (0x47cfbdd44466d5ba, "8.43834345315447e+37"),
            (0x0f7180ea42ce3691, "2.752517763735277e-234"),
            (0x1d57d70abe6c2ebc, "2.526788028536205e-167"),
            (0x3df4865f060ed9e1, "2.9867642218664106e-10"),
            (0x9cc2d942d8ac9c12, "-3.9018746502228826e-170"),
            (0x68c29c22e6ec81aa, "4.347217825559248e+196"),
            (0xa1cd5d166c667229, "-7.348551753937433e-146"),
            (0x950c6175be7d148d, "-2.762460922630013e-207"),
            (0x3f3e42bdb31d6de4, "0.0004617417463259317"),
            (0xb8e8620efee2ae3f, "-1.4675009188927917e-34"),
            (0x6892f24f073504a3, "5.532311359466528e+195"),
            (0x7faf4d50b83a266a, "1.0990510499741633e+307"),
            (0x36eff6fe424288a6, "4.479224922930724e-44"),
            (0x7436a02ab71ee437, "6.479738675343636e+251"),
            (0x833a63ba9131343f, "-4.131963459956029e-293"),
            (0x992cb64cfdf7b997, "-2.0621386115069104e-187"),
            (0xc8b93e004898d324, "-2.1989050319881327e+42"),
            (0x645d2dce235dab82, "2.8867342589916004e+175"),
            (0x808a9a75bdabb055, "-4.735555107626997e-306"),
            (0x995531f7eedc5275, "-1.217811298351344e-186"),
            (0x087d63039379d791, "8.900118122652927e-268"),
            (0xf3c9caffb024e07e, "-5.770954824128418e+249"),
            (0xea06a464360f763e, "-5.546063542174503e+202"),
            (0x53361012dddd2752, "7.190830824723893e+92"),
            (0xd66a74b438fd099c, "-1.941648404758352e+108"),
            (0x8d5497b6e7e3148f, "-1.88493094519322e-244"),
            (0x00252f2c32b41166, "5.892070161490275e-308"),
            (0xf4dba3db8471e0c4, "-8.105792111531084e+254"),
            (0xa447b4c07713b205, "-6.523059653436162e-134"),
            (0x58d68ff52d13b421, "9.103372873476245e+119"),
            (0x7c3d90428332bb09, "2.8810573598215993e+290"),
            (0x98e09052e5853c3f, "-7.435185103913208e-189"),
            (0xb90814763588d987, "-5.797031974500124e-34"),
            (0x04349ea8330d49f4, "2.1158634801682737e-288"),
            (0x213e49f755c9ae67, "1.480492072589796e-148"),
            (0xadc15b82753344fb, "-2.726700493055371e-88"),
            (0x69a0dc20acf37cb2, "6.45269948043362e+200"),
            (0x729bfdea5375f6cb, "1.1945606721328604e+244"),
            (0xf8eb32047bbbcfe6, "-2.9423966222416894e+274"),
            (0x4b85228ee7b181f9, "6.477856523538338e+55"),
            (0x41031bc75f55bcba, "156536.9215502495"),
            (0x2da461eb41957943, "8.004766223603229e-89"),
            (0xd74e810a9b826371, "-3.667973268328953e+112"),
            (0x1cdd495915ad9bf7, "1.2125234815256259e-169"),
            (0xbaeb7806c59edd00, "-7.100532164112103e-25"),
            (0x3ba89ff13576a0ba, "2.6072493943615606e-21"),
            (0x005ca96b1855237b, "6.377460053650196e-307"),
            (0xc7b1d77110b837fd, "-2.37155129898773e+37"),
            (0x69bcfe5aebcf18d2, "2.2193146864212394e+201"),
            (0x6f51d1ecaa02f263, "1.688592499278879e+228"),
            (0x6d8dc769b3280f47, "5.256011819884271e+219"),
            (0xb51767a476976e99, "-6.108951742016336e-53"),
            (0x0418c241fa7ac604, "6.351467798285668e-289"),
            (0xa581230adaef3288, "-4.944515222634075e-128"),
            (0xf1ac9d290f1d4eed, "-3.726522459630376e+239"),
            (0x3dabc8a9eded6f30, "1.263462896392155e-11"),
            (0x55cb59e89b5779ee, "1.9603110021981586e+105"),
            (0x72a54713a9b86c9d, "1.8160593387967868e+244"),
            (0xa3726687db6b9b04, "-6.18061749778593e-138"),
            (0x5fdf2267673f0d32, "6.52256133253205e+153"),
            (0x68dfe63578c47fa8, "1.4903137223520643e+197"),
            (0x624e220196711d57, "3.4704648912517096e+165"),
            (0xbe9e598ebfea4aad, "-4.52247775153582e-07"),
            (0xd79af3ca3ba29578, "-1.037084836001061e+114"),
            (0x6f5e5868097b8e52, "2.8754690828576924e+228"),
            (0x5af01374addcadce, "1.1143358899359637e+130"),
            (0x2faaf7e9e83fa715, "4.5489015505785396e-79"),
            (0xbb42e4b2bad2bd1b, "-3.1256429291924854e-23"),
            (0xd30563d941e93ea1, "-8.714453691721558e+91"),
            (0x1167ea9167c9649c, "8.076568837115626e-225"),
            (0xb6249a7c39665155, "-7.048729021186279e-48"),
            (0xb3f56e3e83c468b7, "-2.1338113414315463e-58"),
            (0xa87bdea4d9832da6, "-1.1317069599854738e-113"),
            (0xe437318091b5827d, "-5.736425033777084e+174"),
            (0x59e1b682e1cbd639, "9.367392914269608e+124"),
            (0x7d32511b3390efd5, "1.1698384100740314e+295"),
            (0x1d70ed6acaa29b4a, "7.176504206107081e-167"),
            (0x1e71c6a67d004cfc, "4.938963803661829e-162"),
            (0xa0c0b3f99ebcf365, "-6.378335818567057e-151"),
            (0x498e468dbb57f3c3, "2.160539023382998e+46"),
            (0x1c54594f126dcce4, "3.290960217035386e-172"),
            (0x16ba1d39f25cae7d, "3.4116072635664438e-199"),
            (0x67de1f8cd39e2861, "2.1474297741370636e+192"),
            (0x108e0b8ab05d2c71, "6.192795587813568e-229"),
            (0x96288ccb432aaa69, "-6.264176696201658e-202"),
            (0x887013f1c0b1457d, "-4.86937695727198e-268"),
            (0xe47a782d96a0d8b7, "-1.0474717780590377e+176"),
            (0xc0f1c836d72154c6, "-72835.42752202143"),
            (0x01ea4c72979ce7ef, "1.9634838852369833e-299"),
            (0xe58cc45382b7c5e0, "-1.4921087049120546e+181"),
            (0x0f265892d429d26b, "1.098128118575969e-235"),
            (0x119be96337a67b0f, "7.540654791281066e-224"),
            (0xad7f4ee4ff441439, "-1.5369479028680968e-89"),
            (0x69765cdf019ab251, "1.069849989958128e+200"),
            (0x6c83796be9953375, "5.244798957954977e+214"),
            (0xb30aa29935c26453, "-8.093307970654829e-63"),
            (0x88c5405c130c945b, "-2.0595934067202045e-266"),
            (0x9ffcaab00de2fbb3, "-1.3362898797649858e-154"),
            (0xf60b6a392f39b284, "-4.215154408802224e+260"),
            (0x34f99df66499e0e1, "1.6715899149649308e-53"),
            (0xb8c6fbae04f4cb60, "-3.458116161801038e-35"),
            (0xddf1a401094ffaf6, "-3.4418528054556186e+144"),
            (0x24e629d896fb1883, "6.244957363033692e-131"),
            (0xe2fd7570a6e4ae72, "-6.948513516814706e+168"),
        ] {
            assert_eq!(format_f64(f64::from_bits(bits)), expected, "{bits:#018x}");
        }
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
