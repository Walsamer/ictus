//! The published identity-digest vectors, asserted as data.
//!
//! `contracts/digest-vectors.json` is the artefact a consumer pins. Every
//! expected digest in it was computed by the reference implementation named in
//! `docs/IDENTITY_DIGEST.md` (CPython's `json.dumps(..., sort_keys=True,
//! separators=(",", ":"), ensure_ascii=True)` over the payload, then SHA-256),
//! never by the Rust writer this test exercises — otherwise the test would only
//! prove self-consistency.
//!
//! Everything in the file is asserted: each digested case twice (the payload
//! must be the contract's complete serialized form, and the Rust implementation
//! must reproduce the published digest), each acceptance document (the ingress
//! must let it through, and it must digest as published), and each rejection by
//! the rule and the path the file names.

use ictus_core::digest::{check_number_domain, check_schema_version, digest_value, DigestError};
use ictus_core::{DecisionProposal, StateSnapshot, DIGEST_SCHEMA_VERSION};
use serde_json::Value;

const VECTORS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../contracts/digest-vectors.json"
));

fn vectors() -> Value {
    serde_json::from_str(VECTORS).expect("the vector file is JSON")
}

/// The digest of a payload through the subject's own implementation.
fn digest_of(digests: &str, payload: &Value, name: &str) -> String {
    match digests {
        "snapshot" => {
            let snapshot: StateSnapshot = serde_json::from_value(payload.clone())
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            // The payload is the complete serialized contract, so a consumer
            // can digest what it received without normalizing a field.
            assert_eq!(
                serde_json::to_value(&snapshot).unwrap(),
                *payload,
                "{name}: the payload is not the contract's serialized form"
            );
            snapshot
                .content_digest()
                .unwrap_or_else(|error| panic!("{name}: {error}"))
        }
        "proposal" => {
            let proposal: DecisionProposal = serde_json::from_value(payload.clone())
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(
                serde_json::to_value(&proposal).unwrap(),
                *payload,
                "{name}: the payload is not the contract's serialized form"
            );
            ictus_core::digest::proposal_digest(&proposal)
                .unwrap_or_else(|error| panic!("{name}: {error}"))
        }
        // An intent payload is digested by the Python boundary, not by Rust, so
        // the shared algorithm is asserted over it directly.
        "intent" => digest_value(payload).unwrap_or_else(|error| panic!("{name}: {error}")),
        other => panic!("{name}: unknown digests {other:?}"),
    }
}

#[test]
fn the_vector_file_is_the_version_this_build_implements() {
    let vectors = vectors();
    assert_eq!(vectors["schema_version"], DIGEST_SCHEMA_VERSION);
    assert_eq!(
        vectors["algorithm"],
        "sorted-keys-compact-ascii-escaped-cpython-number"
    );
    assert_eq!(vectors["hash"], "sha256");
    assert_eq!(vectors["encoding"], "lowercase-hex");
    // An unknown version fails closed rather than being read as a successor.
    assert!(check_schema_version(DIGEST_SCHEMA_VERSION + 1).is_err());
}

#[test]
fn every_published_case_digests_to_its_published_value() {
    let vectors = vectors();
    let cases = vectors["cases"].as_array().expect("cases is an array");
    assert!(
        cases.len() >= 16,
        "the vector set shrank to {}",
        cases.len()
    );
    for case in cases {
        let name = case["name"].as_str().expect("a case has a name");
        let expected = case["sha256"].as_str().expect("a case has a digest");
        let digests = case["digests"].as_str().expect("a case has a subject");
        let actual = digest_of(digests, &case["payload"], name);
        assert_eq!(actual, expected, "{name}");
    }
}

#[test]
fn keys_in_a_different_order_digest_alike() {
    // The three `*-key-order` cases carry the same content as a case written in
    // sorted order: canonicalization has to erase the difference.
    let vectors = vectors();
    let cases = vectors["cases"].as_array().expect("cases is an array");
    let digest_of = |name: &str| {
        cases
            .iter()
            .find(|case| case["name"] == name)
            .map(|case| case["sha256"].as_str().unwrap().to_string())
            .unwrap_or_else(|| panic!("case {name} is missing"))
    };
    assert_eq!(
        digest_of("snapshot-key-order"),
        digest_of("snapshot-minimal")
    );
    assert_eq!(
        digest_of("proposal-key-order"),
        digest_of("proposal-reexecute-minimal")
    );
    assert_eq!(digest_of("intent-key-order"), digest_of("intent-minimal"));
}

#[test]
fn every_published_acceptance_is_accepted_and_digests_as_published() {
    let vectors = vectors();
    let accepts = vectors["accepts"].as_array().expect("accepts is an array");
    assert!(!accepts.is_empty(), "the acceptance set is empty");
    for acceptance in accepts {
        let name = acceptance["name"].as_str().expect("a case has a name");
        let document = acceptance["document"].as_str().expect("a document");
        let expected = acceptance["sha256"].as_str().expect("a digest");
        let digests = acceptance["digests"].as_str().expect("a subject");
        // The ingress must not refuse text the protocol carries...
        check_number_domain(document).unwrap_or_else(|error| panic!("{name}: {error}"));
        // ...and the payload it carries must digest to the published value.
        let payload: Value = serde_json::from_str(document).expect("an accepted document is JSON");
        assert_eq!(digest_of(digests, &payload, name), expected, "{name}");
    }
}

#[test]
fn every_published_rejection_is_refused_by_the_rule_it_names() {
    let vectors = vectors();
    let rejections = vectors["rejections"]
        .as_array()
        .expect("rejections is an array");
    assert!(!rejections.is_empty(), "the rejection set is empty");
    for rejection in rejections {
        let name = rejection["name"].as_str().expect("a rejection has a name");
        assert_eq!(rejection["expect"], "refusal", "{name}");
        let document = rejection["document"].as_str().expect("a document");
        let reason = rejection["reason"].as_str().expect("a reason");
        let path = rejection["path"].as_str().expect("the path it must name");
        let Err(error) = check_number_domain(document) else {
            panic!("{name}: the ingress accepted a document the file refuses");
        };
        assert_eq!(error.reason(), reason, "{name}");
        assert_eq!(error.path(), path, "{name}");
        assert!(
            error.to_string().starts_with(&format!("{path}:")),
            "{name}: the message must name the path, got {error}"
        );
    }
}

#[test]
fn an_out_of_domain_integer_is_not_hashed_as_its_nearest_double() {
    // The failure this replaces: the same literal parsed by a consumer that can
    // represent it exactly would digest a different payload, and Ictus would
    // have bound an identity to an approximated value.
    let document = r#"{"facts":[{"key":"big","value":18446744073709551616}]}"#;
    let error = check_number_domain(document).unwrap_err();
    assert!(matches!(error, DigestError::NumberOutOfDomain { .. }));
    assert_eq!(error.path(), "$.facts[0].value");
    // The parser does accept the document — by rounding — which is exactly why
    // the guard has to run on the text.
    assert!(serde_json::from_str::<Value>(document).is_ok());
}

#[test]
fn a_rounding_literal_is_not_hashed_as_its_nearest_double() {
    // The same failure, one step along: this literal is *inside* the domain but
    // is not the double it parses to, so Rust and a consumer reading the text
    // with a wider type would digest different payloads.
    let document = r#"{"facts":[{"key":"rounded","value":0.1000000000000000055511151}]}"#;
    let error = check_number_domain(document).unwrap_err();
    assert!(matches!(error, DigestError::NumberWouldRound { .. }));
    assert_eq!(error.reason(), "number-would-round");
    assert_eq!(error.path(), "$.facts[0].value");
    assert!(serde_json::from_str::<Value>(document).is_ok());
}
