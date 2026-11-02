//! Normalized state snapshot.
//!
//! A `StateSnapshot` is the only view a decision provider sees. It is produced
//! by a domain `StateProvider` and must not expose internal database models.

use serde::{Deserialize, Serialize};

use crate::digest::{digest_value, DigestError};
use crate::version::{ContractError, SCHEMA_VERSION};

/// The thing a decision is about. Deliberately generic.
///
/// Serialized field is `type` (see `contracts/*.schema.json`); the Rust field
/// is named `kind` because `type` is a keyword.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Subject {
    /// Domain-defined type, e.g. `task`, `verification`, `dataset`.
    #[serde(rename = "type")]
    pub kind: String,
    /// Stable identifier within the source system.
    pub id: String,
}

impl Subject {
    pub fn new(kind: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            id: id.into(),
        }
    }
}

/// A single normalized fact about the subject.
///
/// Facts use a dotted, domain-neutral key namespace (e.g.
/// `observation.category`, `retry.attempt`, `retry.budget`). The value is
/// open JSON so adapters can carry domain detail without changing the core.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Fact {
    pub key: String,
    pub value: serde_json::Value,
}

impl Fact {
    pub fn new(key: impl Into<String>, value: serde_json::Value) -> Self {
        Self {
            key: key.into(),
            value,
        }
    }

    pub fn string(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(key, serde_json::Value::String(value.into()))
    }

    pub fn integer(key: impl Into<String>, value: i64) -> Self {
        Self::new(key, serde_json::Value::from(value))
    }
}

/// Normalized state visible to a decision provider.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StateSnapshot {
    pub schema_version: u32,
    pub snapshot_id: String,
    /// RFC 3339 timestamp, supplied by the caller.
    pub timestamp: String,
    /// Domain namespace, e.g. `software`, `data`.
    pub domain: String,
    pub subject: Subject,
    #[serde(default)]
    pub facts: Vec<Fact>,
    /// Capability ids advertised as available in this state.
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// Domain-neutral constraint tokens (e.g. `approval_required`).
    #[serde(default)]
    pub constraints: Vec<String>,
}

impl StateSnapshot {
    /// Build a snapshot at the current contract version.
    pub fn new(
        snapshot_id: impl Into<String>,
        timestamp: impl Into<String>,
        domain: impl Into<String>,
        subject: Subject,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            snapshot_id: snapshot_id.into(),
            timestamp: timestamp.into(),
            domain: domain.into(),
            subject,
            facts: Vec::new(),
            capabilities: Vec::new(),
            constraints: Vec::new(),
        }
    }

    pub fn with_fact(mut self, fact: Fact) -> Self {
        self.facts.push(fact);
        self
    }

    pub fn with_capability(mut self, id: impl Into<String>) -> Self {
        self.capabilities.push(id.into());
        self
    }

    pub fn with_constraint(mut self, id: impl Into<String>) -> Self {
        self.constraints.push(id.into());
        self
    }

    /// Look up a fact by key.
    pub fn fact(&self, key: &str) -> Option<&serde_json::Value> {
        self.facts
            .iter()
            .find(|fact| fact.key == key)
            .map(|fact| &fact.value)
    }

    /// Read a fact as a string.
    pub fn fact_str(&self, key: &str) -> Option<&str> {
        self.fact(key).and_then(|value| value.as_str())
    }

    /// Read a fact as an integer.
    pub fn fact_i64(&self, key: &str) -> Option<i64> {
        self.fact(key).and_then(|value| value.as_i64())
    }

    /// Deterministic content digest of this snapshot: the lowercase hex
    /// SHA-256 of its canonical form, exactly as a consumer recomputes it.
    ///
    /// The canonical form is defined once in [`crate::digest`] and published in
    /// `docs/IDENTITY_DIGEST.md`: object keys sorted by code point, no
    /// whitespace, every character outside `0x20..=0x7e` escaped, numbers in
    /// CPython's form, UTF-8 bytes. The object digested is the snapshot's
    /// complete serialized form, so a consumer digests the payload it received
    /// without adding, removing or normalizing a field. Changing any fact, id,
    /// timestamp or capability changes the digest. A consumer compares this
    /// against the digest it recorded for the snapshot it decided from; Ictus
    /// never accepts a digest as input.
    pub fn content_digest(&self) -> Result<String, DigestError> {
        let value = serde_json::to_value(self).expect("a StateSnapshot is always serializable");
        digest_value(&value)
    }

    /// Structural validation independent of any policy.
    pub fn validate(&self) -> Result<(), ContractError> {
        ContractError::check_version(self.schema_version)?;
        ContractError::require_non_empty("snapshot_id", &self.snapshot_id)?;
        ContractError::require_non_empty("domain", &self.domain)?;
        ContractError::require_non_empty("subject.kind", &self.subject.kind)?;
        ContractError::require_non_empty("subject.id", &self.subject.id)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Fact, StateSnapshot, Subject};

    fn snapshot() -> StateSnapshot {
        StateSnapshot::new(
            "snap-1",
            "2026-10-01T00:00:00Z",
            "software",
            Subject::new("task", "t1"),
        )
        .with_fact(Fact::integer("attempts.semantic", 0))
        .with_capability("demo.verify")
    }

    fn digest(snapshot: &StateSnapshot) -> String {
        snapshot.content_digest().expect("in-domain snapshot")
    }

    #[test]
    fn content_digest_is_sha256_of_the_canonical_form() {
        // Pinned vector. The canonical form is this exact text: object keys
        // sorted by code point, compact separators, UTF-8. Any change to key
        // ordering, separators or hashing fails here, which is what keeps two
        // Ictus boundaries from silently disagreeing about a snapshot. The
        // published vectors in `contracts/digest-vectors.json` cover the rest.
        //
        // {"capabilities":["demo.verify"],"constraints":[],"domain":"software",
        //  "facts":[{"key":"attempts.semantic","value":0}],"schema_version":1,
        //  "snapshot_id":"snap-1","subject":{"id":"t1","type":"task"},
        //  "timestamp":"2026-10-01T00:00:00Z"}
        assert_eq!(
            digest(&snapshot()),
            "38a4835e821a6e9c7ff1f257b0a7ca4459c0f9d6f7bbcc06d543a31f35eac86e"
        );
    }

    #[test]
    fn content_digest_is_stable_and_lowercase_hex() {
        let first = digest(&snapshot());
        let second = digest(&snapshot());
        assert_eq!(first, second);
        assert_eq!(first.len(), 64);
        assert!(first
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
    }

    #[test]
    fn changing_one_fact_changes_the_digest() {
        let base = digest(&snapshot());
        for mutated in [
            snapshot().with_fact(Fact::integer("attempts.step_retries", 1)),
            snapshot().with_constraint("approval_required"),
            StateSnapshot {
                timestamp: "2026-10-01T00:00:01Z".to_string(),
                ..snapshot()
            },
            StateSnapshot {
                subject: Subject::new("task", "t2"),
                ..snapshot()
            },
        ] {
            assert_ne!(digest(&mutated), base);
        }
        // Changing a fact's value (not just adding one) also changes it.
        let revalued = StateSnapshot::new(
            "snap-1",
            "2026-10-01T00:00:00Z",
            "software",
            Subject::new("task", "t1"),
        )
        .with_fact(Fact::integer("attempts.semantic", 1))
        .with_capability("demo.verify");
        assert_ne!(digest(&revalued), base);
    }
}
