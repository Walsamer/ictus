//! Deterministic rule-based decision provider.
//!
//! This is one `DecisionProvider` implementation. A specialized model, an LLM
//! or a human could produce the same typed proposal; the core treats them
//! uniformly. Rules run first because they are reproducible and auditable.
//!
//! It reads only domain-neutral facts from the snapshot:
//!
//! - `observation.category` (SCREAMING_SNAKE_CASE)
//! - the canonical semantic attempt accounting `attempts.semantic` /
//!   `attempts.max_semantic` (with the complete legacy v1 pair
//!   `retry.attempt` / `retry.budget` still accepted). Missing or ambiguous
//!   accounting escalates: there is no implicit `0/0` exhausted fallback.
//! - `capability.id` (the capability to re-execute)
//! - `observation.message` (optional)
//!
//! This v1 provider emits the frozen `DecisionProposal` vocabulary. Success is
//! intentionally still a proposal-shaped `ABORT` (the vocabulary has no no-op),
//! but it is tagged with `recovery_outcome = "NO_RECOVERY_WORK"` so a
//! cross-system consumer can never mistake it for domain retirement. The
//! authoritative bounded [`crate::recovery::RecoveryDecision`] is produced by
//! [`crate::recovery::GenericRecoveryProvider`].

use ictus_core::{
    DecisionKind, DecisionProposal, ObservationCategory, ProviderMetadata, StateSnapshot,
};
use ictus_ports::{DecisionProvider, PortError};

use crate::recovery::semantic_budget_from_snapshot;

/// The deterministic rules provider id used in proposal provenance.
pub const PROVIDER_ID: &str = "rules.v1";

fn parse_category(value: Option<&str>) -> ObservationCategory {
    match value {
        Some("SUCCESS") => ObservationCategory::Success,
        Some("WORKER_TIMEOUT") => ObservationCategory::WorkerTimeout,
        Some("PROCESS_CRASH") => ObservationCategory::ProcessCrash,
        Some("VERIFICATION_FAILURE") => ObservationCategory::VerificationFailure,
        Some("INTEGRATION_CONFLICT") => ObservationCategory::IntegrationConflict,
        Some("RESOURCE_EXHAUSTED") => ObservationCategory::ResourceExhausted,
        Some("PROVIDER_UNAVAILABLE") => ObservationCategory::ProviderUnavailable,
        _ => ObservationCategory::Unknown,
    }
}

/// A deterministic decision provider that maps an observation plus re-execution
/// budget onto a bounded proposal.
#[derive(Debug, Clone, Copy, Default)]
pub struct RuleDecisionProvider;

impl RuleDecisionProvider {
    pub fn new() -> Self {
        Self
    }
}

impl DecisionProvider for RuleDecisionProvider {
    fn propose(&self, snapshot: &StateSnapshot) -> Result<DecisionProposal, PortError> {
        snapshot.validate()?;
        let category = parse_category(snapshot.fact_str("observation.category"));
        let capability = snapshot
            .fact_str("capability.id")
            .map(str::to_string)
            .filter(|value| !value.trim().is_empty());

        let mut proposal = DecisionProposal::new(
            format!("proposal:{}", snapshot.snapshot_id),
            DecisionKind::Escalate,
            snapshot.subject.clone(),
            ProviderMetadata::rules(PROVIDER_ID).with_version(env!("CARGO_PKG_VERSION")),
            snapshot.timestamp.clone(),
        );

        // INITIAL authorization is not recovery: an INITIAL snapshot carries no
        // execution observation at all. The pinned provider previously mapped a
        // missing `observation.category` to UNKNOWN and escalated, so an initial
        // authorization could never produce an executable intent. It is now an
        // explicit, fail-closed branch: no observation facts → capability
        // request → EXECUTE_CAPABILITY. Any observation fact restores recovery
        // semantics unchanged below.
        let has_observation = snapshot
            .facts
            .iter()
            .any(|fact| fact.key == "observation.category" || fact.key.starts_with("observation."));
        if !has_observation {
            if snapshot.fact_str("recovery.attempt_id").is_some()
                || snapshot.fact_str("recovery.intent_id").is_some()
            {
                proposal.reason = Some(
                    "initial snapshot carries recovery attempt facts; refusing to authorize".into(),
                );
                proposal.validate()?;
                return Ok(proposal);
            }
            match capability {
                None => {
                    proposal.reason =
                        Some("initial snapshot requests no capability to execute".into());
                }
                Some(capability_id) => match semantic_budget_from_snapshot(snapshot) {
                    Ok(budget) if budget.semantic_attempts > 0 => {
                        proposal.reason = Some(format!(
                            "initial snapshot reports {} prior semantic attempts; \
                                 a recovery snapshot is required",
                            budget.semantic_attempts
                        ));
                    }
                    _ => {
                        proposal.decision = DecisionKind::ExecuteCapability;
                        proposal.capability = Some(capability_id);
                        proposal.reason = Some(
                            "initial authorization: no prior attempt and no failure \
                                 observation"
                                .into(),
                        );
                    }
                },
            }
            proposal.validate()?;
            return Ok(proposal);
        }

        match category {
            ObservationCategory::Success => {
                proposal.decision = DecisionKind::Abort;
                proposal.reason = Some(
                    "observation reports success; no semantic recovery work (not domain retirement)"
                        .into(),
                );
                // Make the no-recovery-work semantics machine-checkable so a
                // cross-system fixture cannot read `ABORT` as retirement.
                proposal = proposal.with_argument(
                    "recovery_outcome",
                    serde_json::Value::String("NO_RECOVERY_WORK".to_string()),
                );
            }
            ObservationCategory::WorkerTimeout
            | ObservationCategory::ProcessCrash
            | ObservationCategory::ProviderUnavailable
            | ObservationCategory::ResourceExhausted => {
                match semantic_budget_from_snapshot(snapshot) {
                    Err(error) => {
                        proposal.decision = DecisionKind::Escalate;
                        proposal.reason =
                            Some(format!("ambiguous semantic attempt accounting: {error}"));
                    }
                    Ok(budget) if budget.is_exhausted() => {
                        proposal.decision = DecisionKind::Escalate;
                        proposal.reason = Some(format!(
                            "{category:?} with exhausted semantic attempt budget ({} of {})",
                            budget.semantic_attempts, budget.max_semantic_attempts
                        ));
                    }
                    Ok(_) => {
                        if capability.is_none() {
                            proposal.decision = DecisionKind::Escalate;
                            proposal.reason = Some(
                                "re-executable failure but no capability id to re-execute".into(),
                            );
                        } else {
                            proposal.decision = DecisionKind::Reexecute;
                            proposal.capability = capability;
                            proposal.reason = Some(format!(
                                "re-executable {category:?} within the semantic attempt budget"
                            ));
                        }
                    }
                }
            }
            ObservationCategory::VerificationFailure | ObservationCategory::IntegrationConflict => {
                proposal.decision = DecisionKind::Escalate;
                proposal.reason = Some(format!(
                    "{category:?} needs a domain decision; rules do not decompose or repair"
                ));
            }
            ObservationCategory::Unknown => {
                proposal.decision = DecisionKind::Escalate;
                proposal.reason = Some("unclassified observation".into());
            }
        }

        if let Some(message) = snapshot.fact_str("observation.message") {
            proposal = proposal.with_argument(
                "observation_message",
                serde_json::Value::String(message.to_string()),
            );
        }

        proposal.validate()?;
        Ok(proposal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ictus_core::{Fact, Subject};

    fn snapshot(category: &str, attempt: i64, budget: i64) -> StateSnapshot {
        StateSnapshot::new(
            "s1",
            "2026-10-01T00:00:00Z",
            "software",
            Subject::new("task", "t1"),
        )
        .with_fact(Fact::string("observation.category", category))
        .with_fact(Fact::integer("retry.attempt", attempt))
        .with_fact(Fact::integer("retry.budget", budget))
        .with_fact(Fact::string("capability.id", "demo.verify"))
    }

    #[test]
    fn timeout_within_budget_reexecutes() {
        let proposal = RuleDecisionProvider::new()
            .propose(&snapshot("WORKER_TIMEOUT", 0, 2))
            .unwrap();
        assert_eq!(proposal.decision, DecisionKind::Reexecute);
        assert_eq!(proposal.capability.as_deref(), Some("demo.verify"));
    }

    #[test]
    fn timeout_with_exhausted_budget_escalates() {
        let proposal = RuleDecisionProvider::new()
            .propose(&snapshot("WORKER_TIMEOUT", 2, 2))
            .unwrap();
        assert_eq!(proposal.decision, DecisionKind::Escalate);
    }

    #[test]
    fn verification_failure_escalates_without_decomposing() {
        let proposal = RuleDecisionProvider::new()
            .propose(&snapshot("VERIFICATION_FAILURE", 0, 5))
            .unwrap();
        assert_eq!(proposal.decision, DecisionKind::Escalate);
        // The category must be recognised, not silently degraded to Unknown.
        assert!(proposal
            .reason
            .as_deref()
            .unwrap_or_default()
            .contains("VerificationFailure"));
    }

    #[test]
    fn integration_conflict_escalates_and_keeps_its_category() {
        let proposal = RuleDecisionProvider::new()
            .propose(&snapshot("INTEGRATION_CONFLICT", 0, 5))
            .unwrap();
        assert_eq!(proposal.decision, DecisionKind::Escalate);
        assert!(proposal
            .reason
            .as_deref()
            .unwrap_or_default()
            .contains("IntegrationConflict"));
    }

    #[test]
    fn provider_unavailable_within_budget_reexecutes() {
        let proposal = RuleDecisionProvider::new()
            .propose(&snapshot("PROVIDER_UNAVAILABLE", 0, 2))
            .unwrap();
        assert_eq!(proposal.decision, DecisionKind::Reexecute);
        assert_eq!(proposal.capability.as_deref(), Some("demo.verify"));
    }

    #[test]
    fn success_aborts() {
        let proposal = RuleDecisionProvider::new()
            .propose(&snapshot("SUCCESS", 0, 5))
            .unwrap();
        assert_eq!(proposal.decision, DecisionKind::Abort);
        // `ABORT` here means "no recovery work", never domain retirement.
        assert_eq!(
            proposal.arguments["recovery_outcome"],
            serde_json::json!("NO_RECOVERY_WORK")
        );
    }

    #[test]
    fn missing_accounting_escalates_instead_of_assuming_zero_of_zero() {
        let snapshot = StateSnapshot::new(
            "s1",
            "2026-10-01T00:00:00Z",
            "software",
            Subject::new("task", "t1"),
        )
        .with_fact(Fact::string("observation.category", "WORKER_TIMEOUT"))
        .with_fact(Fact::string("capability.id", "demo.verify"));
        let proposal = RuleDecisionProvider::new().propose(&snapshot).unwrap();
        assert_eq!(proposal.decision, DecisionKind::Escalate);
        assert!(proposal
            .reason
            .as_deref()
            .unwrap_or_default()
            .contains("ambiguous semantic attempt accounting"));
    }

    #[test]
    fn canonical_accounting_is_used_over_the_legacy_pair() {
        let snapshot = StateSnapshot::new(
            "s1",
            "2026-10-01T00:00:00Z",
            "software",
            Subject::new("task", "t1"),
        )
        .with_fact(Fact::string("observation.category", "WORKER_TIMEOUT"))
        .with_fact(Fact::integer("attempts.semantic", 1))
        .with_fact(Fact::integer("attempts.max_semantic", 2))
        // A conflicting legacy pair must not win.
        .with_fact(Fact::integer("retry.attempt", 9))
        .with_fact(Fact::integer("retry.budget", 9))
        .with_fact(Fact::string("capability.id", "demo.verify"));
        let proposal = RuleDecisionProvider::new().propose(&snapshot).unwrap();
        assert_eq!(proposal.decision, DecisionKind::Reexecute);
    }

    #[test]
    fn decisions_are_reproducible() {
        let provider = RuleDecisionProvider::new();
        let first = provider.propose(&snapshot("PROCESS_CRASH", 1, 3)).unwrap();
        let second = provider.propose(&snapshot("PROCESS_CRASH", 1, 3)).unwrap();
        assert_eq!(first, second);
    }
}

#[cfg(test)]
mod initial_authorization_tests {
    use super::*;
    use ictus_core::snapshot::Fact;
    use ictus_core::Subject;

    fn base() -> StateSnapshot {
        StateSnapshot::new(
            "snap-initial-1",
            "2026-10-19T00:00:00Z",
            "software",
            Subject::new("work_order", "WO-1"),
        )
        .with_fact(Fact::string("capability.id", "demo.verify"))
    }

    #[test]
    fn initial_snapshot_authorizes_execute_capability() {
        let proposal = RuleDecisionProvider::new()
            .propose(&base())
            .expect("proposal");
        assert_eq!(proposal.decision, DecisionKind::ExecuteCapability);
        assert_eq!(proposal.capability.as_deref(), Some("demo.verify"));
        assert!(proposal
            .reason
            .as_deref()
            .unwrap()
            .contains("initial authorization"));
    }

    #[test]
    fn initial_snapshot_without_capability_fails_closed() {
        let mut snapshot = base();
        snapshot.facts.clear();
        let proposal = RuleDecisionProvider::new()
            .propose(&snapshot)
            .expect("proposal");
        assert_eq!(proposal.decision, DecisionKind::Escalate);
        assert!(proposal.capability.is_none());
    }

    #[test]
    fn initial_snapshot_with_recovery_facts_fails_closed() {
        let snapshot = base()
            .with_fact(Fact::string("recovery.attempt_id", "attempt-1"))
            .with_fact(Fact::integer("attempts.semantic", 1))
            .with_fact(Fact::integer("attempts.max_semantic", 3));
        let proposal = RuleDecisionProvider::new()
            .propose(&snapshot)
            .expect("proposal");
        assert_eq!(proposal.decision, DecisionKind::Escalate);
    }

    #[test]
    fn initial_snapshot_reporting_prior_attempts_fails_closed() {
        let snapshot = base()
            .with_fact(Fact::integer("attempts.semantic", 1))
            .with_fact(Fact::integer("attempts.max_semantic", 3));
        let proposal = RuleDecisionProvider::new()
            .propose(&snapshot)
            .expect("proposal");
        assert_eq!(proposal.decision, DecisionKind::Escalate);
    }

    #[test]
    fn recovery_snapshot_is_unchanged() {
        let snapshot = base()
            .with_fact(Fact::string("observation.category", "WORKER_TIMEOUT"))
            .with_fact(Fact::integer("attempts.semantic", 1))
            .with_fact(Fact::integer("attempts.max_semantic", 3));
        let proposal = RuleDecisionProvider::new()
            .propose(&snapshot)
            .expect("proposal");
        assert_eq!(proposal.decision, DecisionKind::Reexecute);
    }
}
