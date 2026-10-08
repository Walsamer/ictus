//! Execution-intent generation.
//!
//! Only a validated proposal becomes an intent, and only the policy layer calls
//! this. The intent is the sole object allowed to cross into the execution
//! backend.

use ictus_core::{DecisionProposal, ExecutionIntent, RequestedBy, StateSnapshot};
use ictus_ports::PortError;

use crate::validation::validate_proposal;

/// Build a deterministic `ExecutionIntent` from an approved proposal.
///
/// The intent id is derived from the proposal id so the mapping is
/// reproducible and auditable: `intent:<proposal_id>`.
pub fn build_execution_intent(
    snapshot: &StateSnapshot,
    proposal: &DecisionProposal,
) -> Result<ExecutionIntent, PortError> {
    validate_proposal(proposal)?;
    let capability = proposal.capability.clone().ok_or_else(|| {
        PortError::PolicyFailed(format!(
            "decision {:?} cannot produce an execution intent without a capability",
            proposal.decision
        ))
    })?;

    let mut intent = ExecutionIntent::new(
        format!("intent:{}", proposal.proposal_id),
        capability,
        proposal.subject.clone(),
        RequestedBy::new(
            proposal.provider.provider_id.clone(),
            proposal.proposal_id.clone(),
        ),
    );
    intent.arguments = proposal.arguments.clone();
    intent.policy_context.insert(
        "snapshot_id".to_string(),
        serde_json::Value::String(snapshot.snapshot_id.clone()),
    );
    intent.policy_context.insert(
        "domain".to_string(),
        serde_json::Value::String(snapshot.domain.clone()),
    );
    intent.policy_context.insert(
        "schema_version".to_string(),
        serde_json::json!(snapshot.schema_version),
    );
    // A `ROUTE` decision carries its generic constraints to the execution
    // backend as policy context; the core itself performs no routing.
    if let Some(route) = &proposal.route {
        intent.policy_context.insert(
            "route".to_string(),
            serde_json::to_value(route).map_err(|error| {
                PortError::PolicyFailed(format!("cannot serialize route constraints: {error}"))
            })?,
        );
    }

    intent.validate()?;
    Ok(intent)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ictus_core::{DecisionKind, ProviderMetadata, Subject};

    #[test]
    fn intent_id_is_deterministic_and_references_proposal() {
        let snapshot = StateSnapshot::new(
            "s1",
            "2026-10-01T00:00:00Z",
            "software",
            Subject::new("task", "t1"),
        );
        let proposal = DecisionProposal::new(
            "p1",
            DecisionKind::ExecuteCapability,
            Subject::new("task", "t1"),
            ProviderMetadata::rules("rules"),
            "2026-10-01T00:00:00Z",
        )
        .with_capability("demo.verify");
        let intent = build_execution_intent(&snapshot, &proposal).unwrap();
        assert_eq!(intent.intent_id, "intent:p1");
        assert_eq!(intent.capability, "demo.verify");
        assert_eq!(intent.requested_by.decision_id, "p1");
        assert_eq!(intent.target.id, "t1");
    }

    #[test]
    fn missing_capability_is_rejected() {
        let snapshot = StateSnapshot::new(
            "s1",
            "2026-10-01T00:00:00Z",
            "software",
            Subject::new("task", "t1"),
        );
        let proposal = DecisionProposal::new(
            "p1",
            DecisionKind::Abort,
            Subject::new("task", "t1"),
            ProviderMetadata::rules("rules"),
            "2026-10-01T00:00:00Z",
        );
        assert!(build_execution_intent(&snapshot, &proposal).is_err());
    }
}
