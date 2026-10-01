//! Deterministic default policy evaluator.
//!
//! The evaluator is the trust boundary. It decides `ALLOW`, `DENY`,
//! `REQUIRE_APPROVAL` or `MODIFY`. It never executes anything and never
//! consults a model.
//!
//! Determinism: the decision timestamp is derived from the proposal's
//! `proposed_at`, so the same inputs always produce the same decision record.

use agentic_core::{
    DecisionKind, DecisionProposal, PolicyDecision, PolicyDecisionKind, RiskClass, StateSnapshot,
};
use agentic_ports::{ApprovalProvider, CapabilityRegistry, PolicyEvaluator, PortError};

use crate::{intent::build_execution_intent, validation::validate_capability};

/// The default deterministic policy.
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultPolicyEvaluator;

impl DefaultPolicyEvaluator {
    pub fn new() -> Self {
        Self
    }
}

impl PolicyEvaluator for DefaultPolicyEvaluator {
    fn evaluate(
        &self,
        snapshot: &StateSnapshot,
        proposal: &DecisionProposal,
        capabilities: &dyn CapabilityRegistry,
        approvals: &dyn ApprovalProvider,
    ) -> Result<PolicyDecision, PortError> {
        let decided_at = proposal.proposed_at.clone();
        let mut decision = PolicyDecision::new(
            format!("policy:{}", proposal.proposal_id),
            proposal.proposal_id.clone(),
            PolicyDecisionKind::Deny,
            decided_at,
        );

        // 1. Structural validity is a hard gate.
        if let Err(error) = proposal.validate() {
            return Ok(decision.with_reason(format!("invalid proposal: {error}")));
        }
        if snapshot.schema_version != agentic_core::SCHEMA_VERSION {
            return Ok(decision.with_reason("unsupported snapshot schema_version"));
        }

        // 2. Decisions that carry no execution.
        match proposal.decision {
            DecisionKind::Abort | DecisionKind::Escalate => {
                decision.decision = PolicyDecisionKind::Allow;
                return Ok(decision.with_reason(format!(
                    "{:?} is always permitted and never reaches the execution backend",
                    proposal.decision
                )));
            }
            DecisionKind::Retry | DecisionKind::ExecuteCapability => {}
        }

        // 3. A capability-carrying decision must reference a known capability.
        let capability_id = match proposal.capability.as_deref() {
            Some(id) if !id.trim().is_empty() => id,
            _ => {
                return Ok(decision.with_reason("decision requires a non-empty capability id"));
            }
        };
        let capability = match capabilities.get(capability_id) {
            Some(capability) => capability,
            None => {
                return Ok(
                    decision.with_reason(format!("capability '{capability_id}' is not registered"))
                );
            }
        };
        if let Err(error) = validate_capability(&capability) {
            return Ok(decision.with_reason(format!("invalid capability: {error}")));
        }

        // 4. Retry budget is enforced here, not in the execution backend.
        if proposal.decision == DecisionKind::Retry {
            let attempt = snapshot.fact_i64("retry.attempt").unwrap_or(0);
            let budget = snapshot.fact_i64("retry.budget").unwrap_or(0);
            if attempt >= budget {
                return Ok(
                    decision.with_reason(format!("retry budget exhausted ({attempt}/{budget})"))
                );
            }
        }

        // 5. Approval gate.
        let requires_approval =
            capability.risk_class == RiskClass::High || !capability.required_approvals.is_empty();
        let intent = build_execution_intent(snapshot, proposal)?;
        if requires_approval && !approvals.is_approved(proposal) {
            decision.decision = PolicyDecisionKind::RequireApproval;
            decision = decision.with_intent(intent);
            for approval in &capability.required_approvals {
                decision = decision.with_reason(format!("requires approval: {approval}"));
            }
            if capability.risk_class == RiskClass::High {
                decision = decision.with_reason("requires approval: high risk class");
            }
            return Ok(decision);
        }

        decision.decision = PolicyDecisionKind::Allow;
        decision = decision.with_intent(intent);
        decision = decision.with_reason(format!(
            "capability '{capability_id}' v{capability_version} admitted",
            capability_version = capability.version
        ));
        Ok(decision)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::InMemoryCapabilityRegistry;
    use agentic_core::{ProviderMetadata, Subject};

    fn snapshot() -> StateSnapshot {
        StateSnapshot::new(
            "s1",
            "2026-10-01T00:00:00Z",
            "software",
            Subject::new("task", "t1"),
        )
    }

    fn proposal(decision: DecisionKind, capability: Option<&str>) -> DecisionProposal {
        let mut proposal = DecisionProposal::new(
            "p1",
            decision,
            Subject::new("task", "t1"),
            ProviderMetadata::rules("rules"),
            "2026-10-01T00:00:00Z",
        );
        proposal.capability = capability.map(str::to_string);
        proposal
    }

    fn registry() -> InMemoryCapabilityRegistry {
        InMemoryCapabilityRegistry::new().with(agentic_core::Capability::new(
            "demo.verify",
            "1",
            RiskClass::Low,
        ))
    }

    #[test]
    fn known_low_risk_capability_is_allowed() {
        let evaluator = DefaultPolicyEvaluator::new();
        let decision = evaluator
            .evaluate(
                &snapshot(),
                &proposal(DecisionKind::ExecuteCapability, Some("demo.verify")),
                &registry(),
                &crate::approval::NeverApproved,
            )
            .unwrap();
        assert_eq!(decision.decision, PolicyDecisionKind::Allow);
        assert!(decision.is_executable());
        assert_eq!(decision.modified_intent.unwrap().capability, "demo.verify");
    }

    #[test]
    fn unknown_capability_is_denied() {
        let evaluator = DefaultPolicyEvaluator::new();
        let decision = evaluator
            .evaluate(
                &snapshot(),
                &proposal(DecisionKind::ExecuteCapability, Some("missing")),
                &registry(),
                &crate::approval::AlwaysApproved,
            )
            .unwrap();
        assert_eq!(decision.decision, PolicyDecisionKind::Deny);
        assert!(!decision.is_executable());
        assert!(decision.modified_intent.is_none());
    }

    #[test]
    fn high_risk_capability_requires_approval_then_allows() {
        let evaluator = DefaultPolicyEvaluator::new();
        let registry = InMemoryCapabilityRegistry::new().with(agentic_core::Capability::new(
            "software.promote",
            "1",
            RiskClass::High,
        ));
        let denied = evaluator
            .evaluate(
                &snapshot(),
                &proposal(DecisionKind::ExecuteCapability, Some("software.promote")),
                &registry,
                &crate::approval::NeverApproved,
            )
            .unwrap();
        assert_eq!(denied.decision, PolicyDecisionKind::RequireApproval);
        assert!(!denied.is_executable());

        let allowed = evaluator
            .evaluate(
                &snapshot(),
                &proposal(DecisionKind::ExecuteCapability, Some("software.promote")),
                &registry,
                &crate::approval::AlwaysApproved,
            )
            .unwrap();
        assert_eq!(allowed.decision, PolicyDecisionKind::Allow);
        assert!(allowed.is_executable());
    }

    #[test]
    fn retry_budget_is_enforced() {
        let evaluator = DefaultPolicyEvaluator::new();
        let snapshot = snapshot()
            .with_fact(agentic_core::Fact::integer("retry.attempt", 2))
            .with_fact(agentic_core::Fact::integer("retry.budget", 2));
        let decision = evaluator
            .evaluate(
                &snapshot,
                &proposal(DecisionKind::Retry, Some("demo.verify")),
                &registry(),
                &crate::approval::AlwaysApproved,
            )
            .unwrap();
        assert_eq!(decision.decision, PolicyDecisionKind::Deny);
    }

    #[test]
    fn abort_is_allowed_without_a_capability() {
        let evaluator = DefaultPolicyEvaluator::new();
        let decision = evaluator
            .evaluate(
                &snapshot(),
                &proposal(DecisionKind::Abort, None),
                &registry(),
                &crate::approval::NeverApproved,
            )
            .unwrap();
        assert_eq!(decision.decision, PolicyDecisionKind::Allow);
        assert!(!decision.is_executable());
    }
}
