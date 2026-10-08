//! Deterministic default policy evaluator.
//!
//! The evaluator is the trust boundary. It decides `ALLOW`, `DENY`,
//! `REQUIRE_APPROVAL` or `MODIFY`. It never executes anything and never
//! consults a model.
//!
//! Determinism: the decision timestamp is derived from the proposal's
//! `proposed_at`, so the same inputs always produce the same decision record.

use ictus_core::{
    Capability, DecisionKind, DecisionProposal, PolicyDecision, PolicyDecisionKind, RiskClass,
    StateSnapshot,
};
use ictus_ports::{ApprovalProvider, CapabilityRegistry, PolicyEvaluator, PortError};

use crate::{intent::build_execution_intent, validation::validate_capability};

/// The approval tokens a capability requires.
///
/// A high-risk capability with no named approvals implicitly requires a human
/// gate, so it can never be auto-approved by an empty provider.
fn required_approvals(capability: &Capability) -> Vec<String> {
    if !capability.required_approvals.is_empty() {
        capability.required_approvals.clone()
    } else if capability.risk_class == RiskClass::High {
        vec!["human".to_string()]
    } else {
        Vec::new()
    }
}

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
        if snapshot.schema_version != ictus_core::SCHEMA_VERSION {
            return Ok(decision.with_reason("unsupported snapshot schema_version"));
        }

        // 2. Decisions that carry no execution.
        match proposal.decision {
            DecisionKind::Abort | DecisionKind::Escalate | DecisionKind::Decompose => {
                decision.decision = PolicyDecisionKind::Allow;
                return Ok(decision.with_reason(format!(
                    "{:?} is always permitted and never reaches the execution backend",
                    proposal.decision
                )));
            }
            DecisionKind::Reexecute | DecisionKind::ExecuteCapability | DecisionKind::Route => {}
        }

        // This legacy evaluator has no route facts or compatibility policy in
        // its port signature. It must fail closed rather than forwarding route
        // hints to an adapter. Call `select_route` and
        // `LocalPolicyComposition::validate_with_route_selection` instead.
        if proposal.decision == DecisionKind::Route {
            return Ok(decision.with_reason(
                "ROUTE requires a selected route from observed facts; constraints alone are not executable",
            ));
        }

        // 3. A capability-carrying decision must reference a known capability.
        // `proposal.validate()` above already guarantees a non-empty capability
        // for capability-requiring decisions, so there is no separate blank
        // check to keep in sync here.
        let capability_id = match proposal.capability.as_deref() {
            Some(id) => id,
            None => {
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

        // 4. The semantic re-execution budget is enforced here, not in the
        // execution backend. This is distinct from Dagster's own step retries.
        if proposal.decision == DecisionKind::Reexecute {
            let attempt = snapshot.fact_i64("retry.attempt").unwrap_or(0);
            let budget = snapshot.fact_i64("retry.budget").unwrap_or(0);
            if attempt >= budget {
                return Ok(decision.with_reason(format!(
                    "re-execution budget exhausted ({attempt}/{budget})"
                )));
            }
        }

        // 5. Approval gate. The required set comes from the capability (not the
        // proposal), so an empty provider can never silently approve a gated
        // capability.
        let required = required_approvals(&capability);
        let intent = build_execution_intent(snapshot, proposal)?;
        if !required.is_empty() && !approvals.is_approved(&required) {
            decision.decision = PolicyDecisionKind::RequireApproval;
            decision = decision.with_intent(intent);
            for approval in &required {
                decision = decision.with_reason(format!("requires approval: {approval}"));
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
    use ictus_core::{ProviderMetadata, Subject};

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
        InMemoryCapabilityRegistry::new().with(ictus_core::Capability::new(
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
        let registry = InMemoryCapabilityRegistry::new().with(ictus_core::Capability::new(
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
    fn reexecution_budget_is_enforced() {
        let evaluator = DefaultPolicyEvaluator::new();
        let snapshot = snapshot()
            .with_fact(ictus_core::Fact::integer("retry.attempt", 2))
            .with_fact(ictus_core::Fact::integer("retry.budget", 2));
        let decision = evaluator
            .evaluate(
                &snapshot,
                &proposal(DecisionKind::Reexecute, Some("demo.verify")),
                &registry(),
                &crate::approval::AlwaysApproved,
            )
            .unwrap();
        assert_eq!(decision.decision, PolicyDecisionKind::Deny);
    }

    #[test]
    fn decompose_is_allowed_and_produces_no_intent() {
        let evaluator = DefaultPolicyEvaluator::new();
        let decision = evaluator
            .evaluate(
                &snapshot(),
                &proposal(DecisionKind::Decompose, None),
                &registry(),
                &crate::approval::NeverApproved,
            )
            .unwrap();
        assert_eq!(decision.decision, PolicyDecisionKind::Allow);
        assert!(!decision.is_executable());
        assert!(decision.modified_intent.is_none());
    }

    #[test]
    fn route_without_observed_selection_is_denied() {
        use ictus_core::RouteConstraints;
        let evaluator = DefaultPolicyEvaluator::new();
        let proposal = proposal(DecisionKind::Route, Some("demo.verify")).with_route(
            RouteConstraints::new()
                .with_exclude_backend("backend.a")
                .with_preferred_backend("backend.b")
                .with_required_provider("provider.c")
                .with_required_runtime("wasm"),
        );
        let decision = evaluator
            .evaluate(
                &snapshot(),
                &proposal,
                &registry(),
                &crate::approval::AlwaysApproved,
            )
            .unwrap();
        assert_eq!(decision.decision, PolicyDecisionKind::Deny);
        assert!(decision.modified_intent.is_none());
    }

    #[test]
    fn route_without_a_capability_is_denied() {
        let evaluator = DefaultPolicyEvaluator::new();
        let decision = evaluator
            .evaluate(
                &snapshot(),
                &proposal(DecisionKind::Route, None),
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

    #[test]
    fn high_risk_without_named_approvals_cannot_be_auto_approved() {
        use crate::approval::TokenApprovalProvider;
        let evaluator = DefaultPolicyEvaluator::new();
        let registry = InMemoryCapabilityRegistry::new().with(agentic_capability(
            "software.promote",
            ictus_core::RiskClass::High,
            &[],
        ));

        // An empty token provider must NOT approve a high-risk capability.
        let pending = evaluator
            .evaluate(
                &snapshot(),
                &proposal(DecisionKind::ExecuteCapability, Some("software.promote")),
                &registry,
                &TokenApprovalProvider::new(vec![]),
            )
            .unwrap();
        assert_eq!(pending.decision, PolicyDecisionKind::RequireApproval);
        assert!(pending
            .reasons
            .contains(&"requires approval: human".to_string()));

        // A human token satisfies the implicit gate.
        let allowed = evaluator
            .evaluate(
                &snapshot(),
                &proposal(DecisionKind::ExecuteCapability, Some("software.promote")),
                &registry,
                &TokenApprovalProvider::new(vec![]).with("human"),
            )
            .unwrap();
        assert_eq!(allowed.decision, PolicyDecisionKind::Allow);
    }

    #[test]
    fn named_approvals_are_enforced() {
        use crate::approval::TokenApprovalProvider;
        let evaluator = DefaultPolicyEvaluator::new();
        let registry = InMemoryCapabilityRegistry::new().with(agentic_capability(
            "secure.op",
            ictus_core::RiskClass::Medium,
            &["security"],
        ));

        let wrong = evaluator
            .evaluate(
                &snapshot(),
                &proposal(DecisionKind::ExecuteCapability, Some("secure.op")),
                &registry,
                &TokenApprovalProvider::new(vec![]).with("human"),
            )
            .unwrap();
        assert_eq!(wrong.decision, PolicyDecisionKind::RequireApproval);

        let right = evaluator
            .evaluate(
                &snapshot(),
                &proposal(DecisionKind::ExecuteCapability, Some("secure.op")),
                &registry,
                &TokenApprovalProvider::new(vec![]).with("security"),
            )
            .unwrap();
        assert_eq!(right.decision, PolicyDecisionKind::Allow);
    }

    fn agentic_capability(
        id: &str,
        risk: ictus_core::RiskClass,
        approvals: &[&str],
    ) -> ictus_core::Capability {
        let mut capability = ictus_core::Capability::new(id, "1", risk);
        for approval in approvals {
            capability = capability.with_required_approval(*approval);
        }
        capability
    }
}
