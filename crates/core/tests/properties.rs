//! Property-based tests for the versioned contracts.
//!
//! Example-based tests (`contracts_json.rs`, `contracts_api.rs`) pin documented
//! shapes and error paths. These properties assert invariants over *arbitrary*
//! inputs, which a hand-written example can miss.

use ictus_core::{
    Capability, DecisionKind, DecisionProposal, EvidenceRef, ExecutionIntent, ExecutionObservation,
    ExecutionResult, ExecutionStatus, Fact, ObservationCategory, PolicyDecision,
    PolicyDecisionKind, ProviderMetadata, RequestedBy, RiskClass, StateSnapshot, Subject,
    DECISION_SCHEMA_VERSION, SCHEMA_VERSION,
};
use proptest::prelude::*;

// -- strategies -------------------------------------------------------------

prop_compose! {
    fn arb_subject()(kind in "[a-z][a-z0-9_.]{0,10}", id in "[a-z0-9-]{1,12}") -> Subject {
        Subject::new(kind, id)
    }
}

prop_compose! {
    fn arb_evidence()(kind in "[a-z]{1,8}", uri in "[a-z0-9:/._-]{1,24}") -> EvidenceRef {
        EvidenceRef::new(kind, uri)
    }
}

prop_compose! {
    fn arb_fact()(key in "[a-z][a-z0-9_.]{0,10}", value in any::<i64>()) -> Fact {
        Fact::integer(key, value)
    }
}

prop_compose! {
    fn arb_snapshot()
        (subject in arb_subject(),
         facts in prop::collection::vec(arb_fact(), 0..4),
         domain in "[a-z][a-z0-9_]{0,8}")
        -> StateSnapshot
    {
        let mut snapshot = StateSnapshot::new("snap", "2026-10-01T00:00:00Z", domain, subject);
        for fact in facts {
            snapshot = snapshot.with_fact(fact);
        }
        snapshot
    }
}

prop_compose! {
    fn arb_provider()(id in "[a-z][a-z0-9_.]{0,8}") -> ProviderMetadata {
        ProviderMetadata::rules(id)
    }
}

prop_compose! {
    fn arb_observation()(message in any::<Option<String>>()) -> ExecutionObservation {
        let mut observation = ExecutionObservation::new(
            "obs-1",
            "exec-1",
            "intent-1",
            ObservationCategory::WorkerTimeout,
            "2026-10-01T00:00:00Z",
        );
        observation.message = message;
        observation
    }
}

prop_compose! {
    fn arb_intent()(capability in "[a-z][a-z0-9_.]{0,14}", subject in arb_subject()) -> ExecutionIntent {
        ExecutionIntent::new("intent-1", capability, subject, RequestedBy::new("rules", "proposal-1"))
    }
}

prop_compose! {
    fn arb_result()(evidence in prop::collection::vec(arb_evidence(), 0..3)) -> ExecutionResult {
        let mut result = ExecutionResult::new(
            "exec-1",
            "intent-1",
            ExecutionStatus::Failed,
            ObservationCategory::ProcessCrash,
        );
        for item in evidence {
            result = result.with_evidence(item);
        }
        result
    }
}

prop_compose! {
    fn arb_capability()(id in "[a-z][a-z0-9_.]{0,12}", version in "[0-9]{1,3}") -> Capability {
        Capability::new(id, version, RiskClass::Low)
    }
}

// -- versioning -------------------------------------------------------------

proptest! {
    #[test]
    fn every_contract_carries_the_current_schema_version(
        snapshot in arb_snapshot(),
        observation in arb_observation(),
        intent in arb_intent(),
        result in arb_result(),
        proposal in any::<bool>(),
    ) {
        for value in [
            serde_json::to_value(&snapshot).unwrap(),
            serde_json::to_value(&observation).unwrap(),
            serde_json::to_value(&intent).unwrap(),
            serde_json::to_value(&result).unwrap(),
        ] {
            prop_assert_eq!(value["schema_version"].as_u64(), Some(SCHEMA_VERSION as u64));
        }
        // The DecisionProposal contract is versioned independently and is at v2.
        let proposal = serde_json::to_value(make_proposal(proposal)).unwrap();
        prop_assert_eq!(
            proposal["schema_version"].as_u64(),
            Some(DECISION_SCHEMA_VERSION as u64)
        );
    }
}

// -- round trips ------------------------------------------------------------

proptest! {
    #[test]
    fn state_snapshot_round_trips(snapshot in arb_snapshot()) {
        let json = serde_json::to_string(&snapshot).unwrap();
        let decoded: StateSnapshot = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(&snapshot, &decoded);
    }

    #[test]
    fn execution_observation_round_trips(observation in arb_observation()) {
        let json = serde_json::to_string(&observation).unwrap();
        let decoded: ExecutionObservation = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(&observation, &decoded);
    }

    #[test]
    fn execution_intent_round_trips(intent in arb_intent()) {
        let json = serde_json::to_string(&intent).unwrap();
        let decoded: ExecutionIntent = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(&intent, &decoded);
    }

    #[test]
    fn execution_result_round_trips(result in arb_result()) {
        let json = serde_json::to_string(&result).unwrap();
        let decoded: ExecutionResult = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(&result, &decoded);
    }
}

// -- invariants -------------------------------------------------------------

proptest! {
    #[test]
    fn a_valid_snapshot_is_accepted(snapshot in arb_snapshot()) {
        prop_assert!(snapshot.validate().is_ok());
    }

    #[test]
    fn an_unknown_schema_version_is_always_rejected(mut intent in arb_intent(), version in 2u32..1000) {
        intent.schema_version = version;
        prop_assert!(intent.validate().is_err());
    }

    #[test]
    fn a_deny_decision_never_carries_an_intent(intent in arb_intent()) {
        let mut decision =
            PolicyDecision::new("pd", "p", PolicyDecisionKind::Deny, "t");
        decision.modified_intent = Some(intent);
        prop_assert!(decision.validate().is_err());
        prop_assert!(!decision.is_executable());
    }

    #[test]
    fn an_allow_decision_with_an_intent_is_executable(intent in arb_intent()) {
        let mut decision =
            PolicyDecision::new("pd", "p", PolicyDecisionKind::Allow, "t");
        decision.modified_intent = Some(intent);
        prop_assert!(decision.validate().is_ok());
        prop_assert!(decision.is_executable());
    }

    #[test]
    fn a_capability_requiring_decision_without_a_capability_is_rejected(
        subject in arb_subject(),
        provider in arb_provider(),
    ) {
        let proposal =
            DecisionProposal::new("p", DecisionKind::ExecuteCapability, subject, provider, "t");
        prop_assert!(proposal.validate().is_err());

        let retry = DecisionProposal::new("p", DecisionKind::Reexecute, proposal.subject.clone(), proposal.provider.clone(), "t");
        prop_assert!(retry.validate().is_err());
    }

    #[test]
    fn every_capability_requiring_decision_requires_a_capability(
        subject in arb_subject(),
        provider in arb_provider(),
    ) {
        for decision in [DecisionKind::Reexecute, DecisionKind::Route] {
            let proposal = DecisionProposal::new("p", decision, subject.clone(), provider.clone(), "t");
            prop_assert!(proposal.validate().is_err());
        }
    }

    #[test]
    fn a_blank_capability_id_is_rejected(
        subject in arb_subject(),
        provider in arb_provider(),
        blanks in "[ \t]{1,4}",
    ) {
        let proposal = DecisionProposal::new("p", DecisionKind::ExecuteCapability, subject, provider, "t")
            .with_capability(blanks);
        prop_assert!(proposal.validate().is_err());
    }

    #[test]
    fn a_result_maps_to_an_observation_with_the_same_ids(result in arb_result()) {
        let observation = result.to_observation("obs", "2026-10-01T00:00:02Z");
        prop_assert_eq!(&observation.execution_id, &result.execution_id);
        prop_assert_eq!(&observation.intent_id, &result.intent_id);
        prop_assert_eq!(observation.category, result.observation.category);
        prop_assert!(observation.validate().is_ok());
    }

    #[test]
    fn a_valid_capability_is_accepted(capability in arb_capability()) {
        prop_assert!(capability.validate().is_ok());
    }

    #[test]
    fn evidence_refs_round_trip(evidence in arb_evidence()) {
        let json = serde_json::to_string(&evidence).unwrap();
        let decoded: EvidenceRef = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(&evidence, &decoded);
    }
}

fn make_proposal(as_abort: bool) -> DecisionProposal {
    let decision = if as_abort {
        DecisionKind::Abort
    } else {
        DecisionKind::Escalate
    };
    DecisionProposal::new(
        "p",
        decision,
        Subject::new("task", "t1"),
        ProviderMetadata::rules("rules"),
        "2026-10-01T00:00:00Z",
    )
}
