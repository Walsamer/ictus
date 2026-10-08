//! Route-selection coverage: hard compatibility gates, eligibility, and the
//! deterministic ordering that makes route choice independent of input order.

use ictus_core::{
    ApprovalGrant, AttemptBudget, Capability, DecisionContext, DecisionKind, DecisionProposal,
    EvidenceRef, ObservationState, ProviderMetadata, RiskClass, RouteCandidate, RouteConstraints,
    RouteDescriptor, RouteExclusion, RouteFactRef, RouteFacts, RouteIdentity, RouteObservation,
    RouteRequirements, SelectedRoute, SnapshotRef, StateSnapshot, Subject,
};
use ictus_policy::intent::{build_execution_intent, build_routed_execution_intent};
use ictus_policy::routing::{
    select_route, NoRouteReason, RouteSelection, RoutingPolicy, UncertainFactPolicy,
};
use ictus_policy::{
    InMemoryCapabilityRegistry, LocalPolicyComposition, ValidationRequest, DEFAULT_POLICY_VERSION,
};

const CAP: &str = "capability";

fn candidate(
    backend: &str,
    provider: &str,
    runtime: &str,
    model: &str,
    state: ObservationState,
    capability: &str,
    enabled: bool,
) -> RouteCandidate {
    let route = RouteIdentity::new(backend, provider, runtime, model);
    let mut descriptor = RouteDescriptor::new(
        RouteFactRef::new(format!("{backend}.descriptor"), 1),
        route.clone(),
    )
    .with_capability(capability);
    descriptor.enabled = enabled;
    RouteFacts {
        descriptor,
        health: RouteObservation::new(
            RouteFactRef::new(format!("{backend}.health"), 2),
            route.clone(),
            state,
            "2026-10-01T00:00:00Z",
        ),
        quota: RouteObservation::new(
            RouteFactRef::new(format!("{backend}.quota"), 3),
            route.clone(),
            state,
            "2026-10-01T00:00:00Z",
        ),
        disablement: RouteObservation::new(
            RouteFactRef::new(format!("{backend}.disablement"), 4),
            route.clone(),
            state,
            "2026-10-01T00:00:00Z",
        ),
    }
    .try_into()
    .unwrap()
}

fn simple(backend: &str, state: ObservationState) -> RouteCandidate {
    candidate(backend, "provider", "runtime", "model", state, CAP, true)
}

fn requirements() -> RouteRequirements {
    RouteRequirements::for_capability(CAP)
}

fn no_route_reason(selection: &RouteSelection) -> NoRouteReason {
    match selection {
        RouteSelection::NoRoute(no_route) => no_route.reason,
        other => panic!("expected no-route, got {other:?}"),
    }
}

fn selected_backend(selection: &RouteSelection) -> String {
    match selection {
        RouteSelection::Selected(selected) => selected.route.backend.clone(),
        other => panic!("expected selected, got {other:?}"),
    }
}

fn snapshot() -> StateSnapshot {
    StateSnapshot::new(
        "s1",
        "2026-10-01T00:00:00Z",
        "software",
        Subject::new("task", "t1"),
    )
}

fn route_proposal() -> DecisionProposal {
    DecisionProposal::new(
        "p1",
        DecisionKind::Route,
        Subject::new("task", "t1"),
        ProviderMetadata::rules("rules"),
        "2026-10-01T00:00:00Z",
    )
    .with_capability("demo.verify")
    .with_route(RouteConstraints::new().with_preferred_backend("backend-a"))
}

#[test]
fn empty_candidate_input_yields_no_candidates() {
    let selection = select_route(&requirements(), &[], &RoutingPolicy::default(), &[]);
    assert_eq!(no_route_reason(&selection), NoRouteReason::NoCandidates);
}

#[test]
fn invalid_requirements_or_candidates_fail_closed() {
    // Invalid requirement (empty capability) is rejected before any selection.
    let bad_requirements = RouteRequirements {
        capability: String::new(),
        ..RouteRequirements::default()
    };
    let selection = select_route(
        &bad_requirements,
        &[simple("a", ObservationState::Available)],
        &RoutingPolicy::default(),
        &[],
    );
    assert_eq!(no_route_reason(&selection), NoRouteReason::InvalidInput);

    // Valid requirements but a malformed candidate is rejected as a whole.
    let mut bad_candidate = simple("a", ObservationState::Available);
    bad_candidate.health.fact.fact_id = String::new();
    let selection = select_route(
        &requirements(),
        &[bad_candidate],
        &RoutingPolicy::default(),
        &[],
    );
    assert_eq!(no_route_reason(&selection), NoRouteReason::InvalidInput);
}

#[test]
fn previously_excluded_candidates_report_all_excluded() {
    let excluded = simple("a", ObservationState::Available);
    let exclusion = RouteExclusion::new(excluded.identity().clone());
    let selection = select_route(
        &requirements(),
        &[excluded],
        &RoutingPolicy::default(),
        &[exclusion],
    );
    assert_eq!(
        no_route_reason(&selection),
        NoRouteReason::AllCandidatesExcluded
    );
}

#[test]
fn each_forbidden_dimension_is_rejected_independently() {
    let candidate = candidate(
        "backend-a",
        "provider-a",
        "runtime-a",
        "model-a",
        ObservationState::Available,
        CAP,
        true,
    );
    for policy in [
        RoutingPolicy {
            forbidden_backends: vec!["backend-a".to_string()],
            ..RoutingPolicy::default()
        },
        RoutingPolicy {
            forbidden_providers: vec!["provider-a".to_string()],
            ..RoutingPolicy::default()
        },
        RoutingPolicy {
            forbidden_runtimes: vec!["runtime-a".to_string()],
            ..RoutingPolicy::default()
        },
        RoutingPolicy {
            forbidden_models: vec!["model-a".to_string()],
            ..RoutingPolicy::default()
        },
    ] {
        let selection = select_route(
            &requirements(),
            std::slice::from_ref(&candidate),
            &policy,
            &[],
        );
        assert_eq!(
            no_route_reason(&selection),
            NoRouteReason::AllCandidatesForbidden
        );
    }
}

#[test]
fn compatibility_gates_exclude_each_constraint_violation() {
    // Declared capability is required.
    let wrong_capability = candidate(
        "a",
        "provider",
        "runtime",
        "model",
        ObservationState::Available,
        "other",
        true,
    );
    assert_eq!(
        no_route_reason(&select_route(
            &requirements(),
            &[wrong_capability],
            &RoutingPolicy::default(),
            &[]
        )),
        NoRouteReason::NoCompatibleCandidate
    );

    // A disabled descriptor is not compatible.
    let disabled = candidate(
        "a",
        "provider",
        "runtime",
        "model",
        ObservationState::Available,
        CAP,
        false,
    );
    assert_eq!(
        no_route_reason(&select_route(
            &requirements(),
            &[disabled],
            &RoutingPolicy::default(),
            &[]
        )),
        NoRouteReason::NoCompatibleCandidate
    );

    let compatible = candidate(
        "a",
        "provider",
        "runtime",
        "model",
        ObservationState::Available,
        CAP,
        true,
    );

    let mut exclude = requirements();
    exclude.exclude_backend = Some("a".to_string());
    assert_eq!(
        no_route_reason(&select_route(
            &exclude,
            std::slice::from_ref(&compatible),
            &RoutingPolicy::default(),
            &[]
        )),
        NoRouteReason::NoCompatibleCandidate
    );

    let mut provider = requirements();
    provider.required_provider = Some("other".to_string());
    assert_eq!(
        no_route_reason(&select_route(
            &provider,
            std::slice::from_ref(&compatible),
            &RoutingPolicy::default(),
            &[]
        )),
        NoRouteReason::NoCompatibleCandidate
    );

    let mut runtime = requirements();
    runtime.required_runtime = Some("other".to_string());
    assert_eq!(
        no_route_reason(&select_route(
            &runtime,
            std::slice::from_ref(&compatible),
            &RoutingPolicy::default(),
            &[]
        )),
        NoRouteReason::NoCompatibleCandidate
    );

    // Matching required facts remain selectable.
    let mut matching = requirements();
    matching.required_provider = Some("provider".to_string());
    matching.required_runtime = Some("runtime".to_string());
    assert!(matches!(
        select_route(&matching, &[compatible], &RoutingPolicy::default(), &[]),
        RouteSelection::Selected(_)
    ));
}

#[test]
fn mixed_health_quota_and_disablement_states_fail_closed() {
    for field in 0..3 {
        let mut candidate = simple("a", ObservationState::Available);
        match field {
            0 => candidate.health.state = ObservationState::Unavailable,
            1 => candidate.quota.state = ObservationState::Unknown,
            _ => candidate.disablement.state = ObservationState::Stale,
        }
        assert_eq!(
            no_route_reason(&select_route(
                &requirements(),
                &[candidate],
                &RoutingPolicy::default(),
                &[]
            )),
            NoRouteReason::AllCandidatesUnavailable
        );
    }
}

#[test]
fn unknown_and_stale_facts_follow_the_configured_policy() {
    for state in [ObservationState::Unknown, ObservationState::Stale] {
        let rejected = select_route(
            &requirements(),
            &[simple("a", state)],
            &RoutingPolicy::default(),
            &[],
        );
        assert_eq!(
            no_route_reason(&rejected),
            NoRouteReason::AllCandidatesUnavailable
        );

        let policy = match state {
            ObservationState::Unknown => RoutingPolicy {
                unknown_observation: UncertainFactPolicy::RequireApproval,
                ..RoutingPolicy::default()
            },
            _ => RoutingPolicy {
                stale_observation: UncertainFactPolicy::RequireApproval,
                ..RoutingPolicy::default()
            },
        };
        match select_route(&requirements(), &[simple("a", state)], &policy, &[]) {
            RouteSelection::RequireApproval(approval) => {
                assert_eq!(approval.facts.len(), 3);
            }
            other => panic!("expected approval for {state:?}, got {other:?}"),
        }
    }
}

#[test]
fn approval_facts_are_sorted_and_deduplicated() {
    let policy = RoutingPolicy {
        unknown_observation: UncertainFactPolicy::RequireApproval,
        ..RoutingPolicy::default()
    };
    // Two candidates with identical fact identities: the approval must not
    // name the same fact twice.
    let first = simple("a", ObservationState::Unknown);
    let duplicate = simple("a", ObservationState::Unknown);
    match select_route(&requirements(), &[first, duplicate], &policy, &[]) {
        RouteSelection::RequireApproval(approval) => {
            assert_eq!(approval.facts.len(), 3);
            let mut sorted = approval.facts.clone();
            sorted.sort_by(|left, right| {
                (left.fact_id.as_str(), left.revision)
                    .cmp(&(right.fact_id.as_str(), right.revision))
            });
            assert_eq!(approval.facts, sorted);
        }
        other => panic!("expected approval, got {other:?}"),
    }
}

#[test]
fn requested_preference_beats_lexical_order() {
    let mut requirements = requirements();
    requirements.preferred_backend = Some("z".to_string());
    let selection = select_route(
        &requirements,
        &[
            simple("a", ObservationState::Available),
            simple("z", ObservationState::Available),
        ],
        &RoutingPolicy::default(),
        &[],
    );
    assert_eq!(selected_backend(&selection), "z");
}

#[test]
fn policy_preference_order_beats_lexical_order() {
    let policy = RoutingPolicy {
        preferred_backends: vec!["z".to_string(), "a".to_string()],
        ..RoutingPolicy::default()
    };
    let selection = select_route(
        &requirements(),
        &[
            simple("a", ObservationState::Available),
            simple("z", ObservationState::Available),
        ],
        &policy,
        &[],
    );
    assert_eq!(selected_backend(&selection), "z");
}

#[test]
fn lexical_route_identity_resolves_remaining_ties() {
    // Same backend, so preference cannot decide: provider/runtime/model order.
    let winner = candidate(
        "b",
        "a-provider",
        "a-runtime",
        "a-model",
        ObservationState::Available,
        CAP,
        true,
    );
    let loser = candidate(
        "b",
        "z-provider",
        "z-runtime",
        "z-model",
        ObservationState::Available,
        CAP,
        true,
    );
    let selection = select_route(
        &requirements(),
        &[loser, winner],
        &RoutingPolicy::default(),
        &[],
    );
    match selection {
        RouteSelection::Selected(selected) => {
            assert_eq!(selected.route.provider, "a-provider");
            assert_eq!(selected.route.runtime, "a-runtime");
            assert_eq!(selected.route.model, "a-model");
        }
        other => panic!("expected selected, got {other:?}"),
    }
}

#[test]
fn routed_intent_requires_a_route_decision_and_a_selected_route() {
    // A bare ROUTE proposal cannot produce an intent without a selected route.
    assert!(build_execution_intent(&snapshot(), &route_proposal()).is_err());

    // A selected route is only valid for a ROUTE decision.
    let executable = DecisionProposal::new(
        "p1",
        DecisionKind::ExecuteCapability,
        Subject::new("task", "t1"),
        ProviderMetadata::rules("rules"),
        "2026-10-01T00:00:00Z",
    )
    .with_capability("demo.verify");
    let selected = SelectedRoute::from_candidate(&simple("backend-a", ObservationState::Available));
    assert!(build_routed_execution_intent(&snapshot(), &executable, selected.clone()).is_err());

    // The routed form attaches the typed, revision-bound route.
    let intent = build_routed_execution_intent(&snapshot(), &route_proposal(), selected).unwrap();
    assert!(intent.selected_route.is_some());
    assert!(intent.policy_context.contains_key("route"));
}

// -- composition boundary --------------------------------------------------

fn validation_context() -> DecisionContext {
    DecisionContext::initial(
        "ctx-1",
        "tactus.generic.v1",
        StateSnapshot::new(
            "snap-1",
            "2026-10-01T00:00:00Z",
            "software",
            Subject::new("task", "t1"),
        ),
        7,
        AttemptBudget::new(2),
    )
}

fn promote_registry() -> InMemoryCapabilityRegistry {
    InMemoryCapabilityRegistry::new().with(
        Capability::new("software.promote", "3", RiskClass::High)
            .with_required_approval("human.promote"),
    )
}

fn promote_grant() -> ApprovalGrant {
    ApprovalGrant::new(
        "grant-1",
        "human.promote",
        Subject::new("task", "t1"),
        7,
        "software.promote",
        DEFAULT_POLICY_VERSION,
        "2026-09-30T00:00:00Z",
        "2026-10-02T00:00:00Z",
    )
    .with_evidence(EvidenceRef::new(
        "approval_record",
        "tactus://approvals/grant-1",
    ))
}

fn promote_proposal(decision: DecisionKind) -> DecisionProposal {
    DecisionProposal::new(
        "route-proposal",
        decision,
        Subject::new("task", "t1"),
        ProviderMetadata::rules("rules.v1"),
        "2026-10-01T00:00:00Z",
    )
    .with_capability("software.promote")
}

fn promote_candidate(state: ObservationState) -> RouteCandidate {
    candidate(
        "backend.a",
        "provider.a",
        "container",
        "model.a",
        state,
        "software.promote",
        true,
    )
}

#[test]
fn route_selection_delegates_non_route_decisions() {
    let context = validation_context();
    let proposal = promote_proposal(DecisionKind::ExecuteCapability);
    let registry = promote_registry();
    let grants = [promote_grant()];
    let request = || ValidationRequest {
        context: &context,
        snapshot: SnapshotRef::new("snap-1", 7, "digest-1"),
        proposal: &proposal,
        capabilities: &registry,
        grants: &grants,
        expiry: "2026-10-01T01:00:00Z",
    };
    let delegated = LocalPolicyComposition::default()
        .validate_with_route_selection(request(), &[], &RoutingPolicy::default(), &[])
        .unwrap();
    let direct = LocalPolicyComposition::default()
        .validate(request())
        .unwrap();
    assert!(direct.envelope().is_executable());
    assert_eq!(delegated.envelope().outcome, direct.envelope().outcome);
}

#[test]
fn invalid_route_proposal_is_not_adapted_for_selection() {
    let context = validation_context();
    let proposal = promote_proposal(DecisionKind::Route)
        .with_route(RouteConstraints::new().with_preferred_backend(""));
    let registry = promote_registry();
    let grants = [promote_grant()];
    let request = || ValidationRequest {
        context: &context,
        snapshot: SnapshotRef::new("snap-1", 7, "digest-1"),
        proposal: &proposal,
        capabilities: &registry,
        grants: &grants,
        expiry: "2026-10-01T01:00:00Z",
    };
    let result = LocalPolicyComposition::default().validate_with_route_selection(
        request(),
        &[promote_candidate(ObservationState::Available)],
        &RoutingPolicy::default(),
        &[],
    );
    // Regardless of the plain-validation outcome, no route may be selected
    // from the malformed ROUTE proposal.
    if let Ok(decision) = result {
        assert!(decision
            .executable_intent()
            .and_then(|intent| intent.selected_route.as_ref())
            .is_none());
    }
}

#[test]
fn non_executable_route_admission_stops_before_selection() {
    let context = validation_context();
    let proposal = DecisionProposal::new(
        "route-proposal",
        DecisionKind::Route,
        Subject::new("task", "t1"),
        ProviderMetadata::rules("rules.v1"),
        "2026-10-01T00:00:00Z",
    )
    .with_capability("not.registered");
    let registry = promote_registry();
    let grants: [ApprovalGrant; 0] = [];
    let request = || ValidationRequest {
        context: &context,
        snapshot: SnapshotRef::new("snap-1", 7, "digest-1"),
        proposal: &proposal,
        capabilities: &registry,
        grants: &grants,
        expiry: "2026-10-01T01:00:00Z",
    };
    let decision = LocalPolicyComposition::default()
        .validate_with_route_selection(
            request(),
            &[promote_candidate(ObservationState::Available)],
            &RoutingPolicy::default(),
            &[],
        )
        .unwrap();
    assert!(!decision.envelope().is_executable());
}

#[test]
fn selected_route_records_constraints_in_policy_context() {
    let context = validation_context();
    let proposal = promote_proposal(DecisionKind::Route)
        .with_route(RouteConstraints::new().with_preferred_backend("backend.a"));
    let registry = promote_registry();
    let grants = [promote_grant()];
    let request = || ValidationRequest {
        context: &context,
        snapshot: SnapshotRef::new("snap-1", 7, "digest-1"),
        proposal: &proposal,
        capabilities: &registry,
        grants: &grants,
        expiry: "2026-10-01T01:00:00Z",
    };
    let decision = LocalPolicyComposition::default()
        .validate_with_route_selection(
            request(),
            &[promote_candidate(ObservationState::Available)],
            &RoutingPolicy::default(),
            &[],
        )
        .unwrap();
    let intent = decision.executable_intent().unwrap();
    assert_eq!(
        intent.selected_route.as_ref().unwrap().route.backend,
        "backend.a"
    );
    assert!(intent.policy_context.contains_key("route"));
}

#[test]
fn uncertain_route_facts_surface_a_pending_approval_envelope() {
    let context = validation_context();
    let proposal = promote_proposal(DecisionKind::Route)
        .with_route(RouteConstraints::new().with_preferred_backend("backend.a"));
    let registry = promote_registry();
    let grants = [promote_grant()];
    let request = || ValidationRequest {
        context: &context,
        snapshot: SnapshotRef::new("snap-1", 7, "digest-1"),
        proposal: &proposal,
        capabilities: &registry,
        grants: &grants,
        expiry: "2026-10-01T01:00:00Z",
    };
    let policy = RoutingPolicy {
        unknown_observation: UncertainFactPolicy::RequireApproval,
        ..RoutingPolicy::default()
    };
    let decision = LocalPolicyComposition::default()
        .validate_with_route_selection(
            request(),
            &[promote_candidate(ObservationState::Unknown)],
            &policy,
            &[],
        )
        .unwrap();
    assert_eq!(
        decision.envelope().outcome,
        ictus_core::EnvelopeOutcome::Pending
    );
    assert!(!decision.envelope().is_executable());
}
