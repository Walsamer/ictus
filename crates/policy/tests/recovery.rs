//! Decision tables for the bounded generic semantic recovery provider.
//!
//! Every row asserts a bounded outcome: success bypass, the canonical semantic
//! budget boundaries, ambiguous observation, persistent-backend routing,
//! decomposition plan authorization and terminal abort. The provider is pure:
//! it never loops, schedules, mutates durable state or calls a runtime.

use ictus_core::{
    AttemptBudget, ContextKind, DecisionContext, DecisionKind, ExecutionObservation, Fact,
    ObservationCategory, RecoveryBinding, RouteConstraints, SnapshotRef, StateSnapshot, Subject,
};
use ictus_policy::{
    route_constraints_from_snapshot, semantic_budget_from_snapshot, DecompositionPlan,
    GenericRecoveryProvider, PlannedChild, RecoveryOutcome, RecoveryRequest,
    CONSTRAINT_ABORT_ON_EXHAUSTION, MAX_DECOMPOSITION_CHILDREN, RECOVERY_POLICY_VERSION,
};

fn category_token(category: ObservationCategory) -> String {
    serde_json::to_value(category)
        .unwrap()
        .as_str()
        .unwrap()
        .to_string()
}

fn base_snapshot(category: ObservationCategory, capability: Option<&str>) -> StateSnapshot {
    let mut snapshot = StateSnapshot::new(
        "snap-r",
        "2026-10-01T00:05:00Z",
        "software",
        Subject::new("task", "task-123"),
    )
    .with_fact(Fact::string(
        "observation.category",
        category_token(category),
    ));
    if let Some(capability) = capability {
        snapshot = snapshot.with_fact(Fact::string("capability.id", capability));
    }
    snapshot
}

fn observation(category: ObservationCategory) -> ExecutionObservation {
    ExecutionObservation::new(
        "obs-r",
        "exec-r",
        "intent-r",
        category,
        "2026-10-01T00:00:00Z",
    )
}

fn context_from(
    snapshot: StateSnapshot,
    category: ObservationCategory,
    semantic: u32,
    max: u32,
    step_retries: u32,
) -> DecisionContext {
    DecisionContext::recovery(
        "ctx-r",
        "tactus.generic.v1",
        snapshot,
        2,
        AttemptBudget::with_attempts(semantic, max, step_retries),
        RecoveryBinding::new(
            "attempt-r",
            "intent-r",
            SnapshotRef::new("snap-r0", 1, "digest-r0"),
            observation(category),
        ),
    )
}

fn context(
    category: ObservationCategory,
    semantic: u32,
    max: u32,
    step_retries: u32,
    capability: Option<&str>,
) -> DecisionContext {
    context_from(
        base_snapshot(category, capability),
        category,
        semantic,
        max,
        step_retries,
    )
}

fn decide(
    context: &DecisionContext,
    plan: Option<&DecompositionPlan>,
    approval: Option<&str>,
) -> ictus_policy::RecoveryDecision {
    GenericRecoveryProvider::new()
        .decide(RecoveryRequest {
            context,
            plan,
            plan_approval: approval,
        })
        .expect("a bounded decision")
}

// -- success bypass ---------------------------------------------------------

#[test]
fn success_is_explicit_no_recovery_work_not_abort() {
    let context = context(ObservationCategory::Success, 0, 2, 4, Some("demo.verify"));
    let decision = decide(&context, None, None);
    assert_eq!(decision.outcome, RecoveryOutcome::NoRecoveryWork);
    assert_ne!(decision.outcome, RecoveryOutcome::Abort);
    // Success must not emit an executable fallback, and `to_decision_proposal`
    // yields nothing at all.
    assert!(decision.capability.is_none());
    assert!(decision.to_decision_proposal().unwrap().is_none());
}

#[test]
fn success_bypass_ignores_the_attempt_budget() {
    // Even with a fully exhausted budget, success is not recovery work.
    let context = context(ObservationCategory::Success, 2, 2, 0, Some("demo.verify"));
    let decision = decide(&context, None, None);
    assert_eq!(decision.outcome, RecoveryOutcome::NoRecoveryWork);
}

// -- canonical semantic budget boundaries -----------------------------------

#[test]
fn budget_boundaries_zero_one_remaining_and_exhausted() {
    // Zero accepted attempts yet (a fresh budget).
    let zero = context(
        ObservationCategory::WorkerTimeout,
        0,
        2,
        0,
        Some("demo.verify"),
    );
    assert_eq!(
        decide(&zero, None, None).outcome,
        RecoveryOutcome::Reexecute
    );

    // Exactly one semantic attempt remaining.
    let one = context(
        ObservationCategory::WorkerTimeout,
        1,
        2,
        0,
        Some("demo.verify"),
    );
    assert_eq!(decide(&one, None, None).outcome, RecoveryOutcome::Reexecute);

    // Exhausted: the total bound has been reached.
    let exhausted = context(
        ObservationCategory::WorkerTimeout,
        2,
        2,
        0,
        Some("demo.verify"),
    );
    assert_eq!(
        decide(&exhausted, None, None).outcome,
        RecoveryOutcome::Escalate
    );

    // A zero bound is immediately exhausted and is never executable.
    let zero_bound = context(
        ObservationCategory::WorkerTimeout,
        0,
        0,
        0,
        Some("demo.verify"),
    );
    assert_eq!(
        decide(&zero_bound, None, None).outcome,
        RecoveryOutcome::Escalate
    );
}

#[test]
fn exhausted_budget_aborts_only_under_the_explicit_constraint() {
    let mut context = context(
        ObservationCategory::WorkerTimeout,
        2,
        2,
        0,
        Some("demo.verify"),
    );
    context
        .snapshot
        .constraints
        .push(CONSTRAINT_ABORT_ON_EXHAUSTION.to_string());
    let decision = decide(&context, None, None);
    assert_eq!(decision.outcome, RecoveryOutcome::Abort);
    assert!(decision.capability.is_none());
}

#[test]
fn step_retries_do_not_consume_the_semantic_budget_or_change_the_decision() {
    let without = context(
        ObservationCategory::ProcessCrash,
        1,
        2,
        0,
        Some("demo.verify"),
    );
    let with = context(
        ObservationCategory::ProcessCrash,
        1,
        2,
        7,
        Some("demo.verify"),
    );
    assert_eq!(
        decide(&without, None, None).outcome,
        decide(&with, None, None).outcome
    );
    assert_eq!(with.attempts.semantic_attempts, 1);
    assert_eq!(with.attempts.remaining(), 1);
}

#[test]
fn backend_retryable_hint_never_overrides_exhausted_policy() {
    let mut context = context(
        ObservationCategory::WorkerTimeout,
        2,
        2,
        0,
        Some("demo.verify"),
    );
    if let Some(binding) = context.recovery.as_mut() {
        binding.observation.retryable = Some(true);
    }
    assert_eq!(
        decide(&context, None, None).outcome,
        RecoveryOutcome::Escalate
    );
}

// -- ambiguous observation and missing capability ---------------------------

#[test]
fn unknown_observation_escalates_without_any_executable_fallback() {
    let context = context(ObservationCategory::Unknown, 0, 2, 0, Some("demo.verify"));
    let decision = decide(&context, None, None);
    assert_eq!(decision.outcome, RecoveryOutcome::Escalate);
    assert!(decision.capability.is_none());
    assert!(decision.route.is_none());
    assert!(decision.plan.is_none());
}

#[test]
fn missing_capability_never_produces_an_executable_fallback() {
    for category in [
        ObservationCategory::WorkerTimeout,
        ObservationCategory::ProcessCrash,
        ObservationCategory::ProviderUnavailable,
    ] {
        let context = context(category, 0, 2, 0, None);
        let decision = decide(&context, None, None);
        assert_eq!(
            decision.outcome,
            RecoveryOutcome::Escalate,
            "category {category:?}"
        );
        assert!(decision.capability.is_none());
    }
}

// -- persistent backend failure -> authorized ROUTE -------------------------

#[test]
fn persistent_backend_failure_routes_with_normalized_role_constraints() {
    let context = context_from(
        base_snapshot(
            ObservationCategory::ProviderUnavailable,
            Some("demo.verify"),
        )
        .with_fact(Fact::string("route.required_provider", "provider.b"))
        .with_fact(Fact::string("route.preferred_backend", "backend.b")),
        ObservationCategory::ProviderUnavailable,
        0,
        3,
        0,
    );
    let decision = decide(&context, None, None);
    assert_eq!(decision.outcome, RecoveryOutcome::Route);
    assert_eq!(decision.capability.as_deref(), Some("demo.verify"));
    let route = decision.route.as_ref().expect("role constraints");
    assert_eq!(route.required_provider.as_deref(), Some("provider.b"));
    assert_eq!(route.preferred_backend.as_deref(), Some("backend.b"));
    assert!(decision.to_decision_proposal().unwrap().is_some());
}

#[test]
fn persistent_backend_failure_without_route_facts_still_routes_unconstrained() {
    let context = context(
        ObservationCategory::ProviderUnavailable,
        1,
        2,
        0,
        Some("demo.verify"),
    );
    let decision = decide(&context, None, None);
    assert_eq!(decision.outcome, RecoveryOutcome::Route);
    assert!(decision.route.is_none());
}

#[test]
fn blank_route_evidence_escalates_instead_of_fabricating_a_route() {
    let context = context_from(
        base_snapshot(
            ObservationCategory::ProviderUnavailable,
            Some("demo.verify"),
        )
        .with_fact(Fact::string("route.required_provider", "   ")),
        ObservationCategory::ProviderUnavailable,
        0,
        2,
        0,
    );
    assert_eq!(
        decide(&context, None, None).outcome,
        RecoveryOutcome::Escalate
    );
}

// -- decomposition plan authorization ---------------------------------------

fn plan(children: usize, plan_id: &str) -> DecompositionPlan {
    DecompositionPlan::new(
        plan_id,
        (0..children)
            .map(|index| PlannedChild::new(format!("child-{index}"), "demo.verify"))
            .collect(),
    )
}

#[test]
fn semantic_failure_requires_a_plan_and_an_approval() {
    let context = context(
        ObservationCategory::VerificationFailure,
        0,
        2,
        0,
        Some("demo.verify"),
    );
    assert_eq!(
        decide(&context, None, None).outcome,
        RecoveryOutcome::Escalate
    );
    assert_eq!(
        decide(&context, Some(&plan(2, "plan-1")), None).outcome,
        RecoveryOutcome::Escalate
    );
    assert_eq!(
        decide(&context, Some(&plan(2, "plan-1")), Some("  ")).outcome,
        RecoveryOutcome::Escalate
    );
    let decision = decide(&context, Some(&plan(2, "plan-1")), Some("release-manager"));
    assert_eq!(decision.outcome, RecoveryOutcome::Decompose);
    // Ictus never creates children; the decision carries the description only.
    assert!(decision.capability.is_none());
    assert_eq!(decision.plan.as_ref().unwrap().children.len(), 2);
}

#[test]
fn oversized_or_empty_decomposition_plans_escalate() {
    let context = context(
        ObservationCategory::IntegrationConflict,
        0,
        2,
        0,
        Some("demo.verify"),
    );
    let oversized = plan(MAX_DECOMPOSITION_CHILDREN + 1, "plan-big");
    assert_eq!(
        decide(&context, Some(&oversized), Some("release-manager")).outcome,
        RecoveryOutcome::Escalate
    );
    let empty = plan(0, "plan-empty");
    assert_eq!(
        decide(&context, Some(&empty), Some("release-manager")).outcome,
        RecoveryOutcome::Escalate
    );
}

#[test]
fn decompose_proposal_carries_the_plan_and_recovery_outcome() {
    let context = context(
        ObservationCategory::VerificationFailure,
        0,
        2,
        0,
        Some("demo.verify"),
    );
    let decision = decide(&context, Some(&plan(1, "plan-1")), Some("release-manager"));
    let proposal = decision.to_decision_proposal().unwrap().unwrap();
    assert_eq!(proposal.decision, ictus_core::DecisionKind::Decompose);
    assert_eq!(proposal.arguments["plan_approved"], serde_json::json!(true));
    assert!(proposal.arguments.contains_key("decomposition_plan"));
    assert_eq!(
        proposal.arguments["recovery_outcome"],
        serde_json::json!("DECOMPOSE")
    );
}

// -- recovery-context boundary ----------------------------------------------

#[test]
fn initial_context_is_not_a_recovery_input() {
    let snapshot = base_snapshot(ObservationCategory::WorkerTimeout, Some("demo.verify"));
    let initial = DecisionContext::initial(
        "ctx-initial",
        "tactus.generic.v1",
        snapshot,
        1,
        AttemptBudget::new(2),
    );
    let result = GenericRecoveryProvider::new().decide(RecoveryRequest {
        context: &initial,
        plan: None,
        plan_approval: None,
    });
    assert!(result.is_err());
}

#[test]
fn decisions_are_pure_and_reproducible() {
    let context = context(
        ObservationCategory::WorkerTimeout,
        1,
        2,
        0,
        Some("demo.verify"),
    );
    let first = decide(&context, None, None);
    let second = decide(&context, None, None);
    assert_eq!(first, second);
    assert_eq!(context.kind, ContextKind::Recovery);
}

// -- accounting extraction --------------------------------------------------

#[test]
fn canonical_accounting_wins_over_the_legacy_pair() {
    let snapshot = StateSnapshot::new(
        "s1",
        "2026-10-01T00:00:00Z",
        "software",
        Subject::new("task", "t1"),
    )
    .with_fact(Fact::integer("attempts.semantic", 1))
    .with_fact(Fact::integer("attempts.max_semantic", 4))
    .with_fact(Fact::integer("retry.attempt", 9))
    .with_fact(Fact::integer("retry.budget", 9));
    let budget = semantic_budget_from_snapshot(&snapshot).unwrap();
    assert_eq!(budget.semantic_attempts, 1);
    assert_eq!(budget.max_semantic_attempts, 4);
}

#[test]
fn legacy_pair_is_accepted_only_as_a_complete_pair() {
    let complete = StateSnapshot::new(
        "s1",
        "2026-10-01T00:00:00Z",
        "software",
        Subject::new("task", "t1"),
    )
    .with_fact(Fact::integer("retry.attempt", 1))
    .with_fact(Fact::integer("retry.budget", 2));
    assert!(semantic_budget_from_snapshot(&complete).is_ok());

    let partial = StateSnapshot::new(
        "s1",
        "2026-10-01T00:00:00Z",
        "software",
        Subject::new("task", "t1"),
    )
    .with_fact(Fact::integer("retry.attempt", 1));
    assert!(semantic_budget_from_snapshot(&partial).is_err());
}

#[test]
fn missing_and_invalid_accounting_fail_closed() {
    let empty = StateSnapshot::new(
        "s1",
        "2026-10-01T00:00:00Z",
        "software",
        Subject::new("task", "t1"),
    );
    assert!(semantic_budget_from_snapshot(&empty).is_err());

    let over_bound = StateSnapshot::new(
        "s1",
        "2026-10-01T00:00:00Z",
        "software",
        Subject::new("task", "t1"),
    )
    .with_fact(Fact::integer("attempts.semantic", 3))
    .with_fact(Fact::integer("attempts.max_semantic", 2));
    assert!(semantic_budget_from_snapshot(&over_bound).is_err());

    let negative = StateSnapshot::new(
        "s1",
        "2026-10-01T00:00:00Z",
        "software",
        Subject::new("task", "t1"),
    )
    .with_fact(Fact::integer("attempts.semantic", -1))
    .with_fact(Fact::integer("attempts.max_semantic", 2));
    assert!(semantic_budget_from_snapshot(&negative).is_err());
}

#[test]
fn a_validated_decision_rejects_a_non_executable_fallback() {
    let context = context(ObservationCategory::Unknown, 0, 2, 0, Some("demo.verify"));
    let mut decision = decide(&context, None, None);
    decision.capability = Some("demo.verify".to_string());
    assert!(decision.validate().is_err());
}

// -- accounting extraction: partial and out-of-range ------------------------

fn plain_snapshot() -> StateSnapshot {
    StateSnapshot::new(
        "s-edge",
        "2026-10-01T00:00:00Z",
        "software",
        Subject::new("task", "t1"),
    )
}

#[test]
fn incomplete_canonical_accounting_fails_closed() {
    let semantic_only = plain_snapshot().with_fact(Fact::integer("attempts.semantic", 1));
    assert!(semantic_budget_from_snapshot(&semantic_only).is_err());

    let max_only = plain_snapshot().with_fact(Fact::integer("attempts.max_semantic", 1));
    assert!(semantic_budget_from_snapshot(&max_only).is_err());
}

#[test]
fn negative_and_out_of_range_accounting_fails_closed() {
    let negative_step = plain_snapshot()
        .with_fact(Fact::integer("attempts.semantic", 0))
        .with_fact(Fact::integer("attempts.max_semantic", 2))
        .with_fact(Fact::integer("attempts.step_retries", -1));
    assert!(semantic_budget_from_snapshot(&negative_step).is_err());

    let semantic_out_of_range = plain_snapshot()
        .with_fact(Fact::integer("attempts.semantic", 5_000_000_000))
        .with_fact(Fact::integer("attempts.max_semantic", 6_000_000_000));
    assert!(semantic_budget_from_snapshot(&semantic_out_of_range).is_err());

    let max_out_of_range = plain_snapshot()
        .with_fact(Fact::integer("attempts.semantic", 0))
        .with_fact(Fact::integer("attempts.max_semantic", 5_000_000_000));
    assert!(semantic_budget_from_snapshot(&max_out_of_range).is_err());

    let step_out_of_range = plain_snapshot()
        .with_fact(Fact::integer("attempts.semantic", 0))
        .with_fact(Fact::integer("attempts.max_semantic", 2))
        .with_fact(Fact::integer("attempts.step_retries", 5_000_000_000));
    assert!(semantic_budget_from_snapshot(&step_out_of_range).is_err());
}

// -- route-constraint extraction --------------------------------------------

#[test]
fn every_route_role_fact_is_read_and_constrained() {
    let snapshot = plain_snapshot()
        .with_fact(Fact::string("route.exclude_backend", "backend.a"))
        .with_fact(Fact::string("route.preferred_backend", "backend.b"))
        .with_fact(Fact::string("route.required_provider", "provider.c"))
        .with_fact(Fact::string("route.required_runtime", "runtime.d"));
    let constraints = route_constraints_from_snapshot(&snapshot)
        .unwrap()
        .expect("all role constraints are present");
    assert_eq!(constraints.exclude_backend.as_deref(), Some("backend.a"));
    assert_eq!(constraints.preferred_backend.as_deref(), Some("backend.b"));
    assert_eq!(constraints.required_provider.as_deref(), Some("provider.c"));
    assert_eq!(constraints.required_runtime.as_deref(), Some("runtime.d"));
}

#[test]
fn non_string_route_evidence_is_ambiguous() {
    let snapshot = plain_snapshot().with_fact(Fact::integer("route.exclude_backend", 7));
    assert!(route_constraints_from_snapshot(&snapshot).is_err());
}

// -- generic vocabulary binding ---------------------------------------------

#[test]
fn every_outcome_exposes_its_token_and_decision_kind() {
    let rows = [
        (RecoveryOutcome::NoRecoveryWork, "NO_RECOVERY_WORK", None),
        (
            RecoveryOutcome::Reexecute,
            "REEXECUTE",
            Some(DecisionKind::Reexecute),
        ),
        (RecoveryOutcome::Route, "ROUTE", Some(DecisionKind::Route)),
        (
            RecoveryOutcome::Decompose,
            "DECOMPOSE",
            Some(DecisionKind::Decompose),
        ),
        (
            RecoveryOutcome::Escalate,
            "ESCALATE",
            Some(DecisionKind::Escalate),
        ),
        (RecoveryOutcome::Abort, "ABORT", Some(DecisionKind::Abort)),
    ];
    for (outcome, token, kind) in rows {
        assert_eq!(outcome.token(), token);
        assert_eq!(outcome.decision_kind(), kind);
    }
}

#[test]
fn provider_exposes_its_policy_version() {
    assert_eq!(
        GenericRecoveryProvider::new().policy_version(),
        RECOVERY_POLICY_VERSION
    );
}

// -- provider boundary and exhausted routing --------------------------------

#[test]
fn a_valid_non_recovery_context_is_rejected() {
    let snapshot = plain_snapshot().with_fact(Fact::string("capability.id", "demo.verify"));
    let initial = DecisionContext::initial(
        "ctx-initial",
        "tactus.generic.v1",
        snapshot,
        1,
        AttemptBudget::new(2),
    );
    let result = GenericRecoveryProvider::new().decide(RecoveryRequest {
        context: &initial,
        plan: None,
        plan_approval: None,
    });
    assert!(result.is_err());
}

#[test]
fn exhausted_provider_unavailable_escalates_or_aborts() {
    let mut context = context(
        ObservationCategory::ProviderUnavailable,
        2,
        2,
        0,
        Some("demo.verify"),
    );
    assert_eq!(
        decide(&context, None, None).outcome,
        RecoveryOutcome::Escalate
    );
    context
        .snapshot
        .constraints
        .push(CONSTRAINT_ABORT_ON_EXHAUSTION.to_string());
    assert_eq!(decide(&context, None, None).outcome, RecoveryOutcome::Abort);
}

// -- remaining normalized observation categories ----------------------------

#[test]
fn resource_exhausted_reexecutes_and_integration_conflict_decomposes() {
    let reexecutable = context(
        ObservationCategory::ResourceExhausted,
        0,
        2,
        0,
        Some("demo.verify"),
    );
    assert_eq!(
        decide(&reexecutable, None, None).outcome,
        RecoveryOutcome::Reexecute
    );

    let conflict = context(
        ObservationCategory::IntegrationConflict,
        0,
        2,
        0,
        Some("demo.verify"),
    );
    let decision = decide(
        &conflict,
        Some(&plan(1, "plan-conflict")),
        Some("release-manager"),
    );
    assert_eq!(decision.outcome, RecoveryOutcome::Decompose);
}

// -- decision structural validation -----------------------------------------

#[test]
fn an_executable_decision_rejects_a_malformed_shape() {
    let context = context(
        ObservationCategory::WorkerTimeout,
        0,
        2,
        0,
        Some("demo.verify"),
    );

    let mut no_capability = decide(&context, None, None);
    assert_eq!(no_capability.outcome, RecoveryOutcome::Reexecute);
    no_capability.capability = None;
    assert!(no_capability.validate().is_err());

    let mut with_plan = decide(&context, None, None);
    with_plan.plan = Some(plan(1, "plan-shape"));
    assert!(with_plan.validate().is_err());

    let mut with_route = decide(&context, None, None);
    with_route.route = Some(RouteConstraints::new());
    assert!(with_route.validate().is_err());
}

#[test]
fn a_decompose_decision_rejects_executable_fields_or_a_missing_plan() {
    let context = context(
        ObservationCategory::VerificationFailure,
        0,
        2,
        0,
        Some("demo.verify"),
    );

    let mut with_capability = decide(&context, Some(&plan(1, "plan-shape")), Some("approver"));
    assert_eq!(with_capability.outcome, RecoveryOutcome::Decompose);
    with_capability.capability = Some("demo.verify".to_string());
    assert!(with_capability.validate().is_err());

    let mut with_route = decide(&context, Some(&plan(1, "plan-shape")), Some("approver"));
    with_route.route = Some(RouteConstraints::new());
    assert!(with_route.validate().is_err());

    let mut without_plan = decide(&context, Some(&plan(1, "plan-shape")), Some("approver"));
    without_plan.plan = None;
    assert!(without_plan.validate().is_err());
}

// -- decision-table hardening: error variants and bounded-plan edges -------
//
// These rows pin the exact error variant, the exact child bound and the exact
// rationale strings so the decision table cannot silently degrade into a
// different-but-still-erroneous path.

fn accounting_snapshot(
    semantic: Option<i64>,
    max: Option<i64>,
    step_retries: Option<i64>,
) -> StateSnapshot {
    let mut snapshot = StateSnapshot::new(
        "snap-accounting",
        "2026-10-01T00:05:00Z",
        "software",
        Subject::new("task", "task-123"),
    );
    if let Some(value) = semantic {
        snapshot = snapshot.with_fact(Fact::integer("attempts.semantic", value));
    }
    if let Some(value) = max {
        snapshot = snapshot.with_fact(Fact::integer("attempts.max_semantic", value));
    }
    if let Some(value) = step_retries {
        snapshot = snapshot.with_fact(Fact::integer("attempts.step_retries", value));
    }
    snapshot
}

#[test]
fn absent_accounting_is_reported_as_the_missing_canonical_pair() {
    match semantic_budget_from_snapshot(&accounting_snapshot(None, None, None)) {
        Err(ictus_core::ContractError::MissingField(field)) => {
            assert_eq!(field, "attempts.semantic / attempts.max_semantic");
        }
        other => panic!("expected the missing canonical pair, got {other:?}"),
    }
}

#[test]
fn a_negative_semantic_bound_is_reported_as_non_negative() {
    match semantic_budget_from_snapshot(&accounting_snapshot(Some(-1), Some(2), None)) {
        Err(ictus_core::ContractError::InvalidValue { field, reason }) => {
            assert_eq!(field, "attempts");
            assert!(reason.contains("non-negative"), "reason was {reason}");
        }
        other => panic!("expected a non-negative violation, got {other:?}"),
    }

    // A negative maximum is also invalid, independently of the attempt count.
    assert!(semantic_budget_from_snapshot(&accounting_snapshot(Some(0), Some(-1), None)).is_err());
}

#[test]
fn a_zero_bound_is_valid_accounting() {
    let budget = semantic_budget_from_snapshot(&accounting_snapshot(Some(0), Some(0), None))
        .expect("a zero bound is consistent, not an error");
    assert_eq!(budget.semantic_attempts, 0);
    assert_eq!(budget.max_semantic_attempts, 0);
    assert_eq!(budget.step_retries, 0);
    assert!(budget.is_exhausted());
}

#[test]
fn a_positive_step_retry_count_is_valid_and_separate() {
    let budget = semantic_budget_from_snapshot(&accounting_snapshot(Some(0), Some(2), Some(5)))
        .expect("positive step retries are valid accounting");
    assert_eq!(budget.semantic_attempts, 0);
    assert_eq!(budget.max_semantic_attempts, 2);
    assert_eq!(budget.step_retries, 5);
}

#[test]
fn a_blank_child_field_is_rejected_but_a_complete_child_is_accepted() {
    assert!(PlannedChild::new("", "demo.verify").validate().is_err());
    assert!(PlannedChild::new("child-0", "   ").validate().is_err());
    assert!(PlannedChild::new("child-0", "demo.verify")
        .validate()
        .is_ok());
}

#[test]
fn a_plan_serializes_its_bounded_children() {
    let plan = DecompositionPlan::new(
        "plan-json",
        vec![
            PlannedChild::new("child-0", "demo.verify"),
            PlannedChild::new("child-1", "demo.other"),
        ],
    );
    assert_eq!(
        plan.to_json(),
        serde_json::json!({
            "plan_id": "plan-json",
            "children": [
                { "subject_id": "child-0", "capability": "demo.verify" },
                { "subject_id": "child-1", "capability": "demo.other" },
            ]
        })
    );
}

#[test]
fn a_plan_at_the_child_bound_is_accepted() {
    let at_bound = plan(MAX_DECOMPOSITION_CHILDREN, "plan-at-bound");
    assert!(at_bound.validate().is_ok());
    let over_bound = plan(MAX_DECOMPOSITION_CHILDREN + 1, "plan-over-bound");
    assert!(over_bound.validate().is_err());
}

#[test]
fn an_executable_decision_rejects_a_blank_capability() {
    let context = context(
        ObservationCategory::WorkerTimeout,
        0,
        2,
        0,
        Some("demo.verify"),
    );
    let mut decision = decide(&context, None, None);
    assert_eq!(decision.outcome, RecoveryOutcome::Reexecute);
    decision.capability = Some("   ".to_string());
    assert!(decision.validate().is_err());
}

#[test]
fn a_non_executable_decision_rejects_a_lone_plan_or_route() {
    let context = context(ObservationCategory::Unknown, 0, 2, 0, Some("demo.verify"));

    let mut lone_plan = decide(&context, None, None);
    assert_eq!(lone_plan.outcome, RecoveryOutcome::Escalate);
    lone_plan.plan = Some(plan(1, "plan-lone"));
    assert!(lone_plan.validate().is_err());

    let mut lone_route = decide(&context, None, None);
    lone_route.route = Some(RouteConstraints::new());
    assert!(lone_route.validate().is_err());
}

#[test]
fn a_plan_without_approval_reports_the_missing_approval() {
    let context = context(
        ObservationCategory::VerificationFailure,
        0,
        2,
        0,
        Some("demo.verify"),
    );
    let decision = decide(&context, Some(&plan(1, "plan-no-approval")), None);
    assert_eq!(decision.outcome, RecoveryOutcome::Escalate);
    assert!(
        decision.rationale.contains("requires an explicit approval"),
        "rationale was {}",
        decision.rationale
    );
}

#[test]
fn the_rationale_embeds_the_normalized_category_token() {
    let reexec = context(
        ObservationCategory::WorkerTimeout,
        0,
        2,
        0,
        Some("demo.verify"),
    );
    let decision = decide(&reexec, None, None);
    assert_eq!(decision.outcome, RecoveryOutcome::Reexecute);
    assert!(
        decision.rationale.contains("WORKER_TIMEOUT"),
        "rationale was {}",
        decision.rationale
    );

    let decompose = context(
        ObservationCategory::VerificationFailure,
        0,
        2,
        0,
        Some("demo.verify"),
    );
    let decision = decide(&decompose, Some(&plan(1, "plan-token")), Some("approver"));
    assert_eq!(decision.outcome, RecoveryOutcome::Decompose);
    assert!(
        decision.rationale.contains("VERIFICATION_FAILURE"),
        "rationale was {}",
        decision.rationale
    );
}
