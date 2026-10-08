//! Contract-validation coverage for the routing fact types and the execution
//! receipt binding. These are the fail-closed boundaries a runtime adapter
//! must not be able to bypass.

use ictus_core::{
    ExecutionReceipt, ObservationState, RouteCandidate, RouteConstraints, RouteDescriptor,
    RouteFactRef, RouteFacts, RouteIdentity, RouteObservation, RouteRequirements, SelectedRoute,
};

fn identity() -> RouteIdentity {
    RouteIdentity::new("backend", "provider", "runtime", "model")
}

fn observation(fact_id: &str, route: &RouteIdentity, state: ObservationState) -> RouteObservation {
    RouteObservation::new(
        RouteFactRef::new(fact_id, 1),
        route.clone(),
        state,
        "2026-10-01T00:00:00Z",
    )
}

fn descriptor() -> RouteDescriptor {
    RouteDescriptor::new(RouteFactRef::new("descriptor", 1), identity()).with_capability("cap")
}

fn candidate() -> RouteCandidate {
    let route = identity();
    RouteFacts {
        descriptor: descriptor(),
        health: observation("health", &route, ObservationState::Available),
        quota: observation("quota", &route, ObservationState::Available),
        disablement: observation("disablement", &route, ObservationState::Available),
    }
    .try_into()
    .unwrap()
}

#[test]
fn route_identity_validate_rejects_every_blank_field() {
    assert!(identity().validate().is_ok());

    for blank in 0..4 {
        let mut route = identity();
        match blank {
            0 => route.backend = String::new(),
            1 => route.provider = String::new(),
            2 => route.runtime = String::new(),
            _ => route.model = String::new(),
        }
        assert!(
            route.validate().is_err(),
            "blank field {blank} must be rejected"
        );
    }
}

#[test]
fn route_fact_ref_validate_rejects_blank_id() {
    assert!(RouteFactRef::new("fact", 1).validate("f").is_ok());
    assert!(RouteFactRef::new("", 1).validate("f").is_err());
    assert!(RouteFactRef::new("   ", 1).validate("f").is_err());
}

#[test]
fn route_descriptor_validate_rejects_each_invalid_field() {
    assert!(descriptor().validate().is_ok());

    let mut bad = descriptor();
    bad.schema_version = 999;
    assert!(bad.validate().is_err());

    let mut bad = descriptor();
    bad.fact.fact_id = String::new();
    assert!(bad.validate().is_err());

    let mut bad = descriptor();
    bad.route.backend = String::new();
    assert!(bad.validate().is_err());

    let mut bad = descriptor();
    bad.capabilities = vec![String::new()];
    assert!(bad.validate().is_err());
}

#[test]
fn descriptor_enabled_defaults_to_true_when_absent_from_json() {
    let json = serde_json::json!({
        "schema_version": 1,
        "fact": { "fact_id": "descriptor", "revision": 1 },
        "route": { "backend": "b", "provider": "p", "runtime": "r", "model": "m" },
        "capabilities": ["cap"]
    });
    let decoded: RouteDescriptor = serde_json::from_value(json).unwrap();
    assert!(decoded.enabled);
}

#[test]
fn route_observation_validate_rejects_each_invalid_field() {
    let route = identity();
    assert!(observation("health", &route, ObservationState::Available)
        .validate("health")
        .is_ok());

    let mut bad = observation("health", &route, ObservationState::Available);
    bad.schema_version = 999;
    assert!(bad.validate("health").is_err());

    let mut bad = observation("", &route, ObservationState::Available);
    bad.fact.fact_id = String::new();
    assert!(bad.validate("health").is_err());

    let mut bad = observation("health", &route, ObservationState::Available);
    bad.route.model = String::new();
    assert!(bad.validate("health").is_err());

    let mut bad = observation("health", &route, ObservationState::Available);
    bad.observed_at = String::new();
    assert!(bad.validate("health").is_err());
}

#[test]
fn route_candidate_supports_only_declared_capabilities() {
    let candidate = candidate();
    assert!(candidate.supports("cap"));
    assert!(!candidate.supports("other"));
    assert!(!candidate.supports(""));
}

#[test]
fn route_candidate_validate_rejects_any_route_binding_mismatch() {
    assert!(candidate().validate().is_ok());

    // Exactly one observation bound to a different route must fail closed.
    for mismatch in 0..3 {
        let mut candidate = candidate();
        let other = RouteIdentity::new("other", "provider", "runtime", "model");
        match mismatch {
            0 => candidate.health.route = other,
            1 => candidate.quota.route = other,
            _ => candidate.disablement.route = other,
        }
        assert!(
            candidate.validate().is_err(),
            "observation {mismatch} must bind the descriptor route"
        );
    }
}

#[test]
fn route_requirements_for_capability_sets_only_the_capability() {
    let requirements = RouteRequirements::for_capability("cap");
    assert_eq!(requirements.capability, "cap");
    assert!(requirements.exclude_backend.is_none());
    assert!(requirements.preferred_backend.is_none());
    assert!(requirements.required_provider.is_none());
    assert!(requirements.required_runtime.is_none());
}

#[test]
fn route_requirements_from_constraints_maps_every_field() {
    let constraints = RouteConstraints::new()
        .with_exclude_backend("excluded")
        .with_preferred_backend("preferred")
        .with_required_provider("provider")
        .with_required_runtime("runtime");
    let requirements = RouteRequirements::from_constraints("cap", Some(&constraints));
    assert_eq!(requirements.capability, "cap");
    assert_eq!(requirements.exclude_backend.as_deref(), Some("excluded"));
    assert_eq!(requirements.preferred_backend.as_deref(), Some("preferred"));
    assert_eq!(requirements.required_provider.as_deref(), Some("provider"));
    assert_eq!(requirements.required_runtime.as_deref(), Some("runtime"));

    let empty = RouteRequirements::from_constraints("cap", None);
    assert_eq!(empty.capability, "cap");
    assert!(empty.exclude_backend.is_none());
}

#[test]
fn route_requirements_validate_rejects_blank_values() {
    assert!(RouteRequirements::for_capability("cap").validate().is_ok());

    let mut bad = RouteRequirements::for_capability("");
    assert!(bad.validate().is_err());

    bad = RouteRequirements::for_capability("cap");
    bad.exclude_backend = Some(String::new());
    assert!(bad.validate().is_err());

    bad = RouteRequirements::for_capability("cap");
    bad.preferred_backend = Some("   ".to_string());
    assert!(bad.validate().is_err());

    bad = RouteRequirements::for_capability("cap");
    bad.required_provider = Some(String::new());
    assert!(bad.validate().is_err());

    bad = RouteRequirements::for_capability("cap");
    bad.required_runtime = Some(String::new());
    assert!(bad.validate().is_err());
}

#[test]
fn selected_route_validate_rejects_invalid_identities() {
    let candidate = candidate();
    assert!(SelectedRoute::from_candidate(&candidate).validate().is_ok());

    let mut selected = SelectedRoute::from_candidate(&candidate);
    selected.route.model = String::new();
    assert!(selected.validate().is_err());

    let mut selected = SelectedRoute::from_candidate(&candidate);
    selected.descriptor.fact_id = String::new();
    assert!(selected.validate().is_err());

    let mut selected = SelectedRoute::from_candidate(&candidate);
    selected.health.fact_id = String::new();
    assert!(selected.validate().is_err());

    let mut selected = SelectedRoute::from_candidate(&candidate);
    selected.quota.fact_id = String::new();
    assert!(selected.validate().is_err());

    let mut selected = SelectedRoute::from_candidate(&candidate);
    selected.disablement.fact_id = String::new();
    assert!(selected.validate().is_err());
}

#[test]
fn execution_receipt_validate_rejects_blank_fields_and_bad_digest() {
    let valid = ExecutionReceipt::new(
        "receipt-1",
        "exec-1",
        "intent-1",
        "a".repeat(64),
        "SUBMITTED",
        "2026-10-01T00:00:00Z",
    );
    assert!(valid.validate().is_ok());

    for blank in 0..5 {
        let mut receipt = valid.clone();
        match blank {
            0 => receipt.receipt_id = String::new(),
            1 => receipt.execution_id = String::new(),
            2 => receipt.intent_id = String::new(),
            3 => receipt.status = String::new(),
            _ => receipt.submitted_at = String::new(),
        }
        assert!(
            receipt.validate().is_err(),
            "blank receipt field {blank} must be rejected"
        );
    }

    // Wrong length but otherwise hex.
    let mut short = valid.clone();
    short.intent_digest = "a".repeat(63);
    assert!(short.validate().is_err());

    // Right length but not hexadecimal.
    let mut non_hex = valid.clone();
    non_hex.intent_digest = "z".repeat(64);
    assert!(non_hex.validate().is_err());
}
