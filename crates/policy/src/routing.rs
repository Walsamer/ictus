//! Deterministic compatibility and route selection.
//!
//! Runtime adapters report raw facts and execute the route Ictus selected. They
//! do not rank candidates or substitute fallbacks. This module is intentionally
//! pure: fixed facts and policy always produce the same result.

use ictus_core::{
    ObservationState, RouteCandidate, RouteExclusion, RouteIdentity, RouteRequirements,
    SelectedRoute,
};

/// Policy for facts that cannot prove current availability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UncertainFactPolicy {
    /// Fail closed: an unknown or stale fact makes the route ineligible.
    Reject,
    /// Do not select the route automatically; ask for a bounded approval.
    RequireApproval,
}

/// Compatibility and preference policy owned by Ictus.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingPolicy {
    pub forbidden_backends: Vec<String>,
    pub forbidden_providers: Vec<String>,
    pub forbidden_runtimes: Vec<String>,
    pub forbidden_models: Vec<String>,
    /// Ordered preference. The first matching backend wins over later entries;
    /// lexical route identity resolves every remaining tie.
    pub preferred_backends: Vec<String>,
    pub unknown_observation: UncertainFactPolicy,
    pub stale_observation: UncertainFactPolicy,
}

impl Default for RoutingPolicy {
    fn default() -> Self {
        Self {
            forbidden_backends: Vec::new(),
            forbidden_providers: Vec::new(),
            forbidden_runtimes: Vec::new(),
            forbidden_models: Vec::new(),
            preferred_backends: Vec::new(),
            unknown_observation: UncertainFactPolicy::Reject,
            stale_observation: UncertainFactPolicy::Reject,
        }
    }
}

/// A bounded outcome from route selection. Neither non-selected outcome can be
/// converted to an execution intent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteSelection {
    Selected(SelectedRoute),
    NoRoute(NoRoute),
    RequireApproval(RouteApproval),
}

/// Bounded machine-readable no-route reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoRouteReason {
    InvalidInput,
    NoCandidates,
    NoCompatibleCandidate,
    AllCandidatesUnavailable,
    AllCandidatesExcluded,
    AllCandidatesForbidden,
}

/// No-route output intentionally contains a finite reason code and no intent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoRoute {
    pub reason: NoRouteReason,
    pub detail: String,
}

/// Approval-required output caused only by an explicitly configured uncertain
/// fact policy. It names fact identities but never chooses a route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteApproval {
    pub reason: String,
    pub facts: Vec<ictus_core::RouteFactRef>,
}

/// Select one route from normalized candidate facts.
///
/// Required and excluded constraints are hard gates. Preference is applied only
/// among eligible candidates. Candidate input order is ignored: selected routes
/// are sorted by preference rank and then full route identity.
pub fn select_route(
    requirements: &RouteRequirements,
    candidates: &[RouteCandidate],
    policy: &RoutingPolicy,
    prior_exclusions: &[RouteExclusion],
) -> RouteSelection {
    if requirements.validate().is_err()
        || candidates
            .iter()
            .any(|candidate| candidate.validate().is_err())
    {
        return no_route(
            NoRouteReason::InvalidInput,
            "invalid route requirements or observed facts",
        );
    }
    if candidates.is_empty() {
        return no_route(
            NoRouteReason::NoCandidates,
            "no route candidates were observed",
        );
    }

    let mut compatible = Vec::new();
    let mut excluded = 0usize;
    let mut forbidden = 0usize;
    for candidate in candidates {
        let route = candidate.identity();
        if !candidate.descriptor.enabled
            || !candidate.supports(&requirements.capability)
            || requirements.exclude_backend.as_deref() == Some(route.backend.as_str())
            || requirements
                .required_provider
                .as_deref()
                .is_some_and(|value| value != route.provider)
            || requirements
                .required_runtime
                .as_deref()
                .is_some_and(|value| value != route.runtime)
        {
            continue;
        }
        if prior_exclusions
            .iter()
            .any(|excluded_route| excluded_route.route == *route)
        {
            excluded += 1;
            continue;
        }
        if contains(&policy.forbidden_backends, &route.backend)
            || contains(&policy.forbidden_providers, &route.provider)
            || contains(&policy.forbidden_runtimes, &route.runtime)
            || contains(&policy.forbidden_models, &route.model)
        {
            forbidden += 1;
            continue;
        }
        compatible.push(candidate);
    }

    if compatible.is_empty() {
        let (reason, detail) = if excluded > 0 {
            (
                NoRouteReason::AllCandidatesExcluded,
                "every compatible candidate was previously excluded",
            )
        } else if forbidden > 0 {
            (
                NoRouteReason::AllCandidatesForbidden,
                "every compatible candidate is forbidden by policy",
            )
        } else {
            (
                NoRouteReason::NoCompatibleCandidate,
                "no candidate satisfies the required capability and route constraints",
            )
        };
        return no_route(reason, detail);
    }

    let mut available = Vec::new();
    let mut approval_facts = Vec::new();
    for candidate in compatible {
        match eligibility(candidate, policy, &mut approval_facts) {
            Eligibility::Available => available.push(candidate),
            Eligibility::Unavailable | Eligibility::Uncertain => {}
        }
    }
    if !available.is_empty() {
        available.sort_by(|left, right| candidate_order(left, right, requirements, policy));
        return RouteSelection::Selected(SelectedRoute::from_candidate(available[0]));
    }
    if !approval_facts.is_empty() {
        approval_facts.sort_by(|left, right| {
            (left.fact_id.as_str(), left.revision).cmp(&(right.fact_id.as_str(), right.revision))
        });
        approval_facts.dedup();
        return RouteSelection::RequireApproval(RouteApproval {
            reason: "route observations are stale or unknown under approval policy".to_string(),
            facts: approval_facts,
        });
    }
    no_route(
        NoRouteReason::AllCandidatesUnavailable,
        "all compatible candidates are disabled, unavailable, stale, or out of quota",
    )
}

fn no_route(reason: NoRouteReason, detail: impl Into<String>) -> RouteSelection {
    RouteSelection::NoRoute(NoRoute {
        reason,
        detail: detail.into(),
    })
}

fn contains(items: &[String], value: &str) -> bool {
    items.iter().any(|item| item == value)
}

enum Eligibility {
    Available,
    Unavailable,
    Uncertain,
}

fn eligibility(
    candidate: &RouteCandidate,
    policy: &RoutingPolicy,
    approval_facts: &mut Vec<ictus_core::RouteFactRef>,
) -> Eligibility {
    let facts = [
        (&candidate.health, "health"),
        (&candidate.quota, "quota"),
        (&candidate.disablement, "disablement"),
    ];
    let mut uncertain = false;
    for (fact, _name) in facts {
        match fact.state {
            // A disablement fact uses AVAILABLE to mean explicitly enabled;
            // UNAVAILABLE means disabled. Its role is otherwise identical to
            // health and quota: it must positively prove eligibility.
            ObservationState::Available => {}
            ObservationState::Unavailable => return Eligibility::Unavailable,
            ObservationState::Unknown => match policy.unknown_observation {
                UncertainFactPolicy::Reject => return Eligibility::Unavailable,
                UncertainFactPolicy::RequireApproval => {
                    uncertain = true;
                    approval_facts.push(fact.fact.clone());
                }
            },
            ObservationState::Stale => match policy.stale_observation {
                UncertainFactPolicy::Reject => return Eligibility::Unavailable,
                UncertainFactPolicy::RequireApproval => {
                    uncertain = true;
                    approval_facts.push(fact.fact.clone());
                }
            },
        }
    }
    if uncertain {
        Eligibility::Uncertain
    } else {
        Eligibility::Available
    }
}

fn route_rank(
    route: &RouteIdentity,
    requirements: &RouteRequirements,
    policy: &RoutingPolicy,
) -> (u8, usize, String, String, String, String) {
    let requested_preference =
        requirements.preferred_backend.as_deref() == Some(route.backend.as_str());
    let policy_rank = policy
        .preferred_backends
        .iter()
        .position(|backend| backend == &route.backend)
        .unwrap_or(usize::MAX);
    (
        if requested_preference { 0 } else { 1 },
        policy_rank,
        route.backend.clone(),
        route.provider.clone(),
        route.runtime.clone(),
        route.model.clone(),
    )
}

/// Include the observed fact identities/revisions after route identity so even
/// conflicting duplicate route observations cannot make input order choose the
/// result. The adapter is still responsible for not emitting contradictory
/// facts; this ordering merely keeps policy deterministic while reporting them.
fn candidate_order(
    candidate: &RouteCandidate,
    other: &RouteCandidate,
    requirements: &RouteRequirements,
    policy: &RoutingPolicy,
) -> std::cmp::Ordering {
    let route_order = route_rank(candidate.identity(), requirements, policy).cmp(&route_rank(
        other.identity(),
        requirements,
        policy,
    ));
    if route_order != std::cmp::Ordering::Equal {
        return route_order;
    }
    for (left, right) in [
        (&candidate.descriptor.fact, &other.descriptor.fact),
        (&candidate.health.fact, &other.health.fact),
        (&candidate.quota.fact, &other.quota.fact),
        (&candidate.disablement.fact, &other.disablement.fact),
    ] {
        let order =
            (left.fact_id.as_str(), left.revision).cmp(&(right.fact_id.as_str(), right.revision));
        if order != std::cmp::Ordering::Equal {
            return order;
        }
    }
    std::cmp::Ordering::Equal
}

#[cfg(test)]
mod tests {
    use super::*;
    use ictus_core::{RouteDescriptor, RouteFactRef, RouteFacts, RouteObservation};

    fn candidate(backend: &str, state: ObservationState) -> RouteCandidate {
        let route = RouteIdentity::new(backend, "provider", "runtime", "model");
        RouteFacts {
            descriptor: RouteDescriptor::new(
                RouteFactRef::new(format!("{backend}.descriptor"), 3),
                route,
            )
            .with_capability("capability"),
            health: RouteObservation::new(
                RouteFactRef::new(format!("{backend}.health"), 4),
                RouteIdentity::new(backend, "provider", "runtime", "model"),
                state,
                "2026-10-08T00:00:00Z",
            ),
            quota: RouteObservation::new(
                RouteFactRef::new(format!("{backend}.quota"), 5),
                RouteIdentity::new(backend, "provider", "runtime", "model"),
                state,
                "2026-10-08T00:00:00Z",
            ),
            disablement: RouteObservation::new(
                RouteFactRef::new(format!("{backend}.disablement"), 6),
                RouteIdentity::new(backend, "provider", "runtime", "model"),
                state,
                "2026-10-08T00:00:00Z",
            ),
        }
        .try_into()
        .unwrap()
    }

    #[test]
    fn selection_is_order_independent_and_ties_are_lexical() {
        let requirements = RouteRequirements::for_capability("capability");
        let first = select_route(
            &requirements,
            &[
                candidate("z", ObservationState::Available),
                candidate("a", ObservationState::Available),
            ],
            &RoutingPolicy::default(),
            &[],
        );
        let second = select_route(
            &requirements,
            &[
                candidate("a", ObservationState::Available),
                candidate("z", ObservationState::Available),
            ],
            &RoutingPolicy::default(),
            &[],
        );
        assert_eq!(first, second);
        assert_eq!(
            first,
            RouteSelection::Selected(SelectedRoute::from_candidate(&candidate(
                "a",
                ObservationState::Available
            )))
        );
    }

    #[test]
    fn unavailable_and_forbidden_candidates_are_never_selected() {
        let requirements = RouteRequirements::for_capability("capability");
        let policy = RoutingPolicy {
            forbidden_backends: vec!["allowed".to_string()],
            ..RoutingPolicy::default()
        };
        assert!(matches!(
            select_route(
                &requirements,
                &[
                    candidate("allowed", ObservationState::Available),
                    candidate("down", ObservationState::Unavailable)
                ],
                &policy,
                &[]
            ),
            RouteSelection::NoRoute(_)
        ));
    }

    #[test]
    fn disabled_candidate_is_skipped_in_favor_of_an_eligible_route() {
        let requirements = RouteRequirements::for_capability("capability");
        let mut disabled = candidate("a", ObservationState::Available);
        disabled.descriptor.enabled = false;
        let selection = select_route(
            &requirements,
            &[disabled, candidate("z", ObservationState::Available)],
            &RoutingPolicy::default(),
            &[],
        );
        assert_eq!(
            selection,
            RouteSelection::Selected(SelectedRoute::from_candidate(&candidate(
                "z",
                ObservationState::Available
            )))
        );
    }

    #[test]
    fn stale_facts_can_require_approval_without_selecting_a_route() {
        let requirements = RouteRequirements::for_capability("capability");
        let policy = RoutingPolicy {
            stale_observation: UncertainFactPolicy::RequireApproval,
            ..RoutingPolicy::default()
        };
        assert!(matches!(
            select_route(
                &requirements,
                &[candidate("a", ObservationState::Stale)],
                &policy,
                &[]
            ),
            RouteSelection::RequireApproval(_)
        ));
    }

    #[test]
    fn duplicate_route_identity_is_also_order_independent() {
        let requirements = RouteRequirements::for_capability("capability");
        let mut earlier = candidate("a", ObservationState::Available);
        earlier.health.fact.revision = 4;
        let mut later = candidate("a", ObservationState::Available);
        later.health.fact.revision = 9;
        let first = select_route(
            &requirements,
            &[later.clone(), earlier.clone()],
            &RoutingPolicy::default(),
            &[],
        );
        let second = select_route(
            &requirements,
            &[earlier.clone(), later],
            &RoutingPolicy::default(),
            &[],
        );
        assert_eq!(first, second);
        assert_eq!(
            first,
            RouteSelection::Selected(SelectedRoute::from_candidate(&earlier))
        );
    }
}
