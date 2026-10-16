//! Generic, bounded semantic recovery policy.
//!
//! This is the *generic recovery provider*: it turns a normalized
//! [`DecisionContext`] (a correlated terminal observation plus the canonical
//! semantic attempt accounting) into a **bounded, validated
//! [`RecoveryDecision`]** with an explicit rationale. It never executes,
//! schedules, loops or mutates durable state; it is a pure function of its
//! inputs.
//!
//! The provider chooses between the frozen generic vocabulary outcomes:
//! semantic re-execution (`REEXECUTE`), rerouting (`ROUTE`), decomposition
//! (`DECOMPOSE`), escalation (`ESCALATE`) and abort (`ABORT`). Success is
//! deliberately **not** `ABORT`: it is an explicit [`RecoveryOutcome::NoRecoveryWork`]
//! so a cross-system fixture can never interpret "observation reports success"
//! as permission to retire completed domain work.
//!
//! Boundaries:
//!
//! - Transient execution (step) retries are Dagster-owned. They never consume
//!   the semantic budget and never change the decision; only a correlated
//!   terminal failure enters recovery.
//! - The canonical semantic accounting is `attempts.semantic` /
//!   `attempts.max_semantic`. The legacy v1 keys `retry.attempt` /
//!   `retry.budget` are accepted only as a complete pair. A missing or
//!   ambiguous pair is **not** an implicit `0/0` exhausted budget; it escalates.
//! - `DECOMPOSE` requires a typed, bounded [`DecompositionPlan`] *and* an
//!   explicit approval. Ictus never creates the children.

use ictus_core::{
    facts, AttemptBudget, ContextKind, ContractError, DecisionContext, DecisionKind,
    DecisionProposal, EvidenceRef, ExecutionObservation, ObservationCategory, ProviderMetadata,
    RouteConstraints, StateSnapshot, Subject,
};
use ictus_ports::PortError;

/// Deterministic provider id recorded on proposals emitted by this policy.
pub const RECOVERY_PROVIDER_ID: &str = "recovery.v1";

/// Stable identifier of this implementation's recovery policy semantics.
pub const RECOVERY_POLICY_VERSION: &str = "ictus.recovery.v1";

/// Generic, domain-neutral constraint token.
///
/// When present in the normalized snapshot, an exhausted semantic budget
/// resolves to an explicit `ABORT` instead of handing to a human. The default
/// is fail-closed escalation.
pub const CONSTRAINT_ABORT_ON_EXHAUSTION: &str = "abort_on_exhaustion";

/// Hard bound on the number of children a bounded decomposition plan may
/// describe. Ictus never creates them; the bound keeps the plan finite.
pub const MAX_DECOMPOSITION_CHILDREN: usize = 16;

/// Normalized, role-only route fact keys (never concrete vendors).
pub const ROUTE_EXCLUDE_BACKEND: &str = "route.exclude_backend";
pub const ROUTE_PREFERRED_BACKEND: &str = "route.preferred_backend";
pub const ROUTE_REQUIRED_PROVIDER: &str = "route.required_provider";
pub const ROUTE_REQUIRED_RUNTIME: &str = "route.required_runtime";

/// Read the canonical semantic attempt accounting from a normalized snapshot.
///
/// The canonical keys win. The legacy v1 keys are accepted only as a complete
/// pair for migration. A missing, partial, negative or out-of-range pair is an
/// error: there is deliberately **no** implicit `0/0` fallback that would make
/// absent accounting look exhausted (or make it executable).
pub fn semantic_budget_from_snapshot(
    snapshot: &StateSnapshot,
) -> Result<AttemptBudget, ContractError> {
    let canonical = (
        snapshot.fact_i64(facts::ATTEMPTS_SEMANTIC),
        snapshot.fact_i64(facts::ATTEMPTS_MAX_SEMANTIC),
    );
    let legacy = (
        snapshot.fact_i64(facts::LEGACY_RETRY_ATTEMPT),
        snapshot.fact_i64(facts::LEGACY_RETRY_BUDGET),
    );
    let (semantic, max, source) = match canonical {
        (Some(semantic), Some(max)) => (semantic, max, "canonical"),
        (None, None) => match legacy {
            (Some(semantic), Some(max)) => (semantic, max, "legacy v1"),
            (None, None) => {
                return Err(ContractError::MissingField(
                    "attempts.semantic / attempts.max_semantic",
                ))
            }
            _ => {
                return Err(ContractError::InvalidValue {
                    field: "attempts",
                    reason: "incomplete legacy accounting: both retry.attempt and retry.budget are required".to_string(),
                })
            }
        },
        _ => {
            return Err(ContractError::InvalidValue {
                field: "attempts",
                reason: "incomplete canonical accounting: both attempts.semantic and attempts.max_semantic are required".to_string(),
            })
        }
    };
    if semantic < 0 || max < 0 {
        return Err(ContractError::InvalidValue {
            field: "attempts",
            reason: format!("{source} semantic accounting must be non-negative ({semantic}/{max})"),
        });
    }
    if semantic > max {
        return Err(ContractError::InvalidValue {
            field: "attempts.semantic",
            reason: format!("{semantic} accepted attempts exceeds the total bound of {max}"),
        });
    }
    let step_retries = snapshot.fact_i64(facts::ATTEMPTS_STEP_RETRIES).unwrap_or(0);
    if step_retries < 0 {
        return Err(ContractError::InvalidValue {
            field: facts::ATTEMPTS_STEP_RETRIES,
            reason: "step retries must be non-negative".to_string(),
        });
    }
    let semantic = u32::try_from(semantic).map_err(|_| ContractError::InvalidValue {
        field: "attempts.semantic",
        reason: "semantic attempts exceed the supported range".to_string(),
    })?;
    let max = u32::try_from(max).map_err(|_| ContractError::InvalidValue {
        field: "attempts.max_semantic",
        reason: "semantic bound exceeds the supported range".to_string(),
    })?;
    let step_retries = u32::try_from(step_retries).map_err(|_| ContractError::InvalidValue {
        field: facts::ATTEMPTS_STEP_RETRIES,
        reason: "step retries exceed the supported range".to_string(),
    })?;
    Ok(AttemptBudget::with_attempts(semantic, max, step_retries))
}

/// Read normalized route constraints from a snapshot.
///
/// A present-but-blank or non-string route fact is ambiguous evidence: it
/// returns an error rather than fabricating a fallback. Absent facts mean "no
/// constraint", not "guess".
pub fn route_constraints_from_snapshot(
    snapshot: &StateSnapshot,
) -> Result<Option<RouteConstraints>, ContractError> {
    let exclude = read_route_fact(snapshot, ROUTE_EXCLUDE_BACKEND)?;
    let preferred = read_route_fact(snapshot, ROUTE_PREFERRED_BACKEND)?;
    let provider = read_route_fact(snapshot, ROUTE_REQUIRED_PROVIDER)?;
    let runtime = read_route_fact(snapshot, ROUTE_REQUIRED_RUNTIME)?;
    let mut constraints = RouteConstraints::new();
    let mut any = false;
    if let Some(value) = exclude {
        constraints = constraints.with_exclude_backend(value);
        any = true;
    }
    if let Some(value) = preferred {
        constraints = constraints.with_preferred_backend(value);
        any = true;
    }
    if let Some(value) = provider {
        constraints = constraints.with_required_provider(value);
        any = true;
    }
    if let Some(value) = runtime {
        constraints = constraints.with_required_runtime(value);
        any = true;
    }
    Ok(any.then_some(constraints))
}

fn read_route_fact(
    snapshot: &StateSnapshot,
    key: &'static str,
) -> Result<Option<String>, ContractError> {
    match snapshot.fact(key) {
        None => Ok(None),
        Some(serde_json::Value::String(value)) if !value.trim().is_empty() => {
            Ok(Some(value.clone()))
        }
        Some(serde_json::Value::String(_)) => Err(ContractError::InvalidValue {
            field: key,
            reason: "route fact is present but blank".to_string(),
        }),
        Some(_) => Err(ContractError::InvalidValue {
            field: key,
            reason: "route fact must be a non-empty string".to_string(),
        }),
    }
}

/// One bounded child description in a [`DecompositionPlan`].
///
/// This is a *description*, not a created work item: Ictus never instantiates
/// it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedChild {
    pub subject_id: String,
    pub capability: String,
}

impl PlannedChild {
    pub fn new(subject_id: impl Into<String>, capability: impl Into<String>) -> Self {
        Self {
            subject_id: subject_id.into(),
            capability: capability.into(),
        }
    }

    pub fn validate(&self) -> Result<(), ContractError> {
        ContractError::require_non_empty("decomposition_plan.child.subject_id", &self.subject_id)?;
        ContractError::require_non_empty("decomposition_plan.child.capability", &self.capability)
    }
}

/// A typed, bounded decomposition plan.
///
/// A `DECOMPOSE` decision requires one of these plus an explicit approval; an
/// absent, empty, oversized or malformed plan escalates. The plan is data only:
/// Ictus creates no child work items from it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecompositionPlan {
    pub plan_id: String,
    pub children: Vec<PlannedChild>,
}

impl DecompositionPlan {
    pub fn new(plan_id: impl Into<String>, children: Vec<PlannedChild>) -> Self {
        Self {
            plan_id: plan_id.into(),
            children,
        }
    }

    pub fn validate(&self) -> Result<(), ContractError> {
        validate_decomposition_plan(self)
    }

    /// Parse a plan from its normalized JSON representation.
    pub fn from_json(value: &serde_json::Value) -> Result<Self, ContractError> {
        let object = value.as_object().ok_or(ContractError::InvalidValue {
            field: "decomposition_plan",
            reason: "must be a JSON object".to_string(),
        })?;
        let plan_id = object
            .get("plan_id")
            .and_then(serde_json::Value::as_str)
            .ok_or(ContractError::MissingField("decomposition_plan.plan_id"))?
            .to_string();
        let children = object
            .get("children")
            .and_then(serde_json::Value::as_array)
            .ok_or(ContractError::MissingField("decomposition_plan.children"))?;
        let mut planned = Vec::with_capacity(children.len());
        for child in children {
            let child = child.as_object().ok_or(ContractError::InvalidValue {
                field: "decomposition_plan.child",
                reason: "must be a JSON object".to_string(),
            })?;
            let subject_id = child
                .get("subject_id")
                .and_then(serde_json::Value::as_str)
                .ok_or(ContractError::MissingField(
                    "decomposition_plan.child.subject_id",
                ))?
                .to_string();
            let capability = child
                .get("capability")
                .and_then(serde_json::Value::as_str)
                .ok_or(ContractError::MissingField(
                    "decomposition_plan.child.capability",
                ))?
                .to_string();
            planned.push(PlannedChild::new(subject_id, capability));
        }
        let plan = Self::new(plan_id, planned);
        plan.validate()?;
        Ok(plan)
    }

    /// Normalized JSON representation (without serde derives).
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "plan_id": self.plan_id,
            "children": self
                .children
                .iter()
                .map(|child| serde_json::json!({
                    "subject_id": child.subject_id,
                    "capability": child.capability,
                }))
                .collect::<Vec<_>>(),
        })
    }
}

/// Validate a typed decomposition plan: non-empty identity, a finite bounded
/// child list and well-formed children.
pub fn validate_decomposition_plan(plan: &DecompositionPlan) -> Result<(), ContractError> {
    ContractError::require_non_empty("decomposition_plan.plan_id", &plan.plan_id)?;
    if plan.children.is_empty() {
        return Err(ContractError::MissingField("decomposition_plan.children"));
    }
    if plan.children.len() > MAX_DECOMPOSITION_CHILDREN {
        return Err(ContractError::InvalidValue {
            field: "decomposition_plan.children",
            reason: format!(
                "a bounded plan may describe at most {MAX_DECOMPOSITION_CHILDREN} children"
            ),
        });
    }
    for child in &plan.children {
        child.validate()?;
    }
    Ok(())
}

/// The bounded recovery outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryOutcome {
    /// The observation reports success: there is no recovery work.
    ///
    /// This is explicitly **not** `ABORT`. It must never be interpreted as
    /// retiring the subject.
    NoRecoveryWork,
    /// Another semantic attempt of the same capability is authorized.
    Reexecute,
    /// A different authorized route is required.
    Route,
    /// The subject should be decomposed under an approved bounded plan.
    Decompose,
    /// Ambiguous, unsupported or exhausted: hand to a human.
    Escalate,
    /// No further action for the subject.
    Abort,
}

impl RecoveryOutcome {
    /// The frozen generic vocabulary token for this outcome, if any.
    pub fn token(self) -> &'static str {
        match self {
            RecoveryOutcome::NoRecoveryWork => "NO_RECOVERY_WORK",
            RecoveryOutcome::Reexecute => "REEXECUTE",
            RecoveryOutcome::Route => "ROUTE",
            RecoveryOutcome::Decompose => "DECOMPOSE",
            RecoveryOutcome::Escalate => "ESCALATE",
            RecoveryOutcome::Abort => "ABORT",
        }
    }

    /// The executable decision kind, if this outcome produces one.
    pub fn decision_kind(self) -> Option<DecisionKind> {
        match self {
            RecoveryOutcome::NoRecoveryWork => None,
            RecoveryOutcome::Reexecute => Some(DecisionKind::Reexecute),
            RecoveryOutcome::Route => Some(DecisionKind::Route),
            RecoveryOutcome::Decompose => Some(DecisionKind::Decompose),
            RecoveryOutcome::Escalate => Some(DecisionKind::Escalate),
            RecoveryOutcome::Abort => Some(DecisionKind::Abort),
        }
    }
}

/// A bounded, validated semantic recovery decision.
#[derive(Debug, Clone, PartialEq)]
pub struct RecoveryDecision {
    pub policy_version: String,
    pub decision_id: String,
    pub subject: Subject,
    /// Correlation with the observation that triggered recovery.
    pub observation_id: String,
    /// Correlation with the failed intent.
    pub intent_id: String,
    pub decided_at: String,
    pub outcome: RecoveryOutcome,
    pub capability: Option<String>,
    pub route: Option<RouteConstraints>,
    pub plan: Option<DecompositionPlan>,
    pub rationale: String,
    pub evidence: Vec<EvidenceRef>,
}

impl RecoveryDecision {
    /// Structural validation: an executable outcome carries a capability and no
    /// plan; a `DECOMPOSE` carries a valid plan and nothing executable; a
    /// non-executable outcome carries no fallback at all.
    pub fn validate(&self) -> Result<(), ContractError> {
        ContractError::require_non_empty("policy_version", &self.policy_version)?;
        ContractError::require_non_empty("decision_id", &self.decision_id)?;
        ContractError::require_non_empty("observation_id", &self.observation_id)?;
        ContractError::require_non_empty("intent_id", &self.intent_id)?;
        ContractError::require_non_empty("rationale", &self.rationale)?;
        match self.outcome {
            RecoveryOutcome::Reexecute | RecoveryOutcome::Route => {
                match self.capability.as_deref() {
                    Some(capability) if !capability.trim().is_empty() => {}
                    _ => {
                        return Err(ContractError::InvalidValue {
                            field: "capability",
                            reason: format!(
                                "recovery outcome {} requires a non-empty capability",
                                self.outcome.token()
                            ),
                        })
                    }
                }
                if self.plan.is_some() {
                    return Err(ContractError::InvalidValue {
                        field: "plan",
                        reason: "a plan is valid only for DECOMPOSE".to_string(),
                    });
                }
                if self.outcome == RecoveryOutcome::Reexecute && self.route.is_some() {
                    return Err(ContractError::InvalidValue {
                        field: "route",
                        reason: "route constraints are valid only for ROUTE".to_string(),
                    });
                }
            }
            RecoveryOutcome::Decompose => {
                if self.capability.is_some() || self.route.is_some() {
                    return Err(ContractError::InvalidValue {
                        field: "outcome",
                        reason: "DECOMPOSE must not carry an executable capability or route"
                            .to_string(),
                    });
                }
                match &self.plan {
                    Some(plan) => plan.validate()?,
                    None => return Err(ContractError::MissingField("plan")),
                }
            }
            RecoveryOutcome::NoRecoveryWork
            | RecoveryOutcome::Escalate
            | RecoveryOutcome::Abort => {
                if self.capability.is_some() || self.route.is_some() || self.plan.is_some() {
                    return Err(ContractError::InvalidValue {
                        field: "outcome",
                        reason: "a non-executable recovery outcome must not carry a fallback"
                            .to_string(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Convert the validated decision into the frozen `DecisionProposal`
    /// vocabulary. `NoRecoveryWork` yields `None`: there is nothing to emit.
    pub fn to_decision_proposal(&self) -> Result<Option<DecisionProposal>, PortError> {
        self.validate()?;
        let Some(kind) = self.outcome.decision_kind() else {
            return Ok(None);
        };
        let mut proposal = DecisionProposal::new(
            format!("proposal:{}", self.decision_id),
            kind,
            self.subject.clone(),
            ProviderMetadata::rules(RECOVERY_PROVIDER_ID).with_version(env!("CARGO_PKG_VERSION")),
            self.decided_at.clone(),
        );
        proposal.reason = Some(self.rationale.clone());
        proposal.evidence = self.evidence.clone();
        if let Some(capability) = &self.capability {
            proposal = proposal.with_capability(capability.clone());
        }
        if let Some(route) = &self.route {
            proposal = proposal.with_route(route.clone());
        }
        if let Some(plan) = &self.plan {
            proposal = proposal
                .with_argument("decomposition_plan", plan.to_json())
                .with_argument("plan_approved", serde_json::Value::Bool(true));
        }
        proposal.arguments.insert(
            "recovery_outcome".to_string(),
            serde_json::Value::String(self.outcome.token().to_string()),
        );
        proposal.validate()?;
        Ok(Some(proposal))
    }
}

/// The normalized recovery input.
pub struct RecoveryRequest<'a> {
    /// A `RECOVERY` context bound to a correlated terminal observation.
    pub context: &'a DecisionContext,
    /// An optional, caller-supplied, typed bounded decomposition plan.
    pub plan: Option<&'a DecompositionPlan>,
    /// The approval token authorizing the plan. `None` or blank is not
    /// approval.
    pub plan_approval: Option<&'a str>,
}

/// The generic bounded semantic recovery provider.
#[derive(Debug, Clone, Copy, Default)]
pub struct GenericRecoveryProvider;

impl GenericRecoveryProvider {
    pub fn new() -> Self {
        Self
    }

    pub fn policy_version(&self) -> &'static str {
        RECOVERY_POLICY_VERSION
    }

    /// Produce one bounded, validated [`RecoveryDecision`].
    pub fn decide(&self, request: RecoveryRequest<'_>) -> Result<RecoveryDecision, PortError> {
        let context = request.context;
        context.validate()?;
        if context.kind != ContextKind::Recovery {
            return Err(PortError::DecisionFailed(
                "semantic recovery requires a RECOVERY context bound to a terminal failure"
                    .to_string(),
            ));
        }
        let binding = context.recovery_binding().ok_or_else(|| {
            PortError::DecisionFailed(
                "recovery context is missing its recovery binding".to_string(),
            )
        })?;
        let observation = &binding.observation;
        let choice = self.choose(context, observation, &request);
        let decision = RecoveryDecision {
            policy_version: RECOVERY_POLICY_VERSION.to_string(),
            decision_id: format!("recovery:{}", observation.observation_id),
            subject: context.snapshot.subject.clone(),
            observation_id: observation.observation_id.clone(),
            intent_id: observation.intent_id.clone(),
            decided_at: context.snapshot.timestamp.clone(),
            outcome: choice.outcome,
            capability: choice.capability,
            route: choice.route,
            plan: choice.plan,
            rationale: choice.rationale,
            evidence: observation.evidence.clone(),
        };
        decision.validate()?;
        Ok(decision)
    }

    fn choose(
        &self,
        context: &DecisionContext,
        observation: &ExecutionObservation,
        request: &RecoveryRequest<'_>,
    ) -> RecoveryChoice {
        let capability = context
            .snapshot
            .fact_str(facts::CAPABILITY_ID)
            .map(str::to_string)
            .filter(|value| !value.trim().is_empty());

        match observation.category {
            // Success is explicitly no recovery work, never ABORT.
            ObservationCategory::Success => RecoveryChoice::new(
                RecoveryOutcome::NoRecoveryWork,
                "observation reports success; there is no recovery work (not an abort)",
            ),
            // Re-executable terminal failures: a new semantic attempt is allowed
            // only while the canonical semantic budget is not exhausted.
            ObservationCategory::WorkerTimeout
            | ObservationCategory::ProcessCrash
            | ObservationCategory::ResourceExhausted => {
                if context.attempts.is_exhausted() {
                    self.exhausted(context)
                } else if let Some(capability) = capability {
                    RecoveryChoice::executable(
                        RecoveryOutcome::Reexecute,
                        capability,
                        format!(
                            "{} within the semantic attempt budget ({} of {} used)",
                            observation_category(observation.category),
                            context.attempts.semantic_attempts,
                            context.attempts.max_semantic_attempts,
                        ),
                    )
                } else {
                    RecoveryChoice::new(
                        RecoveryOutcome::Escalate,
                        "re-executable failure but no capability id to re-execute",
                    )
                }
            }
            // A persistent backend failure may be authorized to another route.
            ObservationCategory::ProviderUnavailable => {
                if context.attempts.is_exhausted() {
                    self.exhausted(context)
                } else if let Some(capability) = capability {
                    match route_constraints_from_snapshot(&context.snapshot) {
                        Ok(route) => RecoveryChoice {
                            outcome: RecoveryOutcome::Route,
                            capability: Some(capability),
                            route,
                            plan: None,
                            rationale:
                                "persistent backend failure; routing to an authorized backend"
                                    .to_string(),
                        },
                        Err(error) => RecoveryChoice::new(
                            RecoveryOutcome::Escalate,
                            format!("ambiguous route evidence: {error}"),
                        ),
                    }
                } else {
                    RecoveryChoice::new(
                        RecoveryOutcome::Escalate,
                        "persistent backend failure but no capability id to route",
                    )
                }
            }
            // Semantic failures require an approved, typed, bounded plan.
            ObservationCategory::VerificationFailure | ObservationCategory::IntegrationConflict => {
                let approved = request
                    .plan_approval
                    .filter(|token| !token.trim().is_empty());
                match (request.plan, approved) {
                    (Some(plan), Some(approval)) => match plan.validate() {
                        Ok(()) => RecoveryChoice {
                            outcome: RecoveryOutcome::Decompose,
                            capability: None,
                            route: None,
                            plan: Some(plan.clone()),
                            rationale: format!(
                                "{} under approved bounded plan '{}' ({approval}); Ictus creates no children",
                                observation_category(observation.category),
                                plan.plan_id,
                            ),
                        },
                        Err(error) => RecoveryChoice::new(
                            RecoveryOutcome::Escalate,
                            format!("unsupported decomposition plan: {error}"),
                        ),
                    },
                    (Some(_), None) => RecoveryChoice::new(
                        RecoveryOutcome::Escalate,
                        "a decomposition plan requires an explicit approval",
                    ),
                    _ => RecoveryChoice::new(
                        RecoveryOutcome::Escalate,
                        "a semantic failure requires a typed bounded decomposition plan and approval",
                    ),
                }
            }
            ObservationCategory::Unknown => RecoveryChoice::new(
                RecoveryOutcome::Escalate,
                "ambiguous or unclassified observation: no safe executable fallback",
            ),
        }
    }

    fn exhausted(&self, context: &DecisionContext) -> RecoveryChoice {
        let abort = context
            .snapshot
            .constraints
            .iter()
            .any(|constraint| constraint == CONSTRAINT_ABORT_ON_EXHAUSTION);
        if abort {
            RecoveryChoice::new(
                RecoveryOutcome::Abort,
                format!(
                    "semantic attempt budget exhausted ({} of {}) and policy selects abort",
                    context.attempts.semantic_attempts, context.attempts.max_semantic_attempts,
                ),
            )
        } else {
            RecoveryChoice::new(
                RecoveryOutcome::Escalate,
                format!(
                    "semantic attempt budget exhausted ({} of {})",
                    context.attempts.semantic_attempts, context.attempts.max_semantic_attempts,
                ),
            )
        }
    }
}

struct RecoveryChoice {
    outcome: RecoveryOutcome,
    capability: Option<String>,
    route: Option<RouteConstraints>,
    plan: Option<DecompositionPlan>,
    rationale: String,
}

impl RecoveryChoice {
    fn new(outcome: RecoveryOutcome, rationale: impl Into<String>) -> Self {
        Self {
            outcome,
            capability: None,
            route: None,
            plan: None,
            rationale: rationale.into(),
        }
    }

    fn executable(
        outcome: RecoveryOutcome,
        capability: String,
        rationale: impl Into<String>,
    ) -> Self {
        Self {
            outcome,
            capability: Some(capability),
            route: None,
            plan: None,
            rationale: rationale.into(),
        }
    }
}

fn observation_category(category: ObservationCategory) -> &'static str {
    match category {
        ObservationCategory::Success => "success",
        ObservationCategory::WorkerTimeout => "WORKER_TIMEOUT",
        ObservationCategory::ProcessCrash => "PROCESS_CRASH",
        ObservationCategory::VerificationFailure => "VERIFICATION_FAILURE",
        ObservationCategory::IntegrationConflict => "INTEGRATION_CONFLICT",
        ObservationCategory::ResourceExhausted => "RESOURCE_EXHAUSTED",
        ObservationCategory::ProviderUnavailable => "PROVIDER_UNAVAILABLE",
        ObservationCategory::Unknown => "UNKNOWN",
    }
}
