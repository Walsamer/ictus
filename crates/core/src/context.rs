//! Versioned initial/recovery decision context.
//!
//! A [`DecisionContext`] is the **input** side of the generic interchange a
//! consumer such as Tactus uses: the normalized [`StateSnapshot`], the subject
//! revision, the semantic attempt budget, generic facts and grants. It carries
//! no domain entity and no durable workflow state. It is a versioned wire
//! contract of its own (`schema_version` is per-contract).
//!
//! Two shapes share one contract:
//!
//! - an **initial** context starts a fresh subject. It must not bind or
//!   fabricate an observation: there is nothing observed yet.
//! - a **recovery** context resumes after a failed attempt. It binds the failed
//!   attempt, the failed intent and the snapshot that attempt was decided from.

use serde::{Deserialize, Serialize};

use crate::observation::ExecutionObservation;
use crate::snapshot::StateSnapshot;
use crate::version::{check_contract_version, ContractError, CONTEXT_SCHEMA_VERSION};

/// The dotted, domain-neutral fact-key namespace shared with a consumer such as
/// Tactus.
///
/// Each name is a *role* in the decision input, never a domain entity. The core
/// knows only these keys; a domain adapter supplies the values.
pub mod facts {
    /// Monotonic subject revision the snapshot was taken at.
    pub const SUBJECT_REVISION: &str = "subject.revision";
    /// Accepted semantic attempts, including the attempt that just failed.
    pub const ATTEMPTS_SEMANTIC: &str = "attempts.semantic";
    /// Total semantic attempt bound.
    pub const ATTEMPTS_MAX_SEMANTIC: &str = "attempts.max_semantic";
    /// Execution-backend step retries; separate from the semantic budget.
    pub const ATTEMPTS_STEP_RETRIES: &str = "attempts.step_retries";
    /// Generic observation category. Present only for a recovery context.
    pub const OBSERVATION_CATEGORY: &str = "observation.category";
    /// Capability under consideration.
    pub const CAPABILITY_ID: &str = "capability.id";
    /// Legacy v1 semantic attempt key (`retry.attempt`), still accepted.
    pub const LEGACY_RETRY_ATTEMPT: &str = "retry.attempt";
    /// Legacy v1 semantic budget key (`retry.budget`), still accepted.
    pub const LEGACY_RETRY_BUDGET: &str = "retry.budget";
}

/// Whether a context starts a fresh subject or resumes after a failed attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ContextKind {
    /// A first attempt. There is no observation to bind and no fabricated one.
    Initial,
    /// A subsequent attempt after a failed one; binds the failure.
    Recovery,
}

/// A snapshot bound by identity, subject revision and content digest.
///
/// The digest lets a consumer detect that the underlying snapshot changed
/// between reading and deciding (a profile/subject mismatch).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotRef {
    pub snapshot_id: String,
    /// Monotonic revision of the subject the snapshot was taken at.
    pub revision: u64,
    /// Content digest of the canonical snapshot (e.g. a sha256 hex string).
    pub digest: String,
}

impl SnapshotRef {
    pub fn new(snapshot_id: impl Into<String>, revision: u64, digest: impl Into<String>) -> Self {
        Self {
            snapshot_id: snapshot_id.into(),
            revision,
            digest: digest.into(),
        }
    }

    pub fn validate(&self) -> Result<(), ContractError> {
        ContractError::require_non_empty("snapshot_ref.snapshot_id", &self.snapshot_id)?;
        ContractError::require_non_empty("snapshot_ref.digest", &self.digest)?;
        Ok(())
    }
}

/// Semantic attempt accounting.
///
/// `semantic_attempts` counts **accepted** attempts, including the attempt that
/// just failed: a first failure is `1`. `max_semantic_attempts` is the total
/// bound. `step_retries` is the execution backend's own step-retry counter and
/// is deliberately separate: it never consumes the semantic budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptBudget {
    /// Accepted semantic attempts, including the failed attempt.
    pub semantic_attempts: u32,
    /// Total bound on semantic attempts.
    pub max_semantic_attempts: u32,
    /// Execution-backend step retries; separate from the semantic budget.
    pub step_retries: u32,
}

impl AttemptBudget {
    /// A fresh budget with no attempts yet recorded.
    pub fn new(max_semantic_attempts: u32) -> Self {
        Self {
            semantic_attempts: 0,
            max_semantic_attempts,
            step_retries: 0,
        }
    }

    /// Explicit constructor for fixtures and recovery state.
    pub fn with_attempts(
        semantic_attempts: u32,
        max_semantic_attempts: u32,
        step_retries: u32,
    ) -> Self {
        Self {
            semantic_attempts,
            max_semantic_attempts,
            step_retries,
        }
    }

    /// Semantic attempts still available.
    pub fn remaining(&self) -> u32 {
        self.max_semantic_attempts
            .saturating_sub(self.semantic_attempts)
    }

    /// True when the total semantic bound has been reached.
    pub fn is_exhausted(&self) -> bool {
        self.semantic_attempts >= self.max_semantic_attempts
    }

    /// Accept one more semantic attempt, failing closed at the total bound.
    pub fn accept_attempt(&mut self) -> Result<(), ContractError> {
        if self.is_exhausted() {
            return Err(ContractError::InvalidValue {
                field: "attempts.semantic_attempts",
                reason: "semantic attempt bound exhausted".to_string(),
            });
        }
        self.semantic_attempts += 1;
        Ok(())
    }

    /// Record an execution-backend step retry. Never touches the semantic count.
    pub fn record_step_retry(&mut self) {
        self.step_retries += 1;
    }

    pub fn validate(&self) -> Result<(), ContractError> {
        if self.semantic_attempts > self.max_semantic_attempts {
            return Err(ContractError::InvalidValue {
                field: "attempts.semantic_attempts",
                reason: format!(
                    "{} accepted attempts exceeds the total bound of {}",
                    self.semantic_attempts, self.max_semantic_attempts
                ),
            });
        }
        Ok(())
    }
}

/// Binds the failed attempt a recovery context resumes from.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecoveryBinding {
    /// Identity of the accepted semantic attempt that failed.
    pub failed_attempt_id: String,
    /// Identity of the intent that was executed and failed.
    pub failed_intent_id: String,
    /// The snapshot the failed attempt was decided from.
    pub failed_snapshot: SnapshotRef,
    /// The generic fact the execution produced.
    pub observation: ExecutionObservation,
}

impl RecoveryBinding {
    pub fn new(
        failed_attempt_id: impl Into<String>,
        failed_intent_id: impl Into<String>,
        failed_snapshot: SnapshotRef,
        observation: ExecutionObservation,
    ) -> Self {
        Self {
            failed_attempt_id: failed_attempt_id.into(),
            failed_intent_id: failed_intent_id.into(),
            failed_snapshot,
            observation,
        }
    }

    pub fn validate(&self) -> Result<(), ContractError> {
        ContractError::require_non_empty("recovery.failed_attempt_id", &self.failed_attempt_id)?;
        ContractError::require_non_empty("recovery.failed_intent_id", &self.failed_intent_id)?;
        self.failed_snapshot.validate()?;
        self.observation.validate()?;
        Ok(())
    }
}

/// Versioned initial/recovery context: the normalized snapshot plus the
/// generic accounting a decision provider needs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionContext {
    pub schema_version: u32,
    pub context_id: String,
    pub kind: ContextKind,
    /// Generic fact-key profile id (e.g. `tactus.generic.v1`). A consumer uses
    /// it to detect a profile mismatch; the core never imports the profile's
    /// domain.
    pub profile: String,
    pub snapshot: StateSnapshot,
    /// Monotonic subject revision this context is valid for.
    pub revision: u64,
    pub attempts: AttemptBudget,
    /// Capability ids / approval tokens granted for this subject.
    #[serde(default)]
    pub grants: Vec<String>,
    /// Present if and only if `kind` is [`ContextKind::Recovery`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery: Option<RecoveryBinding>,
}

impl DecisionContext {
    /// An initial context: no recovery binding, no observation.
    pub fn initial(
        context_id: impl Into<String>,
        profile: impl Into<String>,
        snapshot: StateSnapshot,
        revision: u64,
        attempts: AttemptBudget,
    ) -> Self {
        Self {
            schema_version: CONTEXT_SCHEMA_VERSION,
            context_id: context_id.into(),
            kind: ContextKind::Initial,
            profile: profile.into(),
            snapshot,
            revision,
            attempts,
            grants: Vec::new(),
            recovery: None,
        }
    }

    /// A recovery context: binds the failed attempt and snapshot.
    pub fn recovery(
        context_id: impl Into<String>,
        profile: impl Into<String>,
        snapshot: StateSnapshot,
        revision: u64,
        attempts: AttemptBudget,
        recovery: RecoveryBinding,
    ) -> Self {
        Self {
            schema_version: CONTEXT_SCHEMA_VERSION,
            context_id: context_id.into(),
            kind: ContextKind::Recovery,
            profile: profile.into(),
            snapshot,
            revision,
            attempts,
            grants: Vec::new(),
            recovery: Some(recovery),
        }
    }

    pub fn with_grant(mut self, grant: impl Into<String>) -> Self {
        self.grants.push(grant.into());
        self
    }

    pub fn with_attempts(mut self, attempts: AttemptBudget) -> Self {
        self.attempts = attempts;
        self
    }

    pub fn is_initial(&self) -> bool {
        self.kind == ContextKind::Initial
    }

    pub fn recovery_binding(&self) -> Option<&RecoveryBinding> {
        self.recovery.as_ref()
    }

    /// Structural and profile validation. Fails closed on a missing or invalid
    /// attempt budget, an unknown kind, a malformed fact key, or an
    /// initial/recovery shape mismatch.
    pub fn validate(&self) -> Result<(), ContractError> {
        check_contract_version(self.schema_version, CONTEXT_SCHEMA_VERSION)?;
        ContractError::require_non_empty("context_id", &self.context_id)?;
        self.snapshot.validate()?;
        self.attempts.validate()?;
        if self.profile.trim().is_empty() {
            return Err(ContractError::MissingField("profile"));
        }
        if self.profile.chars().any(char::is_whitespace) {
            return Err(ContractError::InvalidValue {
                field: "profile",
                reason: "the fact profile id must not contain whitespace".to_string(),
            });
        }
        // Profile mismatch: every fact key must be a dotted, role-based key.
        for fact in &self.snapshot.facts {
            if !fact.key.contains('.') {
                return Err(ContractError::InvalidValue {
                    field: "facts.key",
                    reason: format!("fact key {:?} is not part of the dotted profile", fact.key),
                });
            }
        }
        for grant in &self.grants {
            if grant.trim().is_empty() {
                return Err(ContractError::MissingField("grants"));
            }
        }
        match self.kind {
            ContextKind::Initial => {
                if self.recovery.is_some() {
                    return Err(ContractError::InvalidValue {
                        field: "recovery",
                        reason: "an initial context must not bind a recovery".to_string(),
                    });
                }
                // No fabricated observation on an initial context.
                if self
                    .snapshot
                    .facts
                    .iter()
                    .any(|fact| fact.key.starts_with("observation."))
                {
                    return Err(ContractError::InvalidValue {
                        field: "facts",
                        reason: "an initial context must not carry an observation".to_string(),
                    });
                }
            }
            ContextKind::Recovery => match &self.recovery {
                Some(binding) => binding.validate()?,
                None => {
                    return Err(ContractError::MissingField("recovery"));
                }
            },
        }
        Ok(())
    }
}
