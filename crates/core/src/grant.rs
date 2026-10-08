//! Bounded approval-grant evidence supplied by the domain.
//!
//! Ictus does not persist grants.  A domain system records them and supplies a
//! bounded record for one validation request.  Unlike the legacy demo approval
//! tokens, a grant is bound to one subject, revision, capability and policy.

use serde::{Deserialize, Serialize};

use crate::{ContractError, EvidenceRef, Subject};

/// Domain-provided evidence that one named approval is valid for one action.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApprovalGrant {
    pub grant_id: String,
    pub approval: String,
    pub subject: Subject,
    pub revision: u64,
    pub capability: String,
    pub policy_version: String,
    pub issued_at: String,
    pub expires_at: String,
    #[serde(default)]
    pub revoked: bool,
    /// Opaque reference to the domain approval/intervention record.
    #[serde(default)]
    pub evidence: Vec<EvidenceRef>,
}

impl ApprovalGrant {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        grant_id: impl Into<String>,
        approval: impl Into<String>,
        subject: Subject,
        revision: u64,
        capability: impl Into<String>,
        policy_version: impl Into<String>,
        issued_at: impl Into<String>,
        expires_at: impl Into<String>,
    ) -> Self {
        Self {
            grant_id: grant_id.into(),
            approval: approval.into(),
            subject,
            revision,
            capability: capability.into(),
            policy_version: policy_version.into(),
            issued_at: issued_at.into(),
            expires_at: expires_at.into(),
            revoked: false,
            evidence: Vec::new(),
        }
    }

    pub fn with_evidence(mut self, evidence: EvidenceRef) -> Self {
        self.evidence.push(evidence);
        self
    }

    pub fn revoked(mut self) -> Self {
        self.revoked = true;
        self
    }

    pub fn validate(&self) -> Result<(), ContractError> {
        for (field, value) in [
            ("grant.grant_id", &self.grant_id),
            ("grant.approval", &self.approval),
            ("grant.capability", &self.capability),
            ("grant.policy_version", &self.policy_version),
            ("grant.issued_at", &self.issued_at),
            ("grant.expires_at", &self.expires_at),
        ] {
            ContractError::require_non_empty(field, value)?;
        }
        ContractError::require_non_empty("grant.subject.kind", &self.subject.kind)?;
        ContractError::require_non_empty("grant.subject.id", &self.subject.id)?;
        if self.approval == "*" {
            return Err(ContractError::InvalidValue {
                field: "grant.approval",
                reason: "wildcard approvals are demo-only and cannot be domain grants".to_string(),
            });
        }
        if self.evidence.is_empty() {
            return Err(ContractError::InvalidValue {
                field: "grant.evidence",
                reason: "a domain grant requires at least one evidence reference".to_string(),
            });
        }
        Ok(())
    }
}
