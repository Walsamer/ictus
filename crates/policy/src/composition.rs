//! Trusted local composition of context, proposal, registry and domain grants.
//!
//! The public token approval providers are deliberately retained for generic
//! demos. They are not accepted here. This boundary accepts only domain
//! [`ApprovalGrant`] records and returns an opaque [`TrustedDecision`]. A
//! deserialized `DecisionEnvelope`, or a caller-created `ALLOW`, is audit data
//! and cannot become a `TrustedDecision`.

use ictus_core::{
    ApprovalBinding, ApprovalGrant, CapabilityValidation, DecisionContext, DecisionEnvelope,
    DecisionProposal, EnvelopeOutcome, PolicyBinding, PolicyDecisionKind, ProposalBinding,
    SnapshotRef, ValidationBinding,
};
use ictus_ports::{ApprovalProvider, CapabilityRegistry, PolicyEvaluator, PortError};

use crate::{
    validate_capability, validate_proposal, DefaultPolicyEvaluator, TokenApprovalProvider,
};

/// Stable identifier of this implementation's policy semantics.
pub const DEFAULT_POLICY_VERSION: &str = "ictus.default.v1";

/// The inputs that are composed at the trusted local boundary.
pub struct ValidationRequest<'a> {
    pub context: &'a DecisionContext,
    pub snapshot: SnapshotRef,
    pub proposal: &'a DecisionProposal,
    pub capabilities: &'a dyn CapabilityRegistry,
    pub grants: &'a [ApprovalGrant],
    /// Expiry selected by the caller's bounded protocol; it must be later than
    /// the deterministic validation time (the proposal timestamp).
    pub expiry: &'a str,
}

/// An envelope produced in-process by [`LocalPolicyComposition`].
///
/// Its inner envelope is intentionally private. Consumers may serialize it for
/// transport/audit or borrow the executable intent, but cannot turn arbitrary
/// JSON or a self-asserted `ALLOW` into this capability-bearing type.
#[derive(Debug, Clone)]
pub struct TrustedDecision {
    envelope: DecisionEnvelope,
}

impl TrustedDecision {
    pub fn envelope(&self) -> &DecisionEnvelope {
        &self.envelope
    }

    pub fn executable_intent(&self) -> Option<&ictus_core::ExecutionIntent> {
        self.envelope
            .is_executable()
            .then_some(self.envelope.intent.as_ref())
            .flatten()
    }

    /// Serialize the validated audit/result contract. Deserialization produces
    /// an untrusted `DecisionEnvelope`, never this local authority token.
    pub fn to_json(&self) -> Result<String, PortError> {
        serde_json::to_string(&self.envelope)
            .map_err(|error| PortError::Serialization(error.to_string()))
    }
}

/// The only production composition path for the default policy.
#[derive(Debug, Clone)]
pub struct LocalPolicyComposition {
    policy_version: String,
}

impl Default for LocalPolicyComposition {
    fn default() -> Self {
        Self::new(DEFAULT_POLICY_VERSION)
    }
}

impl LocalPolicyComposition {
    pub fn new(policy_version: impl Into<String>) -> Self {
        Self {
            policy_version: policy_version.into(),
        }
    }

    pub fn policy_version(&self) -> &str {
        &self.policy_version
    }

    pub fn validate(&self, request: ValidationRequest<'_>) -> Result<TrustedDecision, PortError> {
        let proposal_digest = proposal_digest(request.proposal)?;
        let validated_at = request.proposal.proposed_at.clone();
        let capability_id = request.proposal.capability.clone().unwrap_or_default();
        let capability_version = request
            .capabilities
            .get(&capability_id)
            .map(|capability| capability.version)
            .unwrap_or_else(|| "unregistered".to_string());
        let validation = ValidationBinding::new(
            request.context.context_id.clone(),
            proposal_digest.clone(),
            capability_version,
            validated_at.clone(),
        );

        if self.policy_version != DEFAULT_POLICY_VERSION {
            return Ok(self.denied(
                &request,
                validation,
                format!("unknown policy version '{}'", self.policy_version),
            ));
        }
        if let Err(error) = request.context.validate() {
            return Ok(self.invalid(&request, validation, format!("invalid context: {error}")));
        }
        if request.snapshot.snapshot_id != request.context.snapshot.snapshot_id
            || request.snapshot.revision != request.context.revision
            || request.proposal.subject != request.context.snapshot.subject
        {
            return Ok(self.invalid(
                &request,
                validation,
                "context, snapshot and proposal subject binding mismatch".to_string(),
            ));
        }
        if !canonical_utc(request.expiry) || request.expiry <= validated_at.as_str() {
            return Ok(self.invalid(
                &request,
                validation,
                "expiry must be canonical UTC and later than validation time".to_string(),
            ));
        }

        let capability = request.capabilities.get(&capability_id);
        let required = capability
            .as_ref()
            .map(required_approvals)
            .unwrap_or_default();
        let (qualified, grant_reasons): (Vec<_>, Vec<_>) = request
            .grants
            .iter()
            .cloned()
            .map(|grant| {
                match grant_reason(
                    &grant,
                    request.context,
                    &capability_id,
                    &self.policy_version,
                    &validated_at,
                ) {
                    Ok(()) => Ok(grant),
                    Err(reason) => Err(format!("grant '{}' rejected: {reason}", grant.grant_id)),
                }
            })
            .partition(Result::is_ok);
        let qualified: Vec<ApprovalGrant> = qualified.into_iter().filter_map(Result::ok).collect();
        let grant_reasons: Vec<String> =
            grant_reasons.into_iter().filter_map(Result::err).collect();
        let approvals = TokenApprovalProvider::new(
            qualified
                .iter()
                .map(|grant| grant.approval.clone())
                .collect(),
        );

        let policy = DefaultPolicyEvaluator::new().evaluate(
            &request.context.snapshot,
            request.proposal,
            request.capabilities,
            &approvals,
        )?;
        let admitted = capability.is_some();
        let permitted = matches!(
            policy.decision,
            PolicyDecisionKind::Allow | PolicyDecisionKind::Modify
        );
        let capability_validation = CapabilityValidation::new(
            capability_id.clone(),
            admitted,
            permitted,
            if policy.reasons.is_empty() {
                "validated".to_string()
            } else {
                policy.reasons.join("; ")
            },
        );
        let approval_bindings = required
            .iter()
            .map(|name| {
                ApprovalBinding::new(name, approvals.is_approved(std::slice::from_ref(name)))
            })
            .collect();
        let mut envelope = DecisionEnvelope {
            schema_version: ictus_core::ENVELOPE_SCHEMA_VERSION,
            envelope_id: format!("envelope:{}", request.proposal.proposal_id),
            subject: request.context.snapshot.subject.clone(),
            snapshot: request.snapshot,
            proposal: ProposalBinding::new(
                request.proposal.proposal_id.clone(),
                request.proposal.schema_version,
                request.proposal.decision,
            ),
            policy: PolicyBinding::new(
                policy.policy_decision_id,
                &self.policy_version,
                policy.decision,
            ),
            capability_validation,
            route: request.proposal.route.clone(),
            approvals: approval_bindings,
            grants: qualified,
            validation,
            outcome: EnvelopeOutcome::from_verdict(policy.decision, true),
            expiry: request.expiry.to_string(),
            intent: None,
        };
        if envelope.outcome.is_executable() {
            if let Some(mut intent) = policy.modified_intent {
                intent.policy_context.insert(
                    "context_id".to_string(),
                    serde_json::json!(request.context.context_id),
                );
                intent.policy_context.insert(
                    "snapshot_revision".to_string(),
                    serde_json::json!(request.context.revision),
                );
                intent.policy_context.insert(
                    "snapshot_digest".to_string(),
                    serde_json::json!(envelope.snapshot.digest),
                );
                intent.policy_context.insert(
                    "policy_version".to_string(),
                    serde_json::json!(self.policy_version),
                );
                intent.policy_context.insert(
                    "proposal_digest".to_string(),
                    serde_json::json!(proposal_digest),
                );
                envelope.intent = Some(intent);
            }
        }
        if !grant_reasons.is_empty() {
            envelope.capability_validation.reason = format!(
                "{}; {}",
                envelope.capability_validation.reason,
                grant_reasons.join("; ")
            );
        }
        // Non-executing generic decisions are valid policy results but cannot
        // be represented as an executable intent contract.
        if matches!(
            policy.decision,
            PolicyDecisionKind::Allow | PolicyDecisionKind::Modify
        ) && envelope.intent.is_none()
        {
            envelope.outcome = EnvelopeOutcome::Invalid;
            envelope.capability_validation.permitted = false;
        }
        envelope.validate()?;
        Ok(TrustedDecision { envelope })
    }

    fn denied(
        &self,
        request: &ValidationRequest<'_>,
        validation: ValidationBinding,
        reason: String,
    ) -> TrustedDecision {
        self.result(
            request,
            validation,
            EnvelopeOutcome::Denied,
            PolicyDecisionKind::Deny,
            false,
            reason,
        )
    }

    fn invalid(
        &self,
        request: &ValidationRequest<'_>,
        validation: ValidationBinding,
        reason: String,
    ) -> TrustedDecision {
        self.result(
            request,
            validation,
            EnvelopeOutcome::Invalid,
            PolicyDecisionKind::Deny,
            false,
            reason,
        )
    }

    fn result(
        &self,
        request: &ValidationRequest<'_>,
        validation: ValidationBinding,
        outcome: EnvelopeOutcome,
        verdict: PolicyDecisionKind,
        admitted: bool,
        reason: String,
    ) -> TrustedDecision {
        TrustedDecision {
            envelope: DecisionEnvelope {
                schema_version: ictus_core::ENVELOPE_SCHEMA_VERSION,
                envelope_id: format!("envelope:{}", request.proposal.proposal_id),
                subject: request.context.snapshot.subject.clone(),
                snapshot: request.snapshot.clone(),
                proposal: ProposalBinding::new(
                    request.proposal.proposal_id.clone(),
                    request.proposal.schema_version,
                    request.proposal.decision,
                ),
                policy: PolicyBinding::new(
                    format!("policy:{}", request.proposal.proposal_id),
                    &self.policy_version,
                    verdict,
                ),
                capability_validation: CapabilityValidation::new(
                    request
                        .proposal
                        .capability
                        .clone()
                        .unwrap_or_else(|| "none".to_string()),
                    admitted,
                    false,
                    reason,
                ),
                route: request.proposal.route.clone(),
                approvals: Vec::new(),
                grants: Vec::new(),
                validation,
                outcome,
                expiry: request.expiry.to_string(),
                intent: None,
            },
        }
    }
}

/// Revalidate a rewritten `MODIFY` intent against the exact candidate payload.
///
/// A caller that receives a `MODIFY` from another policy implementation must
/// invoke this before wrapping the result in a trusted decision. The digest,
/// subject, capability and current registry entry must all still agree; a
/// copied digest from the pre-modification payload fails closed.
pub fn revalidate_modified_intent(
    proposal: &DecisionProposal,
    intent: &ictus_core::ExecutionIntent,
    capabilities: &dyn CapabilityRegistry,
) -> Result<(), PortError> {
    validate_proposal(proposal)?;
    intent.validate()?;
    let capability_id = proposal.capability.as_deref().ok_or_else(|| {
        PortError::PolicyFailed("modified proposal has no capability".to_string())
    })?;
    let capability = capabilities.get(capability_id).ok_or_else(|| {
        PortError::PolicyFailed(format!("capability '{capability_id}' is not registered"))
    })?;
    validate_capability(&capability)?;
    if intent.capability != capability.id || intent.target != proposal.subject {
        return Err(PortError::PolicyFailed(
            "modified intent does not bind the proposal capability and subject".to_string(),
        ));
    }
    let digest = proposal_digest(proposal)?;
    if intent
        .policy_context
        .get("proposal_digest")
        .and_then(serde_json::Value::as_str)
        != Some(digest.as_str())
    {
        return Err(PortError::PolicyFailed(
            "modified intent was not revalidated for the exact proposal payload".to_string(),
        ));
    }
    Ok(())
}

fn required_approvals(capability: &ictus_core::Capability) -> Vec<String> {
    if !capability.required_approvals.is_empty() {
        capability.required_approvals.clone()
    } else if capability.risk_class == ictus_core::RiskClass::High {
        vec!["human".to_string()]
    } else {
        Vec::new()
    }
}

fn proposal_digest(proposal: &DecisionProposal) -> Result<String, PortError> {
    let bytes = serde_json::to_vec(proposal)
        .map_err(|error| PortError::Serialization(error.to_string()))?;
    Ok(sha256_hex(&bytes))
}

/// Lowercase hex SHA-256 of `input`.
///
/// The digest is part of the serialized binding contract and must match every
/// other runtime (for example Python `hashlib.sha256`); the standard `sha2`
/// crate provides that interoperability without a hand-rolled primitive whose
/// equivalent boolean identities are impossible to cover by tests.
fn sha256_hex(input: &[u8]) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(input);
    format!("{:x}", hasher.finalize())
}

fn canonical_utc(value: &str) -> bool {
    value.len() == 20
        && value.ends_with('Z')
        && value.as_bytes().get(4) == Some(&b'-')
        && value.as_bytes().get(7) == Some(&b'-')
        && value.as_bytes().get(10) == Some(&b'T')
        && value.as_bytes().get(13) == Some(&b':')
        && value.as_bytes().get(16) == Some(&b':')
        && value.bytes().enumerate().all(|(index, byte)| {
            matches!(index, 4 | 7 | 10 | 13 | 16 | 19) || byte.is_ascii_digit()
        })
}

fn grant_reason(
    grant: &ApprovalGrant,
    context: &DecisionContext,
    capability: &str,
    policy_version: &str,
    validated_at: &str,
) -> Result<(), String> {
    grant.validate().map_err(|error| error.to_string())?;
    if grant.revoked {
        return Err("revoked".to_string());
    }
    if !canonical_utc(&grant.issued_at) || !canonical_utc(&grant.expires_at) {
        return Err("timestamps must be canonical UTC".to_string());
    }
    if grant.issued_at.as_str() > validated_at {
        return Err("issued after validation time".to_string());
    }
    if grant.expires_at.as_str() <= validated_at {
        return Err("expired".to_string());
    }
    if grant.subject != context.snapshot.subject {
        return Err("wrong subject".to_string());
    }
    if grant.revision != context.revision {
        return Err("wrong revision".to_string());
    }
    if grant.capability != capability {
        return Err("wrong capability".to_string());
    }
    if grant.policy_version != policy_version {
        return Err("wrong policy version".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{canonical_utc, sha256_hex};

    #[test]
    fn payload_digest_is_standard_sha256() {
        // Standard FIPS 180-4 vectors, plus padding-boundary and multi-block
        // inputs so every branch of the compression/expansion is exercised.
        for (input, expected) in [
            (
                &b""[..],
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ),
            (
                &b"abc"[..],
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            ),
            (
                &b"a"[..],
                "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb",
            ),
            (
                &b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"[..],
                "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1",
            ),
        ] {
            assert_eq!(sha256_hex(input), expected);
        }
    }

    #[test]
    fn sha256_padding_boundaries_and_high_bit_inputs() {
        let cases: [(Vec<u8>, &str); 5] = [
            (
                vec![b'a'; 55],
                "9f4390f8d30c2dd92ec9f095b65e2b9ae9b0a925a5258e241c9f1e910f734318",
            ),
            (
                vec![b'a'; 56],
                "b35439a4ac6f0948b6d6f9e3c6af0f5f590ce20f1bde7090ef7970686ec6738a",
            ),
            (
                vec![b'a'; 64],
                "ffe054fe7ae0cb6dc65c3af9b61d5209f439851db43d0ba5997337df154668eb",
            ),
            (
                (0u8..64).collect(),
                "fdeab9acf3710362bd2658cdc9a29e8f9c757fcf9811603a8c447cd1d9151108",
            ),
            (
                (1u8..66).collect(),
                "47e9b736f9001fab14640d9c915574c20669621520371f05a4b50906020dc49f",
            ),
        ];
        for (input, expected) in cases {
            assert_eq!(sha256_hex(&input), expected);
        }
    }

    #[test]
    fn canonical_utc_accepts_only_the_exact_shape() {
        assert!(canonical_utc("2026-10-01T00:00:00Z"));

        // Each case falsifies exactly one conjunct of the canonical check, so a
        // weakened `&&` cannot accept it.
        for invalid in [
            "",
            "not-a-timestamp",
            "2026-10-01T00:00:00X",  // wrong terminator
            "2026X10-01T00:00:00Z",  // index 4
            "2026-10X01T00:00:00Z",  // index 7
            "2026-10-01X00:00:00Z",  // index 10
            "2026-10-01T00X00:00Z",  // index 13
            "2026-10-01T00:00X00Z",  // index 16
            "2026-10-01T00:00:0Z",   // too short
            "2026-10-01T00:00:00ZZ", // too long
            "2026-10-01T00:00:00",   // no terminator
            "2026-10-01T00:00:00z",  // lowercase terminator
        ] {
            assert!(!canonical_utc(invalid), "accepted {invalid:?}");
        }
    }
}
