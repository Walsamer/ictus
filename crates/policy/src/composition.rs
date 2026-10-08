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

// Small dependency-free SHA-256 implementation. The digest is part of the
// serialized binding contract, so it must be stable across language runtimes.
fn sha256_hex(input: &[u8]) -> String {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let bit_len = (input.len() as u64).wrapping_mul(8);
    let mut bytes = input.to_vec();
    bytes.push(0x80);
    while bytes.len() % 64 != 56 {
        bytes.push(0);
    }
    bytes.extend_from_slice(&bit_len.to_be_bytes());
    let mut h = [
        0x6a09e667u32,
        0xbb67ae85,
        0x3c6ef372,
        0xa54ff53a,
        0x510e527f,
        0x9b05688c,
        0x1f83d9ab,
        0x5be0cd19,
    ];
    for chunk in bytes.chunks_exact(64) {
        let mut w = [0u32; 64];
        for (index, word) in w[..16].iter_mut().enumerate() {
            *word = u32::from_be_bytes(
                chunk[index * 4..index * 4 + 4]
                    .try_into()
                    .expect("chunk word"),
            );
        }
        for index in 16..64 {
            let s0 = w[index - 15].rotate_right(7)
                ^ w[index - 15].rotate_right(18)
                ^ (w[index - 15] >> 3);
            let s1 = w[index - 2].rotate_right(17)
                ^ w[index - 2].rotate_right(19)
                ^ (w[index - 2] >> 10);
            w[index] = w[index - 16]
                .wrapping_add(s0)
                .wrapping_add(w[index - 7])
                .wrapping_add(s1);
        }
        let (mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut x) =
            (h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]);
        for index in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let choice = (e & f) ^ ((!e) & g);
            let temp1 = x
                .wrapping_add(s1)
                .wrapping_add(choice)
                .wrapping_add(K[index])
                .wrapping_add(w[index]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let majority = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(majority);
            x = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(x);
    }
    h.iter().map(|word| format!("{word:08x}")).collect()
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
    use super::sha256_hex;

    #[test]
    fn payload_digest_is_standard_sha256() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
