//! In-memory capability registry.
//!
//! The registry is data (a set of `Capability` records). It answers questions
//! only; it never executes anything.

use agentic_core::Capability;
use agentic_ports::CapabilityRegistry;

/// A deterministic, in-memory capability registry.
#[derive(Debug, Clone, Default)]
pub struct InMemoryCapabilityRegistry {
    capabilities: Vec<Capability>,
}

impl InMemoryCapabilityRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_capabilities(capabilities: Vec<Capability>) -> Self {
        Self { capabilities }
    }

    /// Register a capability. Returns `self` for chaining.
    pub fn with(mut self, capability: Capability) -> Self {
        self.capabilities.push(capability);
        self
    }

    pub fn list(&self) -> &[Capability] {
        &self.capabilities
    }
}

impl CapabilityRegistry for InMemoryCapabilityRegistry {
    fn get(&self, id: &str) -> Option<Capability> {
        self.capabilities
            .iter()
            .find(|capability| capability.id == id)
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentic_core::RiskClass;

    #[test]
    fn registry_finds_known_capability() {
        let registry = InMemoryCapabilityRegistry::new().with(Capability::new(
            "demo.verify",
            "1",
            RiskClass::Low,
        ));
        assert!(registry.contains("demo.verify"));
        assert!(!registry.contains("missing"));
        assert_eq!(registry.get("demo.verify").unwrap().version, "1");
    }
}
