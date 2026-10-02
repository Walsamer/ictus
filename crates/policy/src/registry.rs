//! In-memory capability registry.
//!
//! The registry is data (a set of `Capability` records). It answers questions
//! only; it never executes anything.

use ictus_core::Capability;
use ictus_ports::CapabilityRegistry;

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
    use ictus_core::RiskClass;

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

    #[test]
    fn empty_registry_is_empty() {
        let registry = InMemoryCapabilityRegistry::new();
        assert!(registry.list().is_empty());
        assert!(!registry.contains("anything"));
        assert!(registry.get("anything").is_none());
    }

    #[test]
    fn from_capabilities_and_list_preserve_order() {
        let registry = InMemoryCapabilityRegistry::from_capabilities(vec![
            Capability::new("a", "1", RiskClass::Low),
            Capability::new("b", "1", RiskClass::High),
        ]);
        assert_eq!(
            registry
                .list()
                .iter()
                .map(|c| c.id.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        assert_eq!(registry.get("b").unwrap().risk_class, RiskClass::High);
    }
}
