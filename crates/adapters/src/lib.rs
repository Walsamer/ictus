//! Generic, non-Dagster adapters for the Ictus ports.
//!
//! These adapters are domain-independent and transport-independent: a
//! file-backed evidence store and a file-backed approval provider. They hold no
//! policy authority — they answer the port questions and fail closed.

pub mod approval;
pub mod error;
pub mod evidence;

pub use approval::JsonFileApprovalProvider;
pub use error::AdapterError;
pub use evidence::JsonlEvidenceStore;
