//! Integration tests for the generic adapters (file-backed approval + evidence).

use ictus_adapters::{AdapterError, JsonFileApprovalProvider, JsonlEvidenceStore};
use ictus_core::EvidenceRef;
use ictus_ports::{ApprovalProvider, EvidenceStore};

fn write(path: &std::path::Path, contents: &str) {
    std::fs::write(path, contents).unwrap();
}

// -- approval provider ------------------------------------------------------

#[test]
fn loads_a_token_array() {
    let dir = std::env::temp_dir().join(format!("ictus-approvals-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("approvals.json");
    write(&path, r#"["human"]"#);

    let provider = JsonFileApprovalProvider::load(&path).unwrap();
    assert_eq!(provider.tokens(), ["human".to_string()]);
    assert_eq!(provider.satisfied_approvals(), vec!["human".to_string()]);
    assert!(provider.is_approved(&["human".to_string()]));
    assert!(!provider.is_approved(&["security".to_string()]));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn from_tokens_preserves_tokens_and_reports_them() {
    let provider = JsonFileApprovalProvider::from_tokens(vec!["human".to_string()]);
    assert_eq!(provider.tokens(), ["human".to_string()]);
    assert_eq!(provider.satisfied_approvals(), vec!["human".to_string()]);
    assert!(provider.is_approved(&["human".to_string()]));
    assert!(!provider.is_approved(&["security".to_string()]));
    assert!(!provider.is_approved(&["human".to_string(), "security".to_string()]));
}

#[test]
fn loads_an_object_with_wildcard() {
    let dir = std::env::temp_dir().join(format!("ictus-approvals-obj-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("approvals.json");
    write(&path, r#"{"satisfied": [], "wildcard": true}"#);

    let provider = JsonFileApprovalProvider::load(&path).unwrap();
    assert!(provider.is_approved(&["anything".to_string()]));
    assert_eq!(provider.satisfied_approvals(), vec!["*".to_string()]);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn an_empty_requirement_is_approved() {
    let provider = JsonFileApprovalProvider::from_tokens(vec![]);
    assert!(provider.is_approved(&[]));
    assert!(!provider.is_approved(&["human".to_string()]));
}

#[test]
fn missing_file_fails_closed() {
    let error = JsonFileApprovalProvider::load("/nonexistent/ictus/approvals.json").unwrap_err();
    assert!(
        matches!(error, AdapterError::Read { .. }),
        "unexpected: {error}"
    );
}

#[test]
fn invalid_json_fails_closed() {
    let dir = std::env::temp_dir().join(format!("ictus-approvals-bad-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("approvals.json");
    write(&path, "{not json");

    let error = JsonFileApprovalProvider::load(&path).unwrap_err();
    assert!(
        matches!(error, AdapterError::Json { .. }),
        "unexpected: {error}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn wrong_shape_and_non_string_tokens_fail_closed() {
    let dir = std::env::temp_dir().join(format!("ictus-approvals-shape-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let object_with_scalar = dir.join("scalar.json");
    write(&object_with_scalar, r#"{"satisfied": "human"}"#);
    assert!(matches!(
        JsonFileApprovalProvider::load(&object_with_scalar).unwrap_err(),
        AdapterError::Shape { .. }
    ));

    let number = dir.join("number.json");
    write(&number, "42");
    assert!(matches!(
        JsonFileApprovalProvider::load(&number).unwrap_err(),
        AdapterError::Shape { .. }
    ));

    let mixed = dir.join("mixed.json");
    write(&mixed, r#"["human", 3]"#);
    assert!(matches!(
        JsonFileApprovalProvider::load(&mixed).unwrap_err(),
        AdapterError::Shape { .. }
    ));

    let _ = std::fs::remove_dir_all(&dir);
}

// -- evidence store ---------------------------------------------------------

#[test]
fn appends_evidence_as_jsonl_and_reads_it_back() {
    let dir = std::env::temp_dir().join(format!("ictus-evidence-{}", std::process::id()));
    let path = dir.join("nested").join("evidence.jsonl");
    let store = JsonlEvidenceStore::new(&path);

    let first = EvidenceRef::new("dagster_run", "dagster://runs/1").with_sha256("abc");
    let second = EvidenceRef::new("log", "file:out.log").with_note("note");
    store.record(&first).unwrap();
    store.record(&second).unwrap();

    let recorded = store.read_all().unwrap();
    assert_eq!(recorded, vec![first, second]);

    // The file is plain JSONL: one object per line.
    let raw = std::fs::read_to_string(&path).unwrap();
    assert_eq!(raw.lines().count(), 2);
    assert!(raw.ends_with('\n'));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn creates_parent_directories() {
    let dir = std::env::temp_dir().join(format!("ictus-evidence-mkdir-{}", std::process::id()));
    let path = dir.join("a").join("b").join("evidence.jsonl");
    let store = JsonlEvidenceStore::new(&path);
    store.record(&EvidenceRef::new("kind", "uri")).unwrap();
    assert!(path.exists());
    assert_eq!(store.read_all().unwrap().len(), 1);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn record_error_on_unwritable_path_type_checks() {
    // A directory as the target file cannot be opened for append.
    let dir = std::env::temp_dir().join(format!("ictus-evidence-err-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let store = JsonlEvidenceStore::new(&dir); // path IS the directory
    let error = store.record(&EvidenceRef::new("kind", "uri")).unwrap_err();
    assert!(
        error.to_string().contains("evidence store failed")
            || error.to_string().contains("Is a directory")
            || error.to_string().contains("Permission")
    );
    let _ = std::fs::remove_dir_all(&dir);
}
