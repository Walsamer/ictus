use ictus_ports::ExecutionReceipt;

#[test]
fn durable_receipt_requires_identity_and_digest_binding() {
    let receipt = ExecutionReceipt::new(
        "dagster-run-1",
        "dagster-run-1",
        "semantic-attempt-1",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "queued",
        "2026-10-08T18:00:00Z",
    );
    receipt.validate().unwrap();
}

#[test]
fn durable_receipt_rejects_an_unbound_identity() {
    let receipt = ExecutionReceipt::new(
        "dagster-run-1",
        "dagster-run-1",
        "",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "queued",
        "2026-10-08T18:00:00Z",
    );
    assert!(receipt.validate().is_err());
}
