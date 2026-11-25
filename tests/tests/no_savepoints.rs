#![cfg(feature = "sqlite")]

//! Tests how the engine runs transactions on a backend without savepoints
//! ([`Capability::savepoints`] is `false`, as on Aurora DSQL).
//!
//! Each test runs SQLite behind an [`InstrumentedDriver`] that reports no
//! savepoints and records what the engine asks of it.

use toasty_core::driver::{Capability, Operation, operation::Transaction};
use toasty_driver_integration_suite::{InstrumentedDriver, InstrumentedHandle};

#[derive(Debug, toasty::Model)]
struct Parent {
    #[key]
    #[auto]
    id: u64,

    name: String,

    #[version]
    version: u64,

    #[has_many]
    children: toasty::Deferred<Vec<Child>>,
}

#[derive(Debug, toasty::Model)]
struct Child {
    #[key]
    #[auto]
    id: u64,

    #[index]
    parent_id: Option<u64>,

    #[belongs_to(key = parent_id, references = id)]
    parent: toasty::Deferred<Option<Parent>>,
}

const WITHOUT_SAVEPOINTS: Capability = Capability {
    savepoints: false,
    ..Capability::SQLITE
};

async fn setup() -> (toasty::Db, InstrumentedHandle) {
    let driver = InstrumentedDriver::new(Box::new(toasty_driver_sqlite::Sqlite::in_memory()))
        .with_capability(&WITHOUT_SAVEPOINTS);
    let log = driver.handle();
    let db = toasty::Db::builder()
        .models(toasty::models!(Parent, Child))
        .build(driver)
        .await
        .unwrap();
    db.push_schema().await.unwrap();
    (db, log)
}

/// The transaction operations the engine issued since the log was cleared.
fn transaction_ops(log: &InstrumentedHandle) -> Vec<Transaction> {
    let mut ops = Vec::new();
    while !log.is_empty() {
        if let Operation::Transaction(t) = log.pop_op() {
            ops.push(t);
        }
    }
    ops
}

#[tokio::test]
async fn nested_transaction_is_rejected() {
    let (mut db, log) = setup().await;
    log.clear();

    let mut tx = db.transaction().await.unwrap();
    let err = tx.transaction().await.err().expect("nested transaction");
    assert!(err.is_unsupported_feature(), "unexpected error: {err}");
    assert!(err.to_string().contains("savepoints"), "{err}");
    tx.commit().await.unwrap();

    assert!(matches!(
        transaction_ops(&log)[..],
        [Transaction::Start { .. }, Transaction::Commit]
    ));
}

#[tokio::test]
async fn multi_statement_plan_runs_without_savepoint() {
    let (mut db, log) = setup().await;
    log.clear();

    let mut tx = db.transaction().await.unwrap();
    let parent = Parent::create()
        .name("p")
        .children([Child::create(), Child::create()])
        .exec(&mut tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    assert!(matches!(
        transaction_ops(&log)[..],
        [Transaction::Start { .. }, Transaction::Commit]
    ));
    assert_eq!(parent.children().exec(&mut db).await.unwrap().len(), 2);
}

/// A stale delete of a `#[version]` parent first detaches its children, then
/// fails on the version check. No statement failed, but the plan's first
/// write would stay in the transaction, so the commit must fail.
#[tokio::test]
async fn failed_multi_statement_plan_fails_commit() {
    let (mut db, log) = setup().await;
    let stale = Parent::create()
        .name("p")
        .children([Child::create(), Child::create()])
        .exec(&mut db)
        .await
        .unwrap();
    let mut fresh = Parent::filter_by_id(stale.id).get(&mut db).await.unwrap();
    fresh.update().name("q").exec(&mut db).await.unwrap();
    log.clear();

    let mut tx = db.transaction().await.unwrap();
    let err = stale.delete().exec(&mut tx).await.unwrap_err();
    assert!(err.is_condition_failed(), "unexpected error: {err}");
    let err = tx.commit().await.unwrap_err();
    assert!(err.is_driver_operation_failed(), "unexpected error: {err}");

    let children = fresh.children().exec(&mut db).await.unwrap();
    assert_eq!(children.len(), 2, "the children stay attached");

    let ops = transaction_ops(&log);
    assert!(
        matches!(ops[..], [Transaction::Start { .. }, Transaction::Rollback]),
        "{ops:?}"
    );

    // The mark does not outlive the transaction.
    let tx = db.transaction().await.unwrap();
    tx.commit().await.unwrap();
}

/// A `#[version]` update reads, checks the version, then writes. Without a
/// savepoint its failure fails the commit, as it may come after the write.
#[tokio::test]
async fn failed_read_modify_write_fails_commit() {
    let (mut db, log) = setup().await;
    let mut stale = Parent::create().name("p").exec(&mut db).await.unwrap();
    let mut fresh = Parent::filter_by_id(stale.id).get(&mut db).await.unwrap();
    fresh.update().name("q").exec(&mut db).await.unwrap();
    log.clear();

    let mut tx = db.transaction().await.unwrap();
    let err = stale.update().name("s").exec(&mut tx).await.unwrap_err();
    assert!(err.is_condition_failed(), "unexpected error: {err}");
    let err = tx.commit().await.unwrap_err();
    assert!(err.is_driver_operation_failed(), "unexpected error: {err}");

    let reloaded = Parent::filter_by_id(stale.id).get(&mut db).await.unwrap();
    assert_eq!(reloaded.name, "q");

    let ops = transaction_ops(&log);
    assert!(
        matches!(ops[..], [Transaction::Start { .. }, Transaction::Rollback]),
        "{ops:?}"
    );
}

/// A failed single-statement operation wrote nothing, so the transaction
/// still commits.
#[tokio::test]
async fn failed_single_statement_does_not_fail_commit() {
    let (mut db, _log) = setup().await;

    let mut tx = db.transaction().await.unwrap();
    Parent::create().name("p").exec(&mut tx).await.unwrap();
    let err = Parent::filter_by_id(u64::MAX)
        .get(&mut tx)
        .await
        .unwrap_err();
    assert!(err.is_record_not_found(), "unexpected error: {err}");
    tx.commit().await.unwrap();

    assert_eq!(Parent::all().count().exec(&mut db).await.unwrap(), 1);
}
