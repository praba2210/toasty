//! Opt-in retry of optimistic-concurrency conflicts (`occ_retry` and
//! `transaction_with_retry`). Conflicts are injected with
//! [`Fault::SerializationFailure`].

use crate::Fault;
use crate::prelude::*;

use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
use std::time::Duration;
use toasty::db::RetryPolicy;

#[derive(Debug, toasty::Model)]
struct User {
    #[key]
    #[auto]
    id: uuid::Uuid,

    name: String,
}

/// A fast policy that counts its retries in `retries`.
fn counting_policy(retries: &Arc<AtomicU32>) -> RetryPolicy {
    let retries = retries.clone();
    RetryPolicy::default()
        .base_delay(Duration::from_millis(1))
        .on_retry(move |_, _| {
            retries.fetch_add(1, Ordering::SeqCst);
        })
}

/// Without `occ_retry`, a conflict is returned as is.
#[driver_test]
pub async fn occ_retry_off_by_default(t: &mut Test) -> Result<()> {
    let mut db = t.setup_db(models!(User)).await;

    t.inject_fault(Fault::SerializationFailure);
    let err = User::create().name("a").exec(&mut db).await.unwrap_err();
    assert!(err.is_serialization_failure());

    Ok(())
}

/// With `occ_retry`, a conflicted statement outside a transaction runs again.
#[driver_test]
pub async fn occ_retry_retries_outside_transaction(t: &mut Test) -> Result<()> {
    let retries = Arc::new(AtomicU32::new(0));
    let mut db = t
        .setup_db_with(models!(User), |b| {
            b.occ_retry(counting_policy(&retries));
        })
        .await;

    t.inject_fault(Fault::SerializationFailure);
    t.inject_fault(Fault::SerializationFailure);
    let user = User::create().name("a").exec(&mut db).await?;

    assert_eq!(retries.load(Ordering::SeqCst), 2);
    assert_eq!(User::get_by_id(&mut db, user.id).await?.name, "a");

    Ok(())
}

/// When every attempt conflicts, the error is `retries_exhausted` with the
/// conflict as its source, after exactly `max_retries` retries.
#[driver_test]
pub async fn occ_retry_exhausted(t: &mut Test) -> Result<()> {
    let retries = Arc::new(AtomicU32::new(0));
    let mut db = t
        .setup_db_with(models!(User), |b| {
            b.occ_retry(counting_policy(&retries).max_retries(2));
        })
        .await;

    for _ in 0..3 {
        t.inject_fault(Fault::SerializationFailure);
    }
    let err = User::create().name("a").exec(&mut db).await.unwrap_err();

    assert!(err.is_retries_exhausted());
    let source = std::error::Error::source(&err).unwrap();
    assert!(
        source
            .downcast_ref::<toasty::Error>()
            .unwrap()
            .is_serialization_failure()
    );
    assert_eq!(retries.load(Ordering::SeqCst), 2);

    // All three faults were used: the next call succeeds at once.
    User::create().name("b").exec(&mut db).await?;
    assert_eq!(retries.load(Ordering::SeqCst), 2);

    Ok(())
}

/// Errors other than serialization failures are not retried.
#[driver_test]
pub async fn occ_retry_skips_other_errors(t: &mut Test) -> Result<()> {
    let retries = Arc::new(AtomicU32::new(0));
    let mut db = t
        .setup_db_with(models!(User), |b| {
            b.occ_retry(counting_policy(&retries));
        })
        .await;

    t.inject_fault(Fault::OperationFailed);
    let err = User::create().name("a").exec(&mut db).await.unwrap_err();

    assert!(err.is_driver_operation_failed());
    assert_eq!(retries.load(Ordering::SeqCst), 0);

    Ok(())
}

/// Statements inside a transaction are never retried one by one.
#[driver_test(requires(sql))]
pub async fn occ_retry_skips_transaction_statements(t: &mut Test) -> Result<()> {
    let retries = Arc::new(AtomicU32::new(0));
    let mut db = t
        .setup_db_with(models!(User), |b| {
            b.occ_retry(counting_policy(&retries));
        })
        .await;

    let mut tx = db.transaction().await?;
    t.inject_fault(Fault::SerializationFailure);
    let err = User::create().name("a").exec(&mut tx).await.unwrap_err();
    tx.rollback().await?;

    assert!(err.is_serialization_failure());
    assert_eq!(retries.load(Ordering::SeqCst), 0);

    Ok(())
}

/// `transaction_with_retry` re-runs the whole closure after a conflict on a
/// statement and after a conflict at commit, rolling back the failed attempt.
/// Without `occ_retry` it uses the default policy.
#[driver_test(requires(sql))]
pub async fn transaction_with_retry_reruns_closure(t: &mut Test) -> Result<()> {
    let mut db = t.setup_db(models!(User)).await;
    let faults = t.log().clone();

    let mut runs = 0;
    let name = db
        .transaction_with_retry(async |tx| {
            runs += 1;
            match runs {
                // The create fails.
                1 => faults.inject_fault(Fault::SerializationFailure),
                // The create succeeds, the commit fails.
                2 => {
                    User::create().name("a").exec(tx).await?;
                    faults.inject_fault(Fault::SerializationFailure);
                    return Ok("");
                }
                _ => {}
            }
            User::create().name("a").exec(tx).await?;
            Ok("done")
        })
        .await?;

    assert_eq!(name, "done");
    assert_eq!(runs, 3);
    assert_eq!(User::all().exec(&mut db).await?.len(), 1);

    Ok(())
}

/// `transaction_with_retry` (here on a `Connection`) follows the `occ_retry`
/// policy and returns `retries_exhausted` when every attempt conflicts.
#[driver_test(requires(sql))]
pub async fn transaction_with_retry_exhausted(t: &mut Test) -> Result<()> {
    let retries = Arc::new(AtomicU32::new(0));
    let db = t
        .setup_db_with(models!(User), |b| {
            b.occ_retry(counting_policy(&retries).max_retries(1));
        })
        .await;
    let faults = t.log().clone();

    let mut conn = db.connection().await?;
    let mut runs = 0;
    let err = conn
        .transaction_with_retry(async |tx| {
            runs += 1;
            faults.inject_fault(Fault::SerializationFailure);
            User::create().name("a").exec(tx).await?;
            Ok(())
        })
        .await
        .unwrap_err();

    assert!(err.is_retries_exhausted());
    assert_eq!(runs, 2);
    assert_eq!(retries.load(Ordering::SeqCst), 1);

    Ok(())
}

/// `transaction_with_retry` returns an error other than a serialization
/// failure after one run, without retrying.
#[driver_test(requires(sql))]
pub async fn transaction_with_retry_does_not_retry_other_errors(t: &mut Test) -> Result<()> {
    let mut db = t.setup_db(models!(User)).await;
    let faults = t.log().clone();

    let mut runs = 0;
    let err = db
        .transaction_with_retry(async |tx| {
            runs += 1;
            faults.inject_fault(Fault::OperationFailed);
            User::create().name("a").exec(tx).await?;
            Ok(())
        })
        .await
        .unwrap_err();

    assert!(err.is_driver_operation_failed(), "{err}");
    assert_eq!(runs, 1);
    assert!(User::all().exec(&mut db).await?.is_empty());

    Ok(())
}

/// With one pooled connection, the `Db` retry paths give the connection back
/// to the pool during the backoff, so another caller can use it.
#[driver_test(requires(sql))]
pub async fn retry_releases_the_connection_during_backoff(t: &mut Test) -> Result<()> {
    let retries = Arc::new(AtomicU32::new(0));
    let mut db = t
        .setup_db_with(models!(User), |b| {
            b.max_pool_size(1)
                .pool_wait_timeout(Some(Duration::from_millis(200)))
                .occ_retry(counting_policy(&retries).base_delay(Duration::from_secs(1)));
        })
        .await;
    let faults = t.log().clone();
    let other = db.clone();
    // Runs during the backoff, which lasts at least 500 ms.
    let use_the_pool = async || {
        tokio::time::sleep(Duration::from_millis(100)).await;
        other.connection().await.map(drop)
    };

    t.inject_fault(Fault::SerializationFailure);
    let (created, acquired) = tokio::join!(User::create().name("a").exec(&mut db), use_the_pool());
    created?;
    acquired?;

    let mut runs = 0;
    let (committed, acquired) = tokio::join!(
        db.transaction_with_retry(async |tx| {
            runs += 1;
            if runs == 1 {
                faults.inject_fault(Fault::SerializationFailure);
            }
            User::create().name("b").exec(tx).await?;
            Ok(())
        }),
        use_the_pool()
    );
    committed?;
    acquired?;
    assert_eq!(retries.load(Ordering::SeqCst), 2);

    Ok(())
}
