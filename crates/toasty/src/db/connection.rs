use super::Transaction;
use super::connection_task::{ConnectionHandle, ConnectionOperation};
use super::pool::Manager;
use super::tx::ConnRef;

use async_trait::async_trait;
use std::sync::Arc;
#[cfg(feature = "migration")]
use toasty_core::schema::db::{AppliedMigration, Migration};
use toasty_core::{
    Schema,
    driver::{
        Capability, ExecResponse,
        operation::{Operation, RawSql},
    },
    stmt,
};
use tokio::sync::oneshot;
use tracing::Instrument;

/// The connection worker task no longer exists. Returned as a
/// [`Error::connection_lost`](toasty_core::Error::connection_lost) error
/// instead of panicking: a worker torn down after a backend disconnect is a
/// normal failure mode, and unwrapping here panics inside caller code that
/// deliberately fails closed on panic (e.g. storage owners), turning one
/// transient disconnect into a permanent outage.
#[derive(Debug)]
struct WorkerGone(&'static str);

impl core::fmt::Display for WorkerGone {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "connection worker is gone: {}", self.0)
    }
}

impl std::error::Error for WorkerGone {}

fn worker_gone(failure: &'static str) -> crate::Error {
    crate::Error::connection_lost(WorkerGone(failure))
}

/// A dedicated database connection retrieved from a pool.
///
/// Holding a `Connection` guarantees that all operations are executed on the
/// same physical connection. This is useful when multiple statements must
/// share connection-level state such as temporary tables or session variables.
///
/// When dropped, the connection is returned to the pool for reuse.
pub struct Connection {
    pub(super) inner: deadpool::managed::Object<Manager>,
    pub(super) shared: Arc<super::Shared>,
}

impl Connection {
    /// Access the underlying connection handle.
    pub(crate) fn handle(&self) -> &ConnectionHandle {
        &self.inner
    }

    /// Returns the compiled schema used by this connection.
    pub fn schema(&self) -> &Arc<Schema> {
        &self.shared.engine.schema
    }

    pub(crate) async fn exec_stmt(
        &self,
        stmt: stmt::Statement,
        in_transaction: bool,
    ) -> crate::Result<ExecResponse> {
        // A statement outside a transaction that failed with a serialization
        // failure did not commit, so it is safe to plan and run it again.
        let Some(policy) = self.shared.occ_retry.as_ref().filter(|_| !in_transaction) else {
            return self.exec_stmt_once(stmt, in_transaction).await;
        };
        let mut retries = 0;
        loop {
            match self.exec_stmt_once(stmt.clone(), false).await {
                Err(err) => tokio::time::sleep(policy.next_retry(&mut retries, err)?).await,
                result => return result,
            }
        }
    }

    pub(crate) async fn exec_stmt_once(
        &self,
        stmt: stmt::Statement,
        in_transaction: bool,
    ) -> crate::Result<ExecResponse> {
        // Created on the caller's task so the span parents to the caller's
        // current span; the worker task enters it while executing.
        let span = crate::instrument::query_span(&self.shared.engine.schema, &stmt);

        let (tx, rx) = oneshot::channel();

        self.handle()
            .in_tx
            .send(ConnectionOperation::ExecStatement {
                stmt: Box::new(stmt),
                in_transaction,
                span: span.clone(),
                tx,
            })
            .map_err(|_| worker_gone("exec statement channel closed"))?;

        rx.instrument(span)
            .await
            .map_err(|_| worker_gone("exec statement worker dropped without replying"))?
    }

    pub(crate) async fn exec_operation(&self, operation: Operation) -> crate::Result<ExecResponse> {
        let (tx, rx) = oneshot::channel();

        self.handle()
            .in_tx
            .send(ConnectionOperation::ExecOperation {
                operation: Box::new(operation),
                span: tracing::Span::current(),
                tx,
            })
            .map_err(|_| worker_gone("exec operation channel closed"))?;

        rx.await
            .map_err(|_| worker_gone("exec operation worker dropped without replying"))?
    }

    pub(crate) async fn exec_raw_sql(&self, raw: RawSql) -> crate::Result<ExecResponse> {
        let span = crate::instrument::raw_sql_span();

        let (tx, rx) = oneshot::channel();

        self.handle()
            .in_tx
            .send(ConnectionOperation::ExecRawSql {
                raw: Box::new(raw),
                span: span.clone(),
                tx,
            })
            .map_err(|_| worker_gone("exec raw sql channel closed"))?;

        rx.instrument(span)
            .await
            .map_err(|_| worker_gone("exec raw sql worker dropped without replying"))?
    }

    /// Begin a transaction on this connection.
    ///
    /// Takes `&mut self` so the `Connection` is exclusively borrowed while
    /// the transaction is open. This prevents statements from running on the
    /// connection directly — bypassing the transaction — when they should
    /// have gone through `&mut tx`.
    pub async fn transaction(&mut self) -> crate::Result<super::Transaction<'_>> {
        <Self as super::Executor>::transaction(self).await
    }

    /// Returns a [`TransactionBuilder`](super::TransactionBuilder) that will
    /// use this connection.
    ///
    /// Like [`transaction`](Self::transaction), this takes `&mut self` so the
    /// `Connection` stays locked for the lifetime of the transaction.
    pub fn transaction_builder(&mut self) -> super::TransactionBuilder<'_> {
        super::TransactionBuilder::new(super::tx::TxSource::Connection(self))
    }

    /// Runs `f` in a transaction on this connection and commits it, re-running
    /// the whole transaction when it fails with an optimistic-concurrency
    /// conflict. See [`Db::transaction_with_retry`](super::Db::transaction_with_retry).
    ///
    /// `f` returns [`crate::Result<T>`]. Where the returned future must be
    /// `Send` (for example in `tokio::spawn` or an axum handler), `f` must be
    /// an `async move` closure that owns what it captures.
    pub async fn transaction_with_retry<T, F>(&mut self, mut f: F) -> crate::Result<T>
    where
        F: AsyncFnMut(&mut super::Transaction<'_>) -> crate::Result<T>,
    {
        let policy = self.shared.occ_retry.clone().unwrap_or_default();
        let mut retries = 0;
        loop {
            match self.transaction_once(&mut f).await {
                Err(err) => tokio::time::sleep(policy.next_retry(&mut retries, err)?).await,
                result => return result,
            }
        }
    }

    /// One attempt of `transaction_with_retry`: runs `f` in a transaction and
    /// commits it, or rolls it back when `f` fails.
    pub(crate) async fn transaction_once<T, F>(&mut self, f: &mut F) -> crate::Result<T>
    where
        F: AsyncFnMut(&mut super::Transaction<'_>) -> crate::Result<T>,
    {
        let mut tx = self.transaction().await?;
        match f(&mut tx).await {
            Ok(value) => tx.commit().await.map(|()| value),
            Err(err) => {
                // Best effort: the original error is the one to report.
                let _ = tx.rollback().await;
                Err(err)
            }
        }
    }

    /// Creates tables and indices defined in the schema on the database.
    pub async fn push_schema(&self) -> crate::Result<()> {
        tracing::info!("pushing schema to database");
        let (tx, rx) = oneshot::channel();
        self.handle()
            .in_tx
            .send(ConnectionOperation::PushSchema {
                span: tracing::Span::current(),
                tx,
            })
            .map_err(|_| worker_gone("push schema channel closed"))?;
        rx.await
            .map_err(|_| worker_gone("push schema worker dropped without replying"))?
    }

    #[cfg(feature = "migration")]
    pub(crate) async fn applied_migrations(&self) -> crate::Result<Vec<AppliedMigration>> {
        let (tx, rx) = oneshot::channel();
        self.handle()
            .in_tx
            .send(ConnectionOperation::AppliedMigrations {
                span: tracing::Span::current(),
                tx,
            })
            .map_err(|_| worker_gone("applied migrations channel closed"))?;
        rx.await
            .map_err(|_| worker_gone("applied migrations worker dropped without replying"))?
    }

    #[cfg(feature = "migration")]
    pub(crate) async fn apply_migration(
        &self,
        id: u64,
        name: &str,
        migration: Migration,
    ) -> crate::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.handle()
            .in_tx
            .send(ConnectionOperation::ApplyMigration {
                id,
                name: name.to_owned(),
                migration: Box::new(migration),
                span: tracing::Span::current(),
                tx,
            })
            .map_err(|_| worker_gone("apply migration channel closed"))?;
        rx.await
            .map_err(|_| worker_gone("apply migration worker dropped without replying"))?
    }
}

#[async_trait]
impl super::Executor for Connection {
    async fn transaction(&mut self) -> crate::Result<Transaction<'_>> {
        Transaction::begin(ConnRef::Borrowed(self)).await
    }

    async fn exec_untyped(
        &mut self,
        stmt: toasty_core::stmt::Statement,
    ) -> crate::Result<ExecResponse> {
        self.exec_stmt(stmt, false).await
    }

    async fn exec_raw_sql(&mut self, raw: RawSql) -> crate::Result<ExecResponse> {
        Connection::exec_raw_sql(self, raw).await
    }

    fn capability(&mut self) -> &Capability {
        self.shared.engine.capability()
    }

    fn schema(&mut self) -> &Arc<Schema> {
        Connection::schema(self)
    }
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("handle", &*self.inner)
            .finish()
    }
}
