//! Aurora DSQL schema execution: one DDL statement per autocommit, waits for
//! asynchronous index jobs, retries schema steps that fail with SQLSTATE
//! `40001`, and rejects schema DSQL cannot create before any statement runs.

use crate::{Connection, DsqlSchemaConfig, classify_pg_error};
use std::{
    future::Future,
    hash::{BuildHasher, RandomState},
    time::Duration,
};
use toasty_core::{
    Error, Result,
    driver::Capability,
    schema::{db, diff},
};
use toasty_sql::stmt::DdlOptions;
use tokio_postgres::{SimpleQueryMessage, error::SqlState};

impl DsqlSchemaConfig {
    pub(crate) fn ddl_options(&self) -> DdlOptions {
        DdlOptions {
            async_index: true,
            identity_cache: Some(self.identity_cache),
        }
    }
}

/// Runs one schema step, retrying it when the server rejects it with
/// SQLSTATE `40001`. A rejected autocommit statement did not apply, so running
/// it again is safe. Other errors are returned as they are.
pub(crate) async fn retry_schema_step<T, F, Fut>(
    config: &DsqlSchemaConfig,
    mut step: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut retries = 0;
    loop {
        let err = match step().await {
            Err(err) if err.is_serialization_failure() => err,
            result => return result,
        };
        if retries == config.schema_retries {
            return Err(Error::retries_exhausted(retries + 1, err));
        }
        retries += 1;
        let delay = backoff(config, retries);
        tracing::warn!(retry = retries, ?delay, error = %err, "Aurora DSQL schema step conflicted; retrying");
        tokio::time::sleep(delay).await;
    }
}

/// Exponential backoff from the base delay, capped at the maximum, with the
/// delay drawn uniformly from its upper half.
fn backoff(config: &DsqlSchemaConfig, retry: u32) -> Duration {
    let delay = config
        .schema_retry_base_delay
        .saturating_mul(1 << (retry - 1).min(16))
        .min(config.schema_retry_max_delay);
    let unit = (RandomState::new().hash_one(retry) >> 11) as f64 / (1u64 << 53) as f64;
    delay / 2 + (delay / 2).mul_f64(unit)
}

/// Returns `true` when the server rejected the statement as unsupported
/// (SQLSTATE `0A000`).
pub(crate) fn is_feature_not_supported(err: &Error) -> bool {
    let mut source = std::error::Error::source(err);
    while let Some(err) = source {
        if let Some(err) = err.downcast_ref::<tokio_postgres::Error>() {
            return err.code() == Some(&SqlState::FEATURE_NOT_SUPPORTED);
        }
        source = err.source();
    }
    false
}

fn job_id(messages: &[SimpleQueryMessage]) -> Option<String> {
    messages.iter().find_map(|message| match message {
        SimpleQueryMessage::Row(row)
            if row.columns().first().map(|c| c.name()) == Some("job_id") =>
        {
            row.get(0).map(str::to_owned)
        }
        _ => None,
    })
}

/// The error message for a failed index-build job. DSQL leaves the index
/// INVALID; a unique one still rejects duplicate values.
fn job_failed_message(
    job_id: &str,
    details: Option<String>,
    object_name: Option<String>,
    sql: &str,
) -> String {
    let mut message = format!(
        "Aurora DSQL index job {job_id} failed: {}; statement: {}; the index is left INVALID",
        details.as_deref().unwrap_or("no details"),
        sql.trim_end().trim_end_matches(';'),
    );
    let unique = sql
        .trim_start()
        .get(..19)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("CREATE UNIQUE INDEX"));
    if unique {
        message.push_str(" and still rejects duplicate values");
    }
    match object_name {
        Some(object_name) => {
            let name = object_name
                .rsplit_once('.')
                .map_or(&*object_name, |(_, name)| name);
            message.push_str(&format!("; drop it with: DROP INDEX {}", quote_ident(name)));
        }
        None => message.push_str("; drop it before retrying"),
    }
    message
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

const CREATE_MIGRATIONS_TABLE: &str = "CREATE TABLE IF NOT EXISTS __toasty_migrations (
    id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at TIMESTAMP NOT NULL
)";

impl Connection {
    /// Runs one DDL statement in autocommit mode. When the server answers
    /// with an index-build job, waits for it and fails unless it succeeded.
    pub(crate) async fn exec_ddl(&mut self, config: &DsqlSchemaConfig, sql: &str) -> Result<()> {
        tracing::debug!(sql, "running Aurora DSQL schema statement");
        let client = &self.client;
        let job_id = retry_schema_step(config, || async {
            let messages = client.simple_query(sql).await.map_err(classify_pg_error)?;
            Ok(job_id(&messages))
        })
        .await?;

        match job_id {
            Some(job_id) => self.wait_for_job(config, &job_id, sql).await,
            None => Ok(()),
        }
    }

    /// Waits for an index-build job with one `CALL sys.wait_for_job`, which
    /// blocks until the job ends, bounded by the job timeout.
    async fn wait_for_job(
        &mut self,
        config: &DsqlSchemaConfig,
        job_id: &str,
        sql: &str,
    ) -> Result<()> {
        tracing::debug!(job_id, "waiting for Aurora DSQL index job");
        let client = &self.client;
        let wait = retry_schema_step(config, || async {
            let row = client
                .query_one("CALL sys.wait_for_job($1)", &[&job_id])
                .await
                .map_err(classify_pg_error)?;
            row.try_get(0).map_err(classify_pg_error)
        });
        let succeeded: bool = match tokio::time::timeout(config.job_timeout, wait).await {
            Ok(result) => result?,
            Err(_) => {
                // The `CALL` is still running on this connection and DSQL
                // ignores cancel requests, so the connection must not be
                // reused. Closing the socket ends the wait.
                self.discard = true;
                if let Some(task) = &self.task {
                    task.abort();
                }
                return Err(Error::driver_operation_failed(std::io::Error::other(
                    format!(
                        "Aurora DSQL index job {job_id} did not finish within {:?}; the job keeps \
                         running on the server; statement: {sql}",
                        config.job_timeout
                    ),
                )));
            }
        };
        if succeeded {
            return Ok(());
        }

        let job = retry_schema_step(config, || async {
            client
                .query_opt(
                    "SELECT details, object_name FROM sys.jobs WHERE job_id = $1",
                    &[&job_id],
                )
                .await
                .map_err(classify_pg_error)
        })
        .await?;
        let (details, object_name) = job.map_or((None, None), |row| (row.get(0), row.get(1)));
        Err(Error::driver_operation_failed(std::io::Error::other(
            job_failed_message(job_id, details, object_name, sql),
        )))
    }

    /// Creates every table and index after checking the whole schema, so an
    /// unsupported column or index fails before the first statement.
    pub(crate) async fn push_dsql_schema(&mut self, schema: &db::Schema) -> Result<()> {
        check_schema(schema)?;
        for table in &schema.tables {
            tracing::debug!(table = %table.name, "creating table");
            self.create_table(schema, table).await?;
        }
        Ok(())
    }

    /// Lists applied migrations, with each step retried on SQLSTATE `40001`.
    pub(crate) async fn dsql_applied_migrations(
        &mut self,
        config: &DsqlSchemaConfig,
    ) -> Result<Vec<db::AppliedMigration>> {
        self.exec_ddl(config, CREATE_MIGRATIONS_TABLE).await?;
        let client = &self.client;
        let rows = retry_schema_step(config, || async {
            client
                .query(
                    "SELECT id FROM __toasty_migrations ORDER BY applied_at",
                    &[],
                )
                .await
                .map_err(classify_pg_error)
        })
        .await?;
        Ok(rows
            .iter()
            .map(|row| db::AppliedMigration::new(row.get::<_, i64>(0) as u64))
            .collect())
    }

    /// Applies a migration one statement per autocommit, then records it.
    /// A failure leaves earlier statements applied and the migration
    /// unrecorded.
    pub(crate) async fn apply_dsql_migration(
        &mut self,
        config: &DsqlSchemaConfig,
        id: u64,
        name: &str,
        migration: &db::Migration,
    ) -> Result<()> {
        self.exec_ddl(config, CREATE_MIGRATIONS_TABLE).await?;

        let statements = migration.statements();
        for (index, statement) in statements.iter().enumerate() {
            if let Err(err) = self.exec_ddl(config, statement).await {
                let hint = if is_feature_not_supported(&err) {
                    "; Aurora DSQL does not support this statement: regenerate the migration for \
                     Aurora DSQL (`--flavor aurora-dsql`)"
                } else {
                    ""
                };
                // `err` stays outermost so its kind is kept; it has no cause.
                let applied = if index > 0 {
                    "earlier statements stay applied and "
                } else {
                    ""
                };
                return Err(Error::from_args(format_args!(
                    "migration {id} `{name}` failed at statement {} of {}; {applied}the \
                     migration is not recorded{hint}; statement: {statement}",
                    index + 1,
                    statements.len(),
                ))
                .context(err));
            }
        }

        let client = &self.client;
        retry_schema_step(config, || async {
            client
                .execute(
                    "INSERT INTO __toasty_migrations (id, name, applied_at) VALUES ($1, $2, NOW())",
                    &[&(id as i64), &name],
                )
                .await
                .map_err(classify_pg_error)
        })
        .await?;
        Ok(())
    }

    /// Drops every table in the current schema, one statement each.
    pub(crate) async fn drop_all_tables(&mut self, config: &DsqlSchemaConfig) -> Result<()> {
        let client = &self.client;
        let rows = retry_schema_step(config, || async {
            client
                .query(
                    "SELECT tablename FROM pg_tables WHERE schemaname = current_schema()",
                    &[],
                )
                .await
                .map_err(classify_pg_error)
        })
        .await?;
        for row in rows {
            let name: String = row.get(0);
            let sql = format!("DROP TABLE IF EXISTS {} CASCADE", quote_ident(&name));
            self.exec_ddl(config, &sql).await?;
        }
        Ok(())
    }
}

/// Rejects schema that Aurora DSQL cannot create, before any statement runs.
pub(crate) fn check_schema(schema: &db::Schema) -> Result<()> {
    let empty = db::Schema::default();
    let hints = diff::RenameHints::new();
    toasty_sql::validate_migration(
        &diff::Schema::from(&empty, schema, &hints),
        &Capability::AURORA_DSQL,
    )
}

/// Binds enum values as TEXT: DSQL stores enums in TEXT columns.
pub(crate) fn enum_as_text(ty: db::Type) -> db::Type {
    match ty {
        db::Type::Enum(_) => db::Type::Text,
        db::Type::List(elem) => db::Type::list(enum_as_text(*elem)),
        ty => ty,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use toasty_core::{driver::operation::TypedValue, stmt};

    fn config() -> DsqlSchemaConfig {
        DsqlSchemaConfig {
            schema_retry_base_delay: Duration::from_millis(1),
            schema_retry_max_delay: Duration::from_millis(2),
            ..DsqlSchemaConfig::default()
        }
    }

    async fn fail_times(calls: &AtomicU32, times: u32, err: fn() -> Error) -> Result<u32> {
        let call = calls.fetch_add(1, Ordering::SeqCst);
        if call < times { Err(err()) } else { Ok(call) }
    }

    fn conflict() -> Error {
        Error::serialization_failure("schema has been updated by another transaction (OC001)")
    }

    #[tokio::test]
    async fn schema_step_retried_on_serialization_failure() {
        let calls = AtomicU32::new(0);
        let result = retry_schema_step(&config(), || fail_times(&calls, 3, conflict)).await;
        assert_eq!(result.unwrap(), 3);
        assert_eq!(calls.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn schema_step_retries_exhausted() {
        let calls = AtomicU32::new(0);
        let err = retry_schema_step(&config(), || fail_times(&calls, 4, conflict))
            .await
            .unwrap_err();
        assert!(err.is_retries_exhausted(), "{err}");
        assert!(err.to_string().contains("after 4 attempts"), "{err}");
        assert_eq!(calls.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn schema_step_other_errors_not_retried() {
        let calls = AtomicU32::new(0);
        let err = retry_schema_step(&config(), || {
            fail_times(&calls, 1, || {
                Error::connection_lost(std::io::Error::other("closed"))
            })
        })
        .await
        .unwrap_err();
        assert!(err.is_connection_lost(), "{err}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    fn table(storage_ty: db::Type, auto_increment: bool, op: db::IndexOp) -> db::Table {
        let id = db::ColumnId {
            table: db::TableId(0),
            index: 0,
        };
        db::Table {
            id: db::TableId(0),
            name: "users".to_string(),
            columns: vec![db::Column {
                id,
                name: "id".to_string(),
                ty: stmt::Type::I64,
                storage_ty,
                nullable: false,
                primary_key: true,
                auto_increment,
                versionable: false,
            }],
            primary_key: db::PrimaryKey {
                columns: vec![id],
                index: db::IndexId {
                    table: db::TableId(0),
                    index: 0,
                },
            },
            indices: vec![db::Index {
                id: db::IndexId {
                    table: db::TableId(0),
                    index: 0,
                },
                name: "index_users_by_id".to_string(),
                on: db::TableId(0),
                columns: vec![db::IndexColumn {
                    column: id,
                    op,
                    scope: db::IndexScope::Local,
                }],
                unique: false,
                primary_key: false,
            }],
        }
    }

    /// A connection with the DSQL profile to the PG instance at
    /// `TOASTY_TEST_POSTGRES_URL`, or `None` when it is unset.
    async fn try_connect() -> Option<Connection> {
        let url = std::env::var("TOASTY_TEST_POSTGRES_URL").ok()?;
        let config: tokio_postgres::Config = url.parse().unwrap();
        let mut conn = Connection::connect(config, tokio_postgres::NoTls)
            .await
            .unwrap();
        conn.dsql = Some(DsqlSchemaConfig::default());
        Some(conn)
    }

    fn unique_name(tag: &str) -> String {
        format!("toasty_dsql_{tag}_{}", std::process::id())
    }

    #[tokio::test]
    async fn push_schema_rejects_before_any_statement() {
        let Some(mut conn) = try_connect().await else {
            return;
        };
        let mut valid = table(db::Type::Integer(8), true, db::IndexOp::Eq);
        valid.name = unique_name("valid");
        valid.indices.clear();
        let mut invalid = table(db::Type::Integer(4), true, db::IndexOp::Eq);
        invalid.id = db::TableId(1);
        invalid.name = unique_name("invalid");
        invalid.indices.clear();
        let schema = db::Schema {
            tables: vec![valid, invalid],
        };

        let err = conn.push_dsql_schema(&schema).await.unwrap_err();
        assert!(err.is_unsupported_feature(), "{err}");

        let rows = conn
            .client
            .query(
                "SELECT 1 FROM pg_tables WHERE tablename = $1",
                &[&schema.tables[0].name],
            )
            .await
            .unwrap();
        assert!(rows.is_empty(), "a table was created before the check");
    }

    #[tokio::test]
    async fn job_timeout_closes_the_connection() {
        let Some(mut conn) = try_connect().await else {
            return;
        };
        // A `sys.wait_for_job` that outlives the timeout, visible only to this
        // session's open transaction.
        conn.client
            .batch_execute(
                "BEGIN; CREATE SCHEMA IF NOT EXISTS sys; \
                 CREATE OR REPLACE PROCEDURE sys.wait_for_job(job text, INOUT ok boolean DEFAULT NULL) \
                 LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(3); ok := true; END $$;",
            )
            .await
            .unwrap();
        let config = DsqlSchemaConfig {
            job_timeout: Duration::from_millis(200),
            ..DsqlSchemaConfig::default()
        };

        let err = conn
            .wait_for_job(&config, "job-1", "CREATE INDEX ASYNC i ON t (c)")
            .await
            .unwrap_err();
        assert!(err.is_driver_operation_failed(), "{err}");
        assert!(err.to_string().contains("job-1"), "{err}");
        assert!(!toasty_core::driver::Connection::is_valid(&conn));

        tokio::time::timeout(Duration::from_secs(5), async {
            while !conn.client.is_closed() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the socket stays open after the timeout");
    }

    #[tokio::test]
    async fn job_failure_is_reported_after_the_wait_ends() {
        let Some(mut conn) = try_connect().await else {
            return;
        };
        // The job fails at once, and looking up its details takes longer
        // than the timeout, which bounds only the wait.
        conn.client
            .batch_execute(
                "BEGIN; CREATE SCHEMA IF NOT EXISTS sys; \
                 CREATE OR REPLACE PROCEDURE sys.wait_for_job(job text, INOUT ok boolean DEFAULT NULL) \
                 LANGUAGE plpgsql AS $$ BEGIN ok := false; END $$; \
                 CREATE OR REPLACE VIEW sys.jobs AS SELECT 'job-2'::text AS job_id, \
                 'duplicate key'::text AS details, 'public.i'::text AS object_name FROM pg_sleep(0.5);",
            )
            .await
            .unwrap();
        let config = DsqlSchemaConfig {
            job_timeout: Duration::from_millis(200),
            ..DsqlSchemaConfig::default()
        };

        let err = conn
            .wait_for_job(&config, "job-2", "CREATE UNIQUE INDEX ASYNC i ON t (c)")
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("index job job-2 failed: duplicate key"),
            "{err}"
        );
        assert!(toasty_core::driver::Connection::is_valid(&conn));
    }

    #[tokio::test]
    async fn failed_migration_statement_is_named_and_not_recorded() {
        let Some(mut conn) = try_connect().await else {
            return;
        };
        let name = unique_name("mig");
        let migration = db::Migration::new_sql_with_breakpoints(&[
            format!("CREATE TABLE {name}_a (id INT PRIMARY KEY)"),
            // PostgreSQL rejects this with SQLSTATE 0A000, like DSQL does
            // for statements it does not support.
            format!("CREATE TABLE {name}_b (a INT CHECK (a IN (SELECT 1)))"),
        ]);
        let id = u64::from(std::process::id());
        let config = DsqlSchemaConfig::default();

        let err = conn
            .apply_dsql_migration(&config, id, "bad", &migration)
            .await
            .unwrap_err();
        assert!(err.is_driver_operation_failed(), "{err}");
        let message = err.to_string();
        assert!(
            message.contains(&format!("migration {id} `bad` failed at statement 2 of 2")),
            "{message}"
        );
        assert!(message.contains("regenerate the migration"), "{message}");

        let recorded = conn
            .client
            .query(
                "SELECT 1 FROM __toasty_migrations WHERE id = $1",
                &[&(id as i64)],
            )
            .await
            .unwrap();
        assert!(recorded.is_empty());
        conn.client
            .batch_execute(&format!("DROP TABLE {name}_a"))
            .await
            .unwrap();
    }

    #[test]
    fn failed_job_message_names_job_statement_and_drop() {
        let message = job_failed_message(
            "j1",
            Some("found duplicate key(s) while validating index uniqueness".to_string()),
            Some("public.index_users_by_email".to_string()),
            "CREATE UNIQUE INDEX ASYNC \"index_users_by_email\" ON \"users\" (\"email\");",
        );
        assert_eq!(
            message,
            "Aurora DSQL index job j1 failed: found duplicate key(s) while validating index \
             uniqueness; statement: CREATE UNIQUE INDEX ASYNC \"index_users_by_email\" ON \
             \"users\" (\"email\"); the index is left INVALID and still rejects duplicate \
             values; drop it with: DROP INDEX \"index_users_by_email\""
        );

        let message = job_failed_message(
            "j2",
            Some("key size too large".to_string()),
            None,
            "CREATE INDEX ASYNC \"index_users_by_name\" ON \"users\" (\"name\")",
        );
        assert_eq!(
            message,
            "Aurora DSQL index job j2 failed: key size too large; statement: CREATE INDEX ASYNC \
             \"index_users_by_name\" ON \"users\" (\"name\"); the index is left INVALID; drop \
             it before retrying"
        );
    }

    #[tokio::test]
    async fn enum_values_bound_as_text() {
        let Some(mut conn) = try_connect().await else {
            return;
        };
        let name = unique_name("enum");
        conn.client
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {name}; CREATE TABLE {name} (status TEXT)"
            ))
            .await
            .unwrap();
        // No PostgreSQL enum type exists, as on DSQL.
        let ty = db::Type::Enum(db::TypeEnum {
            name: Some(format!("{name}_status")),
            variants: vec![db::EnumVariant {
                name: "active".to_string(),
            }],
        });

        conn.exec_sql(
            &format!("INSERT INTO {name} (status) VALUES ($1)"),
            vec![TypedValue {
                value: stmt::Value::String("active".to_string()),
                ty,
            }],
            crate::SqlReturn::Count,
        )
        .await
        .unwrap();

        conn.client
            .batch_execute(&format!("DROP TABLE {name}"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn serialization_failure_clears_statement_cache() {
        let (Some(mut conn), Some(other)) = (try_connect().await, try_connect().await) else {
            return;
        };
        let name = unique_name("occ");
        other
            .client
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {name}; CREATE TABLE {name} (id INT PRIMARY KEY, v INT); \
                 INSERT INTO {name} VALUES (1, 0)"
            ))
            .await
            .unwrap();

        conn.client
            .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
            .await
            .unwrap();
        conn.exec_sql(
            &format!("SELECT v FROM {name} WHERE id = 1"),
            vec![],
            crate::SqlReturn::Count,
        )
        .await
        .unwrap();
        assert_eq!(conn.statement_cache.len(), 1);
        other
            .client
            .batch_execute(&format!("UPDATE {name} SET v = 1 WHERE id = 1"))
            .await
            .unwrap();

        let err = conn
            .exec_sql(
                &format!("UPDATE {name} SET v = 2 WHERE id = 1"),
                vec![],
                crate::SqlReturn::Count,
            )
            .await
            .unwrap_err();
        assert!(err.is_serialization_failure(), "{err}");
        assert_eq!(conn.statement_cache.len(), 0);

        conn.client.batch_execute("ROLLBACK").await.unwrap();
        other
            .client
            .batch_execute(&format!("DROP TABLE {name}"))
            .await
            .unwrap();
    }
}
