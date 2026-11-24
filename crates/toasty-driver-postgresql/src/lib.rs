#![warn(missing_docs)]

//! Toasty driver for [PostgreSQL](https://www.postgresql.org/) using
//! [`tokio-postgres`](https://docs.rs/tokio-postgres).
//!
//! # Examples
//!
//! ```no_run
//! use toasty_driver_postgresql::PostgreSQL;
//!
//! let driver = PostgreSQL::new("postgresql://localhost/mydb").unwrap();
//! ```

mod oid_cache;
mod statement_cache;
#[cfg(feature = "tls")]
mod tls;
mod r#type;
mod value;

pub(crate) use value::Value;

use async_trait::async_trait;
use std::{borrow::Cow, future::Future, pin::Pin, sync::Arc, time::Duration};
use toasty_core::{
    Result, Schema,
    driver::{
        Capability, ConnectContext, ConnectionUrl, Driver, ExecResponse, Operation, QueryLogConfig,
        log::QueryLog,
        operation::{RawSqlRet, Transaction, TransactionMode, TypedValue},
    },
    schema::{
        db::{self, Migration, Table},
        diff,
    },
    stmt,
    stmt::ValueRecord,
};
use toasty_sql::{self as sql};
use tokio_postgres::{Client, Config, Socket, tls::MakeTlsConnect, types::ToSql};

enum SqlReturn {
    Count,
    Infer,
    Types(Vec<stmt::Type>),
}

use crate::{oid_cache::OidCache, statement_cache::StatementCache};

/// Classifies a `tokio_postgres::Error` into a Toasty error.
///
/// Errors that carry a server-side `DbError` are mapped to typed
/// variants where one exists (`SerializationFailure`,
/// `ReadOnlyTransaction`); everything else with a `DbError` becomes
/// `DriverOperationFailed`. Errors *without* a `DbError` are
/// classified as `ConnectionLost`: per `tokio-postgres`, those
/// originate from the underlying socket or protocol layer (closed
/// socket, IO error, end-of-stream during handshake), which the
/// pool treats as evictable.
fn classify_pg_error(e: tokio_postgres::Error) -> toasty_core::Error {
    if let Some(db_err) = e.as_db_error() {
        match db_err.code().code() {
            "40001" => toasty_core::Error::serialization_failure(db_err.message()),
            "25006" => toasty_core::Error::read_only_transaction(db_err.message()),
            _ => toasty_core::Error::driver_operation_failed(e),
        }
    } else {
        toasty_core::Error::connection_lost(e)
    }
}

/// A PostgreSQL [`Driver`] that connects via `tokio-postgres`.
///
/// # Examples
///
/// ```no_run
/// use toasty_driver_postgresql::PostgreSQL;
///
/// let driver = PostgreSQL::new("postgresql://localhost/mydb").unwrap();
/// ```
pub struct PostgreSQL {
    url: String,
    config: Config,
    #[cfg(feature = "tls")]
    tls: Option<tokio_postgres_rustls::MakeRustlsConnect>,
    capability: &'static Capability,
    dsql: Option<DsqlSchemaConfig>,
    password_provider: Option<PasswordProvider>,
}

type PasswordFuture =
    Pin<Box<dyn Future<Output = std::result::Result<String, BoxError>> + Send + 'static>>;
type PasswordProvider = Arc<dyn Fn() -> PasswordFuture + Send + Sync>;
type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Schema-execution settings for Aurora DSQL, read only by this driver.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Read by the F3/F4/F7 implementation.
struct DsqlSchemaConfig {
    /// `CACHE` value for identity columns: 1 or at least 65,536.
    identity_cache: u32,

    /// How long to wait for an asynchronous index-build job.
    job_timeout: Duration,

    /// Retries for a schema step the server rejects with SQLSTATE `40001`.
    schema_retries: u32,
    schema_retry_base_delay: Duration,
    schema_retry_max_delay: Duration,
}

impl Default for DsqlSchemaConfig {
    fn default() -> Self {
        Self {
            identity_cache: 65_536,
            job_timeout: Duration::from_secs(5 * 60),
            schema_retries: 3,
            schema_retry_base_delay: Duration::from_millis(50),
            schema_retry_max_delay: Duration::from_secs(5),
        }
    }
}

impl std::fmt::Debug for PostgreSQL {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            url,
            config,
            #[cfg(feature = "tls")]
            tls,
            capability,
            dsql,
            password_provider,
        } = self;
        let mut s = f.debug_struct("PostgreSQL");
        s.field("url", url);
        s.field("config", config);
        #[cfg(feature = "tls")]
        s.field("tls", &tls.as_ref().map(|_| "MakeRustlsConnect"));
        s.field("capability", &capability.driver_name);
        s.field("dsql", dsql);
        s.field(
            "password_provider",
            &password_provider.as_ref().map(|_| "PasswordProvider"),
        );
        s.finish()
    }
}

impl PostgreSQL {
    /// Create a new PostgreSQL driver from a connection URL.
    ///
    /// The URL takes the `postgresql://user:password@host:port/dbname`
    /// form (`postgres://` also works). Query parameters supplement or
    /// override the URL components, following libpq: `host`, `port`,
    /// `user`, `password`, `dbname`, `application_name`, and `options`
    /// (server startup options such as `-c search_path=my_schema`,
    /// applied to every connection the pool creates), plus the TLS
    /// parameters `sslmode`, `sslrootcert`, `sslcert`, `sslkey`,
    /// `channel_binding`, and `sslnegotiation`. Unrecognized parameters
    /// are ignored.
    pub fn new(url: impl Into<String>) -> Result<Self> {
        let url_str = url.into();
        let url = ConnectionUrl::parse(&url_str)?;

        if !(url.has_scheme("postgresql") || url.has_scheme("postgres")) {
            return Err(toasty_core::Error::invalid_connection_url(format!(
                "connection URL does not have a `postgresql` scheme; url={}",
                url.as_str()
            )));
        }

        if url.path().is_empty() {
            return Err(toasty_core::Error::invalid_connection_url(format!(
                "no database specified - missing path in connection URL; url={}",
                url.as_str()
            )));
        }

        let mut config = Config::new();

        let dbname = url.decoded_path().map_err(|_| {
            toasty_core::Error::invalid_connection_url("database name is not valid UTF-8")
        })?;
        config.dbname(dbname.trim_start_matches('/'));

        if let Some(user) = url.username()?.filter(|user| !user.is_empty()) {
            config.user(&*user);
        }

        if let Some(password) = url.password() {
            config.password(&*password);
        }

        // libpq lets standard connection parameters appear in the query
        // string; honor the ones a Toasty user can reasonably set so that
        // `postgresql:///mydb?host=/tmp&user=alice` reaches the server.
        // Single-valued setters (user, password, dbname, application_name,
        // options) replace earlier calls, so we can apply them inline; host and
        // port are list-valued — staged into Options below so a query
        // parameter cleanly overrides the URL component instead of being
        // appended as a fallback tokio-postgres would try first.
        let mut host: Option<String> = None;
        let mut port: Option<u16> = None;

        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "host" => host = Some(value.into_owned()),
                "port" => {
                    port = Some(value.parse::<u16>().map_err(|_| {
                        toasty_core::Error::invalid_connection_url(format!(
                            "invalid port in connection URL query parameter: {value}"
                        ))
                    })?);
                }
                "user" => {
                    config.user(&*value);
                }
                "password" => {
                    config.password(value.as_bytes());
                }
                "dbname" => {
                    config.dbname(&*value);
                }
                "application_name" => {
                    config.application_name(&*value);
                }
                "options" => {
                    config.options(&*value);
                }
                _ => {}
            }
        }

        let host = host.or(url.host()?.map(String::from)).ok_or_else(|| {
            toasty_core::Error::invalid_connection_url(format!(
                "missing host in connection URL; url={}",
                url.as_str()
            ))
        })?;
        config.host(&host);

        if let Some(port) = port.or(url.port()?) {
            config.port(port);
        }

        #[cfg(feature = "tls")]
        let tls = tls::configure_tls(&url, &mut config)?;

        #[cfg(not(feature = "tls"))]
        for (key, value) in url.query_pairs() {
            if key == "sslmode" && value != "disable" {
                return Err(toasty_core::Error::invalid_connection_url(
                    "TLS not available: compile with the `tls` feature",
                ));
            }
        }

        Ok(Self {
            url: url_str,
            config,
            #[cfg(feature = "tls")]
            tls,
            capability: &Capability::POSTGRESQL,
            dsql: None,
            password_provider: None,
        })
    }

    /// Targets Amazon Aurora DSQL.
    ///
    /// Selects [`Capability::AURORA_DSQL`] and DSQL's schema rules:
    /// `push_schema` and migrations run one DDL statement per transaction,
    /// build indexes with `CREATE INDEX ASYNC` and wait for the build job,
    /// render identity columns with `CACHE 65536`, and retry a schema step
    /// the server rejects with SQLSTATE `40001`. Transactions run at
    /// `REPEATABLE READ` whatever isolation level is requested, with a
    /// warning when it differs.
    ///
    /// Without this call the driver behaves as plain PostgreSQL; the host
    /// name is never inspected.
    pub fn aurora_dsql(mut self) -> Self {
        self.capability = &Capability::AURORA_DSQL;
        self.dsql.get_or_insert_with(DsqlSchemaConfig::default);
        self
    }

    /// Sets the `CACHE` value for identity (auto-increment) columns on
    /// Aurora DSQL. The default is 65,536.
    ///
    /// DSQL accepts 1 or at least 65,536. A large cache gives unique but
    /// non-consecutive ids across connections; 1 gives dense ids at lower
    /// insert throughput.
    ///
    /// Returns [`Error::invalid_driver_configuration`] for any other value,
    /// or when [`aurora_dsql`](Self::aurora_dsql) was not called first.
    ///
    /// [`Error::invalid_driver_configuration`]: toasty_core::Error::invalid_driver_configuration
    pub fn identity_cache(mut self, cache: u32) -> Result<Self> {
        let dsql = self.dsql_config_mut("identity_cache")?;
        if cache != 1 && cache < 65_536 {
            return Err(toasty_core::Error::invalid_driver_configuration(format!(
                "Aurora DSQL identity cache must be 1 or at least 65536, got {cache}"
            )));
        }
        dsql.identity_cache = cache;
        Ok(self)
    }

    /// Sets how long `push_schema` and migrations wait for an Aurora DSQL
    /// index-build job. The default is 5 minutes.
    ///
    /// When the wait times out, the operation fails and the job keeps running
    /// on the server.
    ///
    /// Returns [`Error::invalid_driver_configuration`] when
    /// [`aurora_dsql`](Self::aurora_dsql) was not called first, or when
    /// `timeout` is zero.
    ///
    /// [`Error::invalid_driver_configuration`]: toasty_core::Error::invalid_driver_configuration
    pub fn schema_job_timeout(mut self, timeout: Duration) -> Result<Self> {
        let dsql = self.dsql_config_mut("schema_job_timeout")?;
        if timeout.is_zero() {
            return Err(toasty_core::Error::invalid_driver_configuration(
                "Aurora DSQL schema job timeout must be greater than zero",
            ));
        }
        dsql.job_timeout = timeout;
        Ok(self)
    }

    /// Sets a function that returns the password for each new connection.
    ///
    /// The provider runs once per connection the driver opens: when the pool
    /// grows or replaces a connection, and for every `reset_db` and CLI
    /// connection. It does not run when a pooled connection is reused. Its
    /// result overrides any password in the connection URL. Use it for
    /// short-lived credentials such as Aurora DSQL IAM authentication tokens.
    ///
    /// When the provider fails, connecting fails with
    /// `driver_operation_failed("PostgreSQL password provider failed")`; the
    /// provider's own error text is not included because it may contain
    /// secrets.
    pub fn password_provider<F, Fut, E>(mut self, provider: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<String, E>> + Send + 'static,
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        self.password_provider = Some(Arc::new(move || {
            let fut = provider();
            Box::pin(async move { fut.await.map_err(Into::into) })
        }));
        self
    }

    fn dsql_config_mut(&mut self, option: &str) -> Result<&mut DsqlSchemaConfig> {
        self.dsql.as_mut().ok_or_else(|| {
            toasty_core::Error::invalid_driver_configuration(format!(
                "`{option}` requires `aurora_dsql()` to be called first"
            ))
        })
    }

    async fn connect_with_config(&self, config: Config) -> Result<Connection> {
        if self.password_provider.is_some() {
            todo!("F2: call the password provider and set the password on the config")
        }
        #[cfg(feature = "tls")]
        if let Some(ref tls) = self.tls {
            return Connection::connect(config, tls.clone()).await;
        }
        Connection::connect(config, tokio_postgres::NoTls).await
    }
}

#[async_trait]
impl Driver for PostgreSQL {
    fn url(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.url)
    }

    fn capability(&self) -> &'static Capability {
        self.capability
    }

    async fn connect(
        &self,
        cx: &ConnectContext,
    ) -> toasty_core::Result<Box<dyn toasty_core::driver::Connection>> {
        let mut connection = self.connect_with_config(self.config.clone()).await?;
        connection.query_log = cx.query_log;
        connection.dsql = self.dsql.clone();
        Ok(Box::new(connection))
    }

    fn validate_migration(&self, _schema_diff: &diff::Schema<'_>) -> Result<()> {
        if self.dsql.is_some() {
            todo!("F4/F6: reject schema changes Aurora DSQL cannot apply")
        }
        Ok(())
    }

    fn generate_migration(&self, schema_diff: &diff::Schema<'_>) -> Migration {
        if self.dsql.is_some() {
            todo!("F6: one statement per breakpoint, rendered with DdlOptions")
        }
        let statements = sql::MigrationStatement::from_diff(schema_diff, &Capability::POSTGRESQL);

        let sql_strings: Vec<String> = statements
            .iter()
            .map(|stmt| sql::Serializer::postgresql(stmt.schema()).serialize(stmt.statement()))
            .collect();

        Migration::new_sql(sql_strings.join("\n"))
    }

    async fn reset_db(&self) -> toasty_core::Result<()> {
        if self.dsql.is_some() {
            todo!("F11: drop every table in the current schema")
        }
        let dbname = self
            .config
            .get_dbname()
            .ok_or_else(|| {
                toasty_core::Error::invalid_connection_url("no database name configured")
            })?
            .to_string();

        // We cannot drop a database we are currently connected to, so we need a temp database.
        let temp_dbname = "__toasty_reset_temp";

        let connect = |dbname: &str| {
            let mut config = self.config.clone();
            config.dbname(dbname);
            self.connect_with_config(config)
        };

        // Step 1: Connect to the target DB and create a temp DB
        let conn = connect(&dbname).await?;
        conn.client
            .execute(&format!("DROP DATABASE IF EXISTS \"{}\"", temp_dbname), &[])
            .await
            .map_err(classify_pg_error)?;
        conn.client
            .execute(&format!("CREATE DATABASE \"{}\"", temp_dbname), &[])
            .await
            .map_err(classify_pg_error)?;
        drop(conn);

        // Step 2: Connect to the temp DB, drop and recreate the target
        let conn = connect(temp_dbname).await?;
        conn.client
            .execute(
                "SELECT pg_terminate_backend(pid) \
                 FROM pg_stat_activity \
                 WHERE datname = $1 AND pid <> pg_backend_pid()",
                &[&dbname],
            )
            .await
            .map_err(classify_pg_error)?;
        conn.client
            .execute(&format!("DROP DATABASE IF EXISTS \"{}\"", dbname), &[])
            .await
            .map_err(classify_pg_error)?;
        conn.client
            .execute(&format!("CREATE DATABASE \"{}\"", dbname), &[])
            .await
            .map_err(classify_pg_error)?;
        drop(conn);

        // Step 3: Connect back to the target and clean up the temp DB
        let conn = connect(&dbname).await?;
        conn.client
            .execute(&format!("DROP DATABASE IF EXISTS \"{}\"", temp_dbname), &[])
            .await
            .map_err(classify_pg_error)?;

        Ok(())
    }
}

/// An open connection to a PostgreSQL database.
#[derive(Debug)]
pub struct Connection {
    client: Client,
    statement_cache: StatementCache,
    oid_cache: OidCache,
    query_log: QueryLogConfig,
    dsql: Option<DsqlSchemaConfig>,
}

impl Connection {
    /// Initialize a Toasty PostgreSQL connection using an initialized client.
    pub fn new(client: Client) -> Self {
        Self {
            client,
            statement_cache: StatementCache::new(100),
            oid_cache: OidCache::new(),
            query_log: QueryLogConfig::default(),
            dsql: None,
        }
    }

    /// Connects to a PostgreSQL database using a [`postgres::Config`].
    ///
    /// See [`postgres::Client::configure`] for more information.
    pub async fn connect<T>(config: Config, tls: T) -> Result<Self>
    where
        T: MakeTlsConnect<Socket> + 'static,
        T::Stream: Send,
    {
        let (client, connection) = config.connect(tls).await.map_err(classify_pg_error)?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {e}");
            }
        });

        Ok(Self::new(client))
    }

    async fn exec_sql(
        &mut self,
        sql_as_str: &str,
        typed_params: Vec<TypedValue>,
        ret: SqlReturn,
    ) -> Result<ExecResponse> {
        let mut log = QueryLog::sql(
            &self.query_log,
            "postgresql",
            sql_as_str,
            typed_params.iter().map(|tv| &tv.value),
        );
        let result = self
            .exec_sql_inner(sql_as_str, typed_params, ret, &mut log)
            .await;
        log.finish(&result);
        result
    }

    async fn exec_sql_inner(
        &mut self,
        sql_as_str: &str,
        typed_params: Vec<TypedValue>,
        ret: SqlReturn,
        log: &mut QueryLog<'_>,
    ) -> Result<ExecResponse> {
        self.oid_cache
            .preload(&self.client, typed_params.iter().map(|tv| &tv.ty))
            .await?;
        let param_types: Vec<_> = typed_params
            .iter()
            .map(|tv| self.oid_cache.get(&tv.ty).clone())
            .collect();

        let values: Vec<_> = typed_params
            .into_iter()
            .map(|tv| Value::from(tv.value))
            .collect();
        let params = values
            .iter()
            .map(|param| param as &(dyn ToSql + Sync))
            .collect::<Vec<_>>();

        let statement = self
            .statement_cache
            .prepare_typed(&mut self.client, sql_as_str, &param_types)
            .await
            .map_err(classify_pg_error)?;

        if matches!(ret, SqlReturn::Count) {
            let count = self
                .client
                .execute(&statement, &params)
                .await
                .map_err(classify_pg_error)?;
            return Ok(ExecResponse::count(count));
        }

        let rows = self
            .client
            .query(&statement, &params)
            .await
            .map_err(classify_pg_error)?;

        log.rows(rows.len() as u64);

        let results = rows.into_iter().map(move |row| {
            let mut results = Vec::new();

            match &ret {
                SqlReturn::Count => unreachable!(),
                SqlReturn::Infer => {
                    for (i, column) in row.columns().iter().enumerate() {
                        results.push(Value::from_sql_infer(i, &row, column).into_inner());
                    }
                }
                SqlReturn::Types(ret_tys) => {
                    for (i, column) in row.columns().iter().enumerate() {
                        results.push(Value::from_sql(i, &row, column, &ret_tys[i]).into_inner());
                    }
                }
            }

            Ok(ValueRecord::from_vec(results))
        });

        Ok(ExecResponse::value_stream(stmt::ValueStream::from_iter(
            results,
        )))
    }

    /// Creates a table.
    pub async fn create_table(&mut self, schema: &db::Schema, table: &Table) -> Result<()> {
        let serializer = sql::Serializer::postgresql(schema);

        let sql = serializer.serialize(&sql::Statement::create_table(
            table,
            &Capability::POSTGRESQL,
        ));

        self.client
            .execute(&sql, &[])
            .await
            .map_err(classify_pg_error)?;

        for index in &table.indices {
            if index.primary_key {
                continue;
            }

            let sql = serializer.serialize(&sql::Statement::create_index(index));

            self.client
                .execute(&sql, &[])
                .await
                .map_err(classify_pg_error)?;
        }

        Ok(())
    }
}

impl From<Client> for Connection {
    fn from(client: Client) -> Self {
        Self::new(client)
    }
}

#[async_trait]
impl toasty_core::driver::Connection for Connection {
    async fn exec(&mut self, schema: &Arc<Schema>, op: Operation) -> Result<ExecResponse> {
        tracing::trace!(driver = "postgresql", op = %op.name(), "driver exec");

        if let Operation::Transaction(ref t) = op {
            // PostgreSQL has no `BEGIN IMMEDIATE` / `BEGIN EXCLUSIVE`
            // analogue; reject non-Default modes loudly rather than
            // silently dropping them at the serializer.
            if let Transaction::Start {
                mode: mode @ (TransactionMode::Immediate | TransactionMode::Exclusive),
                ..
            } = t
            {
                return Err(toasty_core::Error::unsupported_feature(format!(
                    "PostgreSQL does not support TransactionMode::{mode:?}"
                )));
            }
            if let Transaction::Start {
                isolation: Some(_), ..
            } = t
                && self.dsql.is_some()
            {
                todo!("F9: plain BEGIN, warn when the level is not REPEATABLE READ")
            }
            let sql = sql::Serializer::postgresql(&schema.db).serialize_transaction(t);
            self.client
                .batch_execute(&sql)
                .await
                .map_err(classify_pg_error)?;
            return Ok(ExecResponse::count(0));
        }

        let (sql, typed_params, ret_tys) = match op {
            Operation::Insert(op) => (sql::Statement::from(op.stmt), op.params, op.ret),
            Operation::QuerySql(query) => {
                (sql::Statement::from(query.stmt), query.params, query.ret)
            }
            Operation::RawSql(op) => {
                let ret = match op.ret {
                    RawSqlRet::None => SqlReturn::Count,
                    RawSqlRet::Infer => SqlReturn::Infer,
                    RawSqlRet::Types(types) => SqlReturn::Types(types),
                };
                return self.exec_sql(&op.sql, op.params, ret).await;
            }
            op => todo!("op={:#?}", op),
        };

        let sql_as_str = sql::Serializer::postgresql(&schema.db).serialize(&sql);

        let ret = if sql.returning_len().is_some() {
            SqlReturn::Types(ret_tys.unwrap())
        } else {
            SqlReturn::Count
        };

        self.exec_sql(&sql_as_str, typed_params, ret).await
    }

    async fn push_schema(&mut self, schema: &Schema) -> Result<()> {
        if self.dsql.is_some() {
            todo!("F3/F4/F5: one DDL per statement, ASYNC indexes and job waits")
        }
        let serializer = sql::Serializer::postgresql(&schema.db);

        // Create PostgreSQL enum types before creating tables.
        // Collect unique enum types across all columns.
        let mut created_enum_types = hashbrown::HashSet::new();
        for table in &schema.db.tables {
            for column in &table.columns {
                if let Some(type_enum) = column.storage_ty.named_enum()
                    && created_enum_types.insert(type_enum.name.clone())
                {
                    let sql = serializer.serialize(&sql::Statement::create_enum_type(type_enum));

                    tracing::debug!(enum_type = ?type_enum.name, "creating enum type");
                    self.client
                        .execute(&sql, &[])
                        .await
                        .map_err(classify_pg_error)?;
                }
            }
        }

        for table in &schema.db.tables {
            tracing::debug!(table = %table.name, "creating table");
            self.create_table(&schema.db, table).await?;
        }
        Ok(())
    }

    async fn applied_migrations(
        &mut self,
    ) -> Result<Vec<toasty_core::schema::db::AppliedMigration>> {
        // Ensure the migrations table exists
        self.client
            .execute(
                "CREATE TABLE IF NOT EXISTS __toasty_migrations (
                id BIGINT PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at TIMESTAMP NOT NULL
            )",
                &[],
            )
            .await
            .map_err(classify_pg_error)?;

        // Query all applied migrations
        let rows = self
            .client
            .query(
                "SELECT id FROM __toasty_migrations ORDER BY applied_at",
                &[],
            )
            .await
            .map_err(classify_pg_error)?;

        Ok(rows
            .iter()
            .map(|row| {
                let id: i64 = row.get(0);
                toasty_core::schema::db::AppliedMigration::new(id as u64)
            })
            .collect())
    }

    async fn apply_migration(
        &mut self,
        id: u64,
        name: &str,
        migration: &toasty_core::schema::db::Migration,
    ) -> Result<()> {
        tracing::info!(id = id, name = %name, "applying migration");
        if self.dsql.is_some() {
            todo!("F7: one statement per transaction, job waits, schema-step retry")
        }
        // Ensure the migrations table exists
        self.client
            .execute(
                "CREATE TABLE IF NOT EXISTS __toasty_migrations (
                id BIGINT PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at TIMESTAMP NOT NULL
            )",
                &[],
            )
            .await
            .map_err(classify_pg_error)?;

        // Start transaction
        let transaction = self.client.transaction().await.map_err(classify_pg_error)?;

        // Execute each migration statement
        for statement in migration.statements() {
            if let Err(e) = transaction
                .batch_execute(statement)
                .await
                .map_err(classify_pg_error)
            {
                transaction.rollback().await.map_err(classify_pg_error)?;
                return Err(e);
            }
        }

        // Record the migration
        if let Err(e) = transaction
            .execute(
                "INSERT INTO __toasty_migrations (id, name, applied_at) VALUES ($1, $2, NOW())",
                &[&(id as i64), &name],
            )
            .await
            .map_err(classify_pg_error)
        {
            transaction.rollback().await.map_err(classify_pg_error)?;
            return Err(e);
        }

        // Commit transaction
        transaction.commit().await.map_err(classify_pg_error)?;
        Ok(())
    }

    fn is_valid(&self) -> bool {
        !self.client.is_closed()
    }

    async fn ping(&mut self) -> Result<()> {
        // An empty `simple_query` is the lightest sync round-trip in
        // the PG protocol — it skips parsing entirely. Any failure is
        // surfaced as `connection_lost`: the only meaningful outcome
        // of a ping is "the connection is alive" or "evict it."
        self.client
            .simple_query("")
            .await
            .map(|_| ())
            .map_err(toasty_core::Error::connection_lost)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_postgres::config::Host;

    fn cfg(url: &str) -> Config {
        PostgreSQL::new(url).expect("valid URL").config
    }

    #[test]
    fn host_in_url_authority() {
        let c = cfg("postgresql://example.com/mydb");
        assert_eq!(c.get_hosts(), &[Host::Tcp("example.com".into())]);
        assert_eq!(c.get_dbname(), Some("mydb"));
    }

    // `Host::Unix` only exists on Unix targets in tokio-postgres, so the
    // tests that assert socket-path resolution are gated to those
    // platforms. Windows builds still exercise the URL-parsing path via
    // the non-Unix tests below.
    #[cfg(unix)]
    mod unix_socket {
        use super::*;
        use std::path::PathBuf;

        #[test]
        fn unix_socket_via_host_query_param() {
            // Regression for #984: libpq lets a Unix-socket directory be
            // supplied via `?host=/path`, since URL syntax cannot put a
            // filesystem path in the authority.
            let c = cfg("postgresql:///mydb?host=/tmp&user=myuser");
            assert_eq!(c.get_hosts(), &[Host::Unix(PathBuf::from("/tmp"))]);
            assert_eq!(c.get_user(), Some("myuser"));
            assert_eq!(c.get_dbname(), Some("mydb"));
        }

        #[test]
        fn query_param_host_overrides_url_authority() {
            // libpq semantics: a `host=` query parameter replaces (not
            // appends to) the URL authority host. tokio-postgres's
            // `Config::host` is additive across calls, so an authority
            // host would otherwise be tried first and the configured
            // socket reached only as a fallback.
            let c = cfg("postgresql://example.com/mydb?host=/var/run/postgresql");
            assert_eq!(
                c.get_hosts(),
                &[Host::Unix(PathBuf::from("/var/run/postgresql"))]
            );
        }

        #[test]
        fn query_param_port_user_password_dbname() {
            let c = cfg(
                "postgresql:///placeholder?host=/tmp&port=5433&user=alice&password=s3cret&dbname=real",
            );
            assert_eq!(c.get_hosts(), &[Host::Unix(PathBuf::from("/tmp"))]);
            assert_eq!(c.get_ports(), &[5433]);
            assert_eq!(c.get_user(), Some("alice"));
            assert_eq!(c.get_password(), Some(&b"s3cret"[..]));
            assert_eq!(c.get_dbname(), Some("real"));
        }
    }

    #[test]
    fn query_param_port_overrides_url_port() {
        // `Config::port` is additive too, so a `port=` query parameter
        // must replace the URL authority port for the same reason.
        let c = cfg("postgresql://example.com:5432/mydb?port=5433");
        assert_eq!(c.get_ports(), &[5433]);
    }

    #[test]
    fn application_name_query_param() {
        let c = cfg("postgresql://localhost/mydb?application_name=my_app");
        assert_eq!(c.get_application_name(), Some("my_app"));
    }

    #[test]
    fn options_query_param() {
        // libpq's `options` startup parameter passes through, e.g. to set
        // a per-connection `search_path`. Form-decoding maps `%20` (and
        // `+`) to a space and `%3D` to `=`.
        let c = cfg("postgresql://localhost/mydb?options=-c%20search_path%3Dtenant_a%2Cpublic");
        assert_eq!(c.get_options(), Some("-c search_path=tenant_a,public"));
    }

    #[test]
    fn missing_host_rejected() {
        let err = PostgreSQL::new("postgresql:///mydb").unwrap_err();
        assert!(
            err.to_string().contains("missing host"),
            "expected missing-host error, got: {err}"
        );
    }

    #[test]
    fn invalid_port_query_param_rejected() {
        let err = PostgreSQL::new("postgresql:///mydb?host=/tmp&port=not-a-number").unwrap_err();
        assert!(
            err.to_string().contains("invalid port"),
            "expected invalid-port error, got: {err}"
        );
    }

    #[test]
    fn aurora_dsql_selects_dsql_capability() {
        let pg = PostgreSQL::new("postgresql://localhost/mydb").unwrap();
        assert_eq!(pg.capability().driver_name, "PostgreSQL");
        assert!(pg.capability().savepoints);

        let dsql = pg.aurora_dsql();
        assert_eq!(dsql.capability().driver_name, "Aurora DSQL");
        assert!(!dsql.capability().savepoints);
    }

    #[test]
    fn dsql_options_validated() {
        let pg = || PostgreSQL::new("postgresql://localhost/mydb").unwrap();

        assert!(pg().aurora_dsql().identity_cache(1).is_ok());
        let dsql = pg().aurora_dsql().identity_cache(1).unwrap().dsql.unwrap();
        assert_eq!(dsql.identity_cache, 1);
        assert!(pg().aurora_dsql().identity_cache(65_536).is_ok());
        let err = pg().aurora_dsql().identity_cache(65_535).unwrap_err();
        assert!(err.is_invalid_driver_configuration(), "{err}");
        let err = pg()
            .aurora_dsql()
            .schema_job_timeout(Duration::ZERO)
            .unwrap_err();
        assert!(err.is_invalid_driver_configuration(), "{err}");

        let err = pg().identity_cache(65_536).unwrap_err();
        assert!(err.is_invalid_driver_configuration(), "{err}");
        let err = pg()
            .schema_job_timeout(Duration::from_secs(60))
            .unwrap_err();
        assert!(err.is_invalid_driver_configuration(), "{err}");
    }
}
