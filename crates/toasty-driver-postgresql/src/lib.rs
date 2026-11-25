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

mod dsql;
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
        operation::{IsolationLevel, RawSqlRet, Transaction, TransactionMode, TypedValue},
    },
    schema::db::{self, Table},
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

/// Whether `err` is a server-side error, as classified by
/// [`classify_pg_error`]. Inside a transaction, such an error leaves the
/// transaction aborted.
fn is_server_error(err: &toasty_core::Error) -> bool {
    err.is_driver_operation_failed()
        || err.is_serialization_failure()
        || err.is_read_only_transaction()
}

/// Aurora DSQL runs every transaction at `REPEATABLE READ` and rejects
/// `BEGIN ISOLATION LEVEL` with any other level, so a `Start` that names a
/// level is sent without it. Returns `None` when `t` needs no change.
fn dsql_transaction_start(t: &Transaction) -> Option<Transaction> {
    let Transaction::Start {
        isolation: Some(level),
        read_only,
        mode,
    } = *t
    else {
        return None;
    };
    if level != IsolationLevel::RepeatableRead {
        tracing::warn!(
            "Aurora DSQL supports only REPEATABLE READ; running the transaction at \
             REPEATABLE READ instead of {level:?}"
        );
    }
    Some(Transaction::Start {
        isolation: None,
        read_only,
        mode,
    })
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
        let url = ConnectionUrl::parse(url)
            .map_or(Cow::Borrowed(url.as_str()), |url| url.redact_password());
        let mut s = f.debug_struct("PostgreSQL");
        s.field("url", &url);
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
    /// render identity columns with `CACHE 65536` by default (see
    /// [`identity_cache`](Self::identity_cache)), and retry a schema step
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
    /// This applies to tables created by `push_schema`. Generated migrations
    /// always use `CACHE 65536`; edit the migration SQL to change it.
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
    /// grows or replaces a connection, and for every `reset_db`
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

    async fn connect_with_config(&self, mut config: Config) -> Result<Connection> {
        if let Some(provider) = &self.password_provider {
            // The provider's error may contain a token, so it is dropped.
            let password = provider().await.map_err(|_| {
                toasty_core::Error::driver_operation_failed(std::io::Error::other(
                    "PostgreSQL password provider failed",
                ))
            })?;
            config.password(password);
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

    async fn reset_db(&self) -> toasty_core::Result<()> {
        // DSQL has a single database and no CREATE/DROP DATABASE.
        if let Some(config) = &self.dsql {
            let mut conn = self.connect_with_config(self.config.clone()).await?;
            return conn.drop_all_tables(config).await;
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
    /// Set when the server rejects a statement, which aborts the open
    /// transaction, and cleared when the server accepts one. PostgreSQL
    /// answers `COMMIT` on an aborted transaction by rolling it back without
    /// an error, and tokio-postgres does not expose the `ROLLBACK` command
    /// tag, so the driver tracks the state.
    aborted: bool,
    dsql: Option<DsqlSchemaConfig>,
    /// Set when the connection must not be reused (an abandoned `CALL` is
    /// still running on it).
    discard: bool,
    /// The spawned connection task, aborted to close the socket when the
    /// connection is discarded.
    task: Option<tokio::task::AbortHandle>,
}

impl Connection {
    /// Initialize a Toasty PostgreSQL connection using an initialized client.
    pub fn new(client: Client) -> Self {
        Self {
            client,
            statement_cache: StatementCache::new(100),
            oid_cache: OidCache::new(),
            query_log: QueryLogConfig::default(),
            aborted: false,
            dsql: None,
            discard: false,
            task: None,
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

        let task = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {e}");
            }
        });

        Ok(Self {
            task: Some(task.abort_handle()),
            ..Self::new(client)
        })
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
        self.track_aborted(&result);
        // A schema change by another session can leave cached plans stale.
        if self.dsql.is_some() && matches!(&result, Err(err) if err.is_serialization_failure()) {
            self.statement_cache.clear();
        }
        log.finish(&result);
        result
    }

    /// Updates `aborted` from the server's answer to a statement. In an
    /// aborted transaction PostgreSQL rejects every command except those that
    /// end the transaction or roll back to a savepoint, so a success means
    /// the transaction is not aborted. A client-side error never reached the
    /// server and leaves the state unchanged.
    fn track_aborted<T>(&mut self, result: &Result<T>) {
        match result {
            Ok(_) => self.aborted = false,
            Err(err) if is_server_error(err) => self.aborted = true,
            Err(_) => {}
        }
    }

    async fn exec_sql_inner(
        &mut self,
        sql_as_str: &str,
        typed_params: Vec<TypedValue>,
        ret: SqlReturn,
        log: &mut QueryLog<'_>,
    ) -> Result<ExecResponse> {
        let typed_params = if self.dsql.is_some() {
            typed_params
                .into_iter()
                .map(|tv| TypedValue {
                    ty: dsql::enum_as_text(tv.ty),
                    value: tv.value,
                })
                .collect()
        } else {
            typed_params
        };

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

        if let Some(config) = self.dsql.clone() {
            let options = config.ddl_options();
            let sql = serializer.serialize(&sql::Statement::create_table_with(
                table,
                &Capability::AURORA_DSQL,
                &options,
            ));
            self.exec_ddl(&config, &sql).await?;

            for index in table.indices.iter().filter(|index| !index.primary_key) {
                let sql = serializer.serialize(&sql::Statement::create_index_with(index, &options));
                self.exec_ddl(&config, &sql).await?;
            }
            return Ok(());
        }

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
            let dsql_start = self.dsql.as_ref().and_then(|_| dsql_transaction_start(t));
            let t = dsql_start.as_ref().unwrap_or(t);
            let sql = sql::Serializer::postgresql(&schema.db).serialize_transaction(t);
            let commit_after_abort = matches!(t, Transaction::Commit) && self.aborted;
            let result = self
                .client
                .batch_execute(&sql)
                .await
                .map_err(classify_pg_error);
            self.track_aborted(&result);
            result?;
            if commit_after_abort {
                return Err(toasty_core::Error::driver_operation_failed(
                    std::io::Error::other(
                        "transaction was aborted by an earlier error and has been rolled back",
                    ),
                ));
            }
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
        // DSQL has no enum types; enums are TEXT columns with a CHECK.
        if self.dsql.is_some() {
            return self.push_dsql_schema(&schema.db).await;
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
        if let Some(config) = self.dsql.clone() {
            return self.dsql_applied_migrations(&config).await;
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
        if let Some(config) = self.dsql.clone() {
            return self
                .apply_dsql_migration(&config, id, name, migration)
                .await;
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
        !self.discard && !self.client.is_closed()
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

    /// A server error from a savepoint command aborts the transaction, so
    /// committing it returns an error and nothing persists.
    #[tokio::test]
    async fn commit_after_failed_savepoint_command_errors() {
        use toasty_core::driver::{Connection as _, Rows, operation::RawSql};

        let Ok(url) = std::env::var("TOASTY_TEST_POSTGRES_URL") else {
            return;
        };
        let mut conn = Connection::connect(cfg(&url), tokio_postgres::NoTls)
            .await
            .unwrap();
        let app = toasty_core::schema::app::Schema::from_macro([]).unwrap();
        let schema = Arc::new(
            toasty_core::schema::Builder::new()
                .build(app, &Capability::POSTGRESQL)
                .unwrap(),
        );
        let raw = |sql: &str| {
            Operation::from(RawSql {
                sql: sql.to_string(),
                params: vec![],
                ret: RawSqlRet::None,
            })
        };

        conn.exec(&schema, raw("CREATE TEMP TABLE release_failure (id int)"))
            .await
            .unwrap();
        conn.exec(&schema, Transaction::start().into())
            .await
            .unwrap();
        conn.exec(&schema, raw("INSERT INTO release_failure VALUES (1)"))
            .await
            .unwrap();
        let release = Transaction::ReleaseSavepoint("does_not_exist".to_string());
        assert!(conn.exec(&schema, release.into()).await.is_err());
        let err = conn
            .exec(&schema, Transaction::Commit.into())
            .await
            .unwrap_err();
        assert!(err.is_driver_operation_failed(), "{err}");

        let deleted = conn
            .exec(&schema, raw("DELETE FROM release_failure"))
            .await
            .unwrap();
        assert!(matches!(deleted.values, Rows::Count(0)));
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
    fn dsql_transaction_start_drops_isolation_level() {
        use IsolationLevel::*;

        for level in [ReadUncommitted, ReadCommitted, RepeatableRead, Serializable] {
            for read_only in [false, true] {
                let t = Transaction::Start {
                    isolation: Some(level),
                    read_only,
                    mode: TransactionMode::Default,
                };
                let start = dsql_transaction_start(&t);
                assert!(
                    matches!(
                        start,
                        Some(Transaction::Start {
                            isolation: None,
                            read_only: r,
                            mode: TransactionMode::Default,
                        }) if r == read_only
                    ),
                    "{level:?}, read_only = {read_only}: {start:?}"
                );
            }
        }

        assert!(dsql_transaction_start(&Transaction::start()).is_none());
        assert!(dsql_transaction_start(&Transaction::Commit).is_none());
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

    #[test]
    fn debug_redacts_url_password() {
        let pg = PostgreSQL::new("postgresql://alice:url-sentinel@localhost/mydb").unwrap();
        let debug = format!("{pg:?}");
        assert!(!debug.contains("url-sentinel"), "{debug}");
    }

    #[tokio::test]
    async fn password_provider_error_has_fixed_message() {
        let pg = PostgreSQL::new("postgresql://127.0.0.1:1/mydb")
            .unwrap()
            .password_provider(|| async { Err::<String, _>("error-sentinel") });
        let err = pg.connect(&ConnectContext::default()).await.unwrap_err();
        assert!(err.is_driver_operation_failed(), "{err}");
        assert_eq!(err.to_string(), "PostgreSQL password provider failed");
    }

    #[tokio::test]
    async fn password_provider_result_not_exposed() {
        let pg = PostgreSQL::new("postgresql://127.0.0.1:1/mydb")
            .unwrap()
            .password_provider(|| async { Ok::<_, std::io::Error>("pw-sentinel".to_string()) });
        let err = pg.connect(&ConnectContext::default()).await.unwrap_err();
        for text in [err.to_string(), format!("{pg:?}"), pg.url().into_owned()] {
            assert!(!text.contains("pw-sentinel"), "{text}");
        }
    }
}
