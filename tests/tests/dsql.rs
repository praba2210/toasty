#![cfg(feature = "dsql")]

//! Runs the driver integration suite against Amazon Aurora DSQL through the
//! PostgreSQL driver's `aurora_dsql()` profile.
//!
//! Set `TOASTY_TEST_DSQL_HOST` to the cluster endpoint and
//! `TOASTY_TEST_DSQL_TOKEN` to an IAM auth token for the `admin` role. Each
//! test opens new connections, so the token must stay valid for the whole run.
//! The suite is generated only when `TOASTY_TEST_DSQL_HOST` is set at build
//! time (`tests/build.rs`); otherwise this target reports one ignored test.
//!
//! Tests run one at a time: DDL from concurrent tests on one cluster fails with
//! schema-change conflicts. To run in parallel, use one cluster per process.
//!
//! ```text
//! cargo test -p tests --features dsql --test dsql
//! ```

#[cfg(toasty_dsql)]
toasty_driver_integration_suite::generate_driver_tests!(
    setup::DsqlSetup::new(),
    bigdecimal_implemented: false,
    transaction_lock_mode: false,
    native_enum: false,
    native_array: false,
    vec_scalar: false,
    unique_list_index: false,
    vec_remove: false,
    vec_pop: false,
    vec_remove_at: false,
    decimal_arbitrary_precision: false,
    savepoints: false,
);

#[cfg(not(toasty_dsql))]
#[test]
#[ignore = "set TOASTY_TEST_DSQL_HOST and TOASTY_TEST_DSQL_TOKEN to run"]
fn dsql_suite() {}

#[cfg_attr(not(toasty_dsql), allow(dead_code))]
mod setup {
    use std::sync::Arc;
    use toasty_driver_postgresql::PostgreSQL;
    use tokio::sync::{Mutex, MutexGuard, OnceCell};
    use tokio_postgres::error::SqlState;

    /// Held by every test's setup, so tests run one at a time.
    static SERIAL: Mutex<()> = Mutex::const_new(());

    fn env(name: &str) -> String {
        std::env::var(name).unwrap_or_else(|_| panic!("{name} not set"))
    }

    pub struct DsqlSetup {
        client: OnceCell<tokio_postgres::Client>,
        _serial: MutexGuard<'static, ()>,
    }

    impl DsqlSetup {
        pub fn new() -> Self {
            Self {
                client: OnceCell::new(),
                _serial: SERIAL.blocking_lock(),
            }
        }

        async fn client(&self) -> &tokio_postgres::Client {
            self.client
                .get_or_init(|| async {
                    let provider = Arc::new(rustls::crypto::ring::default_provider());
                    let verifier = rustls_platform_verifier::Verifier::new(provider.clone())
                        .expect("Failed to load platform certificate verifier");
                    let tls = rustls::ClientConfig::builder_with_provider(provider)
                        .with_safe_default_protocol_versions()
                        .expect("Failed to configure TLS")
                        .dangerous()
                        .with_custom_certificate_verifier(Arc::new(verifier))
                        .with_no_client_auth();

                    let mut config = tokio_postgres::Config::new();
                    config
                        .host(env("TOASTY_TEST_DSQL_HOST"))
                        .user("admin")
                        .password(env("TOASTY_TEST_DSQL_TOKEN"))
                        .dbname("postgres")
                        .ssl_mode(tokio_postgres::config::SslMode::Require);
                    let (client, connection) = config
                        .connect(tokio_postgres_rustls::MakeRustlsConnect::new(tls))
                        .await
                        .expect("Failed to connect to Aurora DSQL");

                    tokio::spawn(async move {
                        if let Err(e) = connection.await {
                            eprintln!("connection error: {e}");
                        }
                    });

                    client
                })
                .await
        }
    }

    #[async_trait::async_trait]
    impl toasty_driver_integration_suite::Setup for DsqlSetup {
        fn driver(&self) -> Box<dyn toasty_core::driver::Driver> {
            let token: String =
                url::form_urlencoded::byte_serialize(env("TOASTY_TEST_DSQL_TOKEN").as_bytes())
                    .collect();
            let url = format!(
                "postgresql://admin:{token}@{}/postgres?sslmode=verify-full&sslrootcert=system",
                env("TOASTY_TEST_DSQL_HOST")
            );
            Box::new(
                // The error can contain the URL, which holds the token.
                PostgreSQL::new(&url)
                    .unwrap_or_else(|_| {
                        panic!("invalid Aurora DSQL URL; check TOASTY_TEST_DSQL_HOST")
                    })
                    .aurora_dsql(),
            )
        }

        async fn delete_table(&self, name: &str) {
            let sql = format!("DROP TABLE IF EXISTS \"{name}\" CASCADE");
            // DSQL can reject DDL with a schema-change conflict after other
            // DDL; the DROP did not commit, so it is safe to retry.
            for attempt in 0.. {
                match self.client().await.execute(&sql, &[]).await {
                    Ok(_) => return,
                    Err(e)
                        if attempt < 5
                            && e.code() == Some(&SqlState::T_R_SERIALIZATION_FAILURE) =>
                    {
                        tokio::time::sleep(std::time::Duration::from_millis(50 << attempt)).await;
                    }
                    Err(e) => panic!("Failed to drop table: {e}"),
                }
            }
        }
    }
}
