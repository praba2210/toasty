use crate::Flavor;

/// Redact the password portion of a database URL for safe display.
///
/// If the URL can be parsed and contains a password, replaces it with `***`.
/// If parsing fails, returns the original string unchanged.
pub(crate) fn redact_url_password(url: &str) -> String {
    toasty_core::driver::ConnectionUrl::parse(url)
        .map(|url| url.redact_password().into_owned())
        .unwrap_or_else(|_| url.to_string())
}

/// Connect to a database without any models registered; migration commands
/// only need the driver's migration tracking table.
///
/// The `aurora-dsql` flavor builds the PostgreSQL driver with the Aurora DSQL
/// profile; every other flavor connects from the URL alone.
pub(crate) async fn connect(url: &str, flavor: Option<Flavor>) -> anyhow::Result<toasty::Db> {
    let mut builder = toasty::Db::builder();
    let db = match flavor {
        #[cfg(feature = "postgresql")]
        Some(Flavor::AuroraDsql) => {
            // The driver's scheme error prints the URL, password included.
            if toasty_core::driver::ConnectionUrl::parse(url)
                .is_ok_and(|url| !(url.has_scheme("postgresql") || url.has_scheme("postgres")))
            {
                anyhow::bail!("the `aurora-dsql` flavor needs a `postgresql://` URL");
            }
            match toasty_driver_postgresql::PostgreSQL::new(url) {
                Ok(driver) => builder.build(driver.aurora_dsql()).await,
                Err(err) => Err(err),
            }
        }
        _ => builder.connect(url).await,
    };
    db.map_err(|err| anyhow::anyhow!("failed to connect to `{}`: {err}", redact_url_password(url)))
}

#[cfg(all(test, feature = "postgresql"))]
mod tests {
    use super::*;

    #[test]
    fn aurora_dsql_flavor_rejects_another_scheme_without_printing_the_url() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let url = "mysql://root:hunter2@localhost/app";
        let Err(err) = runtime.block_on(connect(url, Some(Flavor::AuroraDsql))) else {
            panic!("connected with a mysql URL");
        };
        let message = format!("{err:#}");
        assert!(!message.contains("hunter2"), "{message}");
        assert!(message.contains("needs a `postgresql://` URL"), "{message}");
    }
}
