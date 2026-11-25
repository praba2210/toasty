use serde::{Deserialize, Serialize};
use toasty::db::Capability;

/// The database flavor a migration targets: selects the capability the schema
/// is lowered with and the SQL dialect it is rendered in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum Flavor {
    /// SQLite
    Sqlite,

    /// PostgreSQL
    Postgresql,

    /// MySQL
    Mysql,

    /// MariaDB
    ///
    /// Distinct from [`Flavor::Mysql`]: MariaDB has its own dialect and a
    /// native `UUID` column type.
    #[value(name = "mariadb")]
    MariaDb,

    /// Turso
    Turso,

    /// Amazon Aurora DSQL
    ///
    /// PostgreSQL dialect with DSQL's rules: one statement per migration
    /// step, asynchronous index builds, and no schema changes DSQL cannot
    /// apply. `migrate apply` and `migrate reset` connect with the PostgreSQL
    /// driver's Aurora DSQL profile.
    #[value(name = "aurora-dsql")]
    #[serde(rename = "aurora-dsql")]
    AuroraDsql,
}

impl Flavor {
    /// The name passed to the schema dumper in `TOASTY_DUMP_SCHEMA`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Flavor::Sqlite => "sqlite",
            Flavor::Postgresql => "postgresql",
            Flavor::Mysql => "mysql",
            Flavor::MariaDb => "mariadb",
            Flavor::Turso => "turso",
            Flavor::AuroraDsql => "aurora-dsql",
        }
    }

    /// The capability describing the target database.
    pub fn capability(&self) -> &'static Capability {
        match self {
            Flavor::Sqlite => &Capability::SQLITE,
            Flavor::Postgresql => &Capability::POSTGRESQL,
            Flavor::Mysql => &Capability::MYSQL,
            Flavor::MariaDb => &Capability::MARIADB,
            Flavor::Turso => &Capability::TURSO,
            Flavor::AuroraDsql => &Capability::AURORA_DSQL,
        }
    }
}

impl std::fmt::Display for Flavor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::ValueEnum;

    #[test]
    fn aurora_dsql_flavor_selects_the_dsql_capability() {
        let flag = Flavor::from_str("aurora-dsql", false).unwrap();
        #[derive(Deserialize)]
        struct Config {
            flavor: Flavor,
        }
        let config: Config = toml::from_str("flavor = \"aurora-dsql\"").unwrap();

        for flavor in [flag, config.flavor] {
            assert_eq!(flavor, Flavor::AuroraDsql);
            assert_eq!(flavor.as_str(), "aurora-dsql");
            assert_eq!(flavor.capability().driver_name, "Aurora DSQL");
        }
    }
}
