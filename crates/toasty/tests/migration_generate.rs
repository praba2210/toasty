#![cfg(feature = "migration")]

use async_trait::async_trait;
use std::borrow::Cow;
use toasty::Result;
use toasty::migration;
use toasty::schema::{db, diff};
use toasty_core::driver::{Capability, ConnectContext, Connection, Driver};
use toasty_core::stmt;
use toasty_driver_sqlite::Sqlite;

fn round_trip_snapshot(schema: db::Schema) -> (String, migration::Snapshot) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot.toml");

    migration::Snapshot::new(schema).save(&path).unwrap();

    let contents = std::fs::read_to_string(&path).unwrap();
    let snapshot = migration::Snapshot::load(path).unwrap();
    (contents, snapshot)
}

fn users_schema() -> db::Schema {
    let table_id = db::TableId(0);
    let id = db::ColumnId {
        table: table_id,
        index: 0,
    };

    db::Schema {
        tables: vec![db::Table {
            id: table_id,
            name: "users".to_string(),
            columns: vec![db::Column {
                id,
                name: "id".to_string(),
                ty: stmt::Type::I64,
                storage_ty: db::Type::Integer(8),
                nullable: false,
                primary_key: true,
                auto_increment: false,
                versionable: false,
            }],
            primary_key: db::PrimaryKey {
                columns: vec![id],
                index: db::IndexId {
                    table: table_id,
                    index: 0,
                },
            },
            indices: vec![],
        }],
    }
}

#[test]
fn generate_returns_none_for_empty_diff() {
    let schema = users_schema();
    let hints = diff::RenameHints::new();
    let driver = Sqlite::in_memory();

    let generated = migration::generate(&driver, &schema, &schema, &hints).unwrap();

    assert!(generated.is_none());
}

#[test]
fn generate_returns_migration_and_next_snapshot() {
    let previous = db::Schema::default();
    let next = users_schema();
    let hints = diff::RenameHints::new();
    let driver = Sqlite::in_memory();

    let generated = migration::generate(&driver, &previous, &next, &hints)
        .unwrap()
        .unwrap();

    assert_eq!(generated.snapshot.schema.tables[0].name, "users");
    let db::Migration::Sql(sql) = generated.migration;
    assert!(sql.contains("CREATE TABLE"));
    assert!(sql.contains("users"));
}

/// Wraps SQLite but rejects every migration, like a backend that cannot apply
/// the change.
#[derive(Debug)]
struct RejectingDriver(Sqlite);

#[async_trait]
impl Driver for RejectingDriver {
    fn url(&self) -> Cow<'_, str> {
        self.0.url()
    }

    fn capability(&self) -> &'static Capability {
        self.0.capability()
    }

    async fn connect(&self, cx: &ConnectContext) -> Result<Box<dyn Connection>> {
        self.0.connect(cx).await
    }

    fn generate_migration(&self, _schema_diff: &diff::Schema<'_>) -> db::Migration {
        panic!("generate_migration must not run after validation fails")
    }

    fn validate_migration(&self, _schema_diff: &diff::Schema<'_>) -> Result<()> {
        Err(toasty::Error::unsupported_feature("change not supported"))
    }

    async fn reset_db(&self) -> Result<()> {
        self.0.reset_db().await
    }
}

#[test]
fn generate_returns_driver_validation_error() {
    let previous = db::Schema::default();
    let next = users_schema();
    let hints = diff::RenameHints::new();
    let driver = RejectingDriver(Sqlite::in_memory());

    let err = migration::generate(&driver, &previous, &next, &hints).unwrap_err();

    assert!(err.is_unsupported_feature());
}

#[test]
fn snapshot_round_trips_empty_indices() {
    let (_, snapshot) = round_trip_snapshot(users_schema());

    assert!(snapshot.schema.tables[0].indices.is_empty());
}

#[test]
fn snapshot_round_trips_unconstrained_numeric() {
    let mut schema = users_schema();
    schema.tables[0].columns[0].storage_ty = db::Type::Numeric(None);

    let (contents, snapshot) = round_trip_snapshot(schema);

    assert!(contents.contains("storage_ty = { Numeric = [] }"));
    assert_eq!(
        snapshot.schema.tables[0].columns[0].storage_ty,
        db::Type::Numeric(None)
    );
}

#[test]
fn snapshot_round_trips_bounded_numeric() {
    let mut schema = users_schema();
    schema.tables[0].columns[0].storage_ty = db::Type::Numeric(Some((28, 10)));

    let (contents, snapshot) = round_trip_snapshot(schema);

    assert!(contents.contains("storage_ty = { Numeric = [28, 10] }"));
    assert_eq!(
        snapshot.schema.tables[0].columns[0].storage_ty,
        db::Type::Numeric(Some((28, 10)))
    );
}

#[test]
fn snapshot_round_trips_list_of_unconstrained_numeric() {
    let mut schema = users_schema();
    schema.tables[0].columns[0].storage_ty = db::Type::List(Box::new(db::Type::Numeric(None)));

    let (contents, snapshot) = round_trip_snapshot(schema);

    assert!(contents.contains("storage_ty = { List = { Numeric = [] } }"));
    assert_eq!(
        snapshot.schema.tables[0].columns[0].storage_ty,
        db::Type::List(Box::new(db::Type::Numeric(None)))
    );
}
