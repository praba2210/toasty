//! Aurora DSQL migration rules: the schema changes DSQL cannot apply, and how
//! the changes it can apply are rendered.

use toasty_core::{
    Error, Result,
    driver::{Capability, SchemaMutations},
    schema::{db, diff},
};

use crate::stmt::DdlOptions;

/// Whether `capability` is [`Capability::AURORA_DSQL`].
pub(super) fn is_aurora_dsql(capability: &Capability) -> bool {
    capability.driver_name == Capability::AURORA_DSQL.driver_name
}

/// DDL options for generated migrations: asynchronous index builds and the
/// default identity cache.
pub(super) const DDL_OPTIONS: DdlOptions = DdlOptions {
    async_index: true,
    identity_cache: Some(65_536),
};

fn check_table(table: &db::Table) -> Result<()> {
    for column in &table.columns {
        check_column(table, column)?;
    }
    for index in table.indices.iter().filter(|index| !index.primary_key) {
        check_index(table, index)?;
    }
    Ok(())
}

fn check_column(table: &db::Table, column: &db::Column) -> Result<()> {
    // DSQL identity columns must be `bigint`, which PostgreSQL renders for
    // signed integers of more than 4 bytes and unsigned ones of more than 2.
    let bigint = matches!(
        column.storage_ty,
        db::Type::Integer(5..) | db::Type::UnsignedInteger(3..)
    );
    if column.auto_increment && !bigint {
        return Err(unsupported(
            column_subject(table, column),
            "identity columns must be 64-bit; use i64/u64 or a UUID key",
        ));
    }
    if column.storage_ty == db::Type::Numeric(None) {
        return Err(unsupported(
            column_subject(table, column),
            "NUMERIC without precision is numeric(18,6) and rounds to 6 decimal places; \
             declare a precision and scale, for example `#[column(type = numeric(38, 10))]`",
        ));
    }
    Ok(())
}

fn check_index(table: &db::Table, index: &db::Index) -> Result<()> {
    if index
        .columns
        .iter()
        .any(|column| matches!(column.op, db::IndexOp::Sort(_)))
    {
        return Err(unsupported(
            format!("index `{}` on table `{}`", index.name, table.name),
            "index key sort order is not supported",
        ));
    }
    Ok(())
}

/// Capability used to generate Aurora DSQL migrations.
///
/// [`validate_migration`] has already rejected every column change DSQL
/// cannot make in place, so the generator renders the remaining ones
/// (`DROP NOT NULL`, adding or dropping an identity) as `ALTER` statements
/// instead of a table rebuild.
pub(super) const MIGRATION_CAPABILITY: Capability = Capability {
    schema_mutations: SchemaMutations {
        alter_column_type: true,
        alter_column_properties_atomic: false,
    },
    ..Capability::AURORA_DSQL
};

/// Rejects schema changes Aurora DSQL cannot apply, before a migration is
/// generated.
pub(super) fn validate_migration(schema_diff: &diff::Schema<'_>) -> Result<()> {
    for table in schema_diff.tables() {
        match table {
            diff::Table::Create(table) => check_table(table)?,
            diff::Table::Drop(_) => {}
            diff::Table::Alter {
                next,
                columns,
                indices,
                ..
            } => {
                for column in columns {
                    check_column_change(next, column)?;
                }
                for index in indices {
                    if let diff::Index::Create(index) | diff::Index::Alter { next: index, .. } =
                        index
                    {
                        check_index(next, index)?;
                    }
                }
            }
        }
    }
    Ok(())
}

fn check_column_change(table: &db::Table, change: &diff::Column<'_>) -> Result<()> {
    match change {
        diff::Column::Add(column) => {
            let subject = column_subject(table, column);
            if column.auto_increment || matches!(column.storage_ty, db::Type::Enum(_)) {
                return Err(unsupported(
                    subject,
                    "added columns cannot have a CHECK or identity; recreate the table",
                ));
            }
            if !column.nullable {
                return Err(unsupported(
                    subject,
                    "added columns cannot be NOT NULL; declare the field `Option<T>` (it stays \
                     nullable)",
                ));
            }
            check_column(table, column)
        }
        diff::Column::Drop(column) if column.primary_key => Err(unsupported(
            column_subject(table, column),
            "dropping a primary-key column is not supported; recreate the table",
        )),
        diff::Column::Drop(_) => Ok(()),
        diff::Column::Alter { previous, next } => {
            let subject = column_subject(table, next);
            if previous.storage_ty != next.storage_ty {
                return Err(unsupported(
                    subject,
                    "changing a column's type is not supported; recreate the table",
                ));
            }
            if previous.nullable && !next.nullable {
                return Err(unsupported(
                    subject,
                    "adding NOT NULL to an existing column is not supported",
                ));
            }
            if previous.primary_key != next.primary_key {
                return Err(unsupported(
                    subject,
                    "this change needs a table rebuild, which is not supported; create a new \
                     table and copy the data",
                ));
            }
            if next.auto_increment && !previous.auto_increment {
                return check_column(table, next);
            }
            Ok(())
        }
    }
}

fn column_subject(table: &db::Table, column: &db::Column) -> String {
    format!("column `{}.{}`", table.name, column.name)
}

fn unsupported(subject: impl std::fmt::Display, message: &str) -> Error {
    Error::unsupported_feature(format!("Aurora DSQL: {subject}: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use toasty_core::stmt;

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

    #[track_caller]
    fn assert_rejected(table: db::Table, message: &str) {
        let err = check_table(&table).unwrap_err();
        assert!(err.is_unsupported_feature(), "{err}");
        assert!(err.to_string().contains(message), "{err}");
    }

    #[test]
    fn check_accepts_bigint_identity() {
        check_table(&table(db::Type::Integer(8), true, db::IndexOp::Eq)).unwrap();
        check_table(&table(db::Type::UnsignedInteger(4), true, db::IndexOp::Eq)).unwrap();
        check_table(&table(
            db::Type::Numeric(Some((57, 28))),
            false,
            db::IndexOp::Eq,
        ))
        .unwrap();
    }

    #[test]
    fn check_rejects_narrow_identity() {
        assert_rejected(
            table(db::Type::Integer(4), true, db::IndexOp::Eq),
            "column `users.id`: identity columns must be 64-bit",
        );
        assert_rejected(
            table(db::Type::UnsignedInteger(2), true, db::IndexOp::Eq),
            "identity columns must be 64-bit",
        );
    }

    #[test]
    fn check_rejects_unbounded_numeric() {
        assert_rejected(
            table(db::Type::Numeric(None), false, db::IndexOp::Eq),
            "NUMERIC without precision",
        );
    }

    #[test]
    fn check_rejects_sorted_index_key() {
        assert_rejected(
            table(
                db::Type::Integer(8),
                false,
                db::IndexOp::Sort(stmt::Direction::Desc),
            ),
            "index `index_users_by_id` on table `users`: index key sort order is not supported",
        );
    }
}
