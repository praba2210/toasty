//! Migrations for Aurora DSQL: rejected changes and the rendered statements.

use toasty_core::driver::Capability;
use toasty_core::schema::{db, diff};
use toasty_core::stmt;
use toasty_sql::{generate_migration, validate_migration};

fn col(index: usize, name: &str, storage_ty: db::Type, nullable: bool) -> db::Column {
    db::Column {
        id: db::ColumnId {
            table: db::TableId(0),
            index,
        },
        name: name.to_string(),
        ty: stmt::Type::String,
        storage_ty,
        nullable,
        primary_key: index == 0,
        auto_increment: false,
        versionable: false,
    }
}

fn index(index: usize, name: &str, column: usize, op: db::IndexOp) -> db::Index {
    db::Index {
        id: db::IndexId {
            table: db::TableId(0),
            index,
        },
        name: name.to_string(),
        on: db::TableId(0),
        columns: vec![db::IndexColumn {
            column: db::ColumnId {
                table: db::TableId(0),
                index: column,
            },
            op,
            scope: db::IndexScope::Local,
        }],
        unique: false,
        primary_key: false,
    }
}

fn schema(name: &str, columns: Vec<db::Column>, indices: Vec<db::Index>) -> db::Schema {
    let pk = columns
        .iter()
        .filter(|c| c.primary_key)
        .map(|c| c.id)
        .collect();
    db::Schema {
        tables: vec![db::Table {
            id: db::TableId(0),
            name: name.to_string(),
            columns,
            primary_key: db::PrimaryKey {
                columns: pk,
                index: db::IndexId {
                    table: db::TableId(0),
                    index: 0,
                },
            },
            indices,
        }],
    }
}

fn users() -> db::Schema {
    schema(
        "users",
        vec![
            col(0, "id", db::Type::Integer(8), false),
            col(1, "name", db::Type::Text, false),
            col(2, "nick", db::Type::Text, true),
        ],
        vec![index(0, "index_users_by_name", 1, db::IndexOp::Eq)],
    )
}

#[test]
fn rejects_changes_dsql_cannot_apply() {
    let base = users();
    let with = |f: &dyn Fn(&mut db::Table)| {
        let mut s = users();
        f(&mut s.tables[0]);
        s
    };
    let cases: Vec<(db::Schema, db::Schema, &str)> = vec![
        (
            base.clone(),
            with(&|t| t.columns[1].storage_ty = db::Type::VarChar(10)),
            "column `users.name`: changing a column's type is not supported",
        ),
        (
            base.clone(),
            with(&|t| t.columns[2].nullable = false),
            "column `users.nick`: adding NOT NULL to an existing column is not supported",
        ),
        (
            base.clone(),
            with(&|t| t.columns.push(col(3, "age", db::Type::Integer(8), false))),
            "column `users.age`: added columns cannot be NOT NULL",
        ),
        (
            base.clone(),
            with(&|t| {
                let mut c = col(3, "status", db::Type::Text, true);
                c.storage_ty = db::Type::Enum(db::TypeEnum {
                    name: Some("status".to_string()),
                    variants: vec![],
                });
                t.columns.push(c)
            }),
            "column `users.status`: added columns cannot have a CHECK or identity",
        ),
        (
            base.clone(),
            with(&|t| {
                let mut c = col(3, "seq", db::Type::Integer(8), true);
                c.auto_increment = true;
                t.columns.push(c)
            }),
            "column `users.seq`: added columns cannot have a CHECK or identity",
        ),
        (
            base.clone(),
            with(&|t| {
                t.columns
                    .push(col(3, "price", db::Type::Numeric(None), true))
            }),
            "column `users.price`: NUMERIC without precision",
        ),
        (
            base.clone(),
            with(&|t| {
                t.columns.remove(0);
                t.columns[0].primary_key = true;
                t.primary_key.columns = vec![t.columns[0].id];
            }),
            "column `users.id`: dropping a primary-key column is not supported",
        ),
        (
            base.clone(),
            with(&|t| t.columns[2].primary_key = true),
            "column `users.nick`: this change needs a table rebuild",
        ),
        (
            base.clone(),
            with(&|t| {
                t.indices.push(index(
                    1,
                    "index_users_by_nick",
                    2,
                    db::IndexOp::Sort(stmt::Direction::Asc),
                ))
            }),
            "index `index_users_by_nick` on table `users`: index key sort order is not supported",
        ),
        (
            with(&|t| t.columns[1].storage_ty = db::Type::Integer(4)),
            with(&|t| {
                t.columns[1].storage_ty = db::Type::Integer(4);
                t.columns[1].auto_increment = true;
            }),
            "column `users.name`: identity columns must be 64-bit",
        ),
        (
            db::Schema::default(),
            {
                let mut s = users();
                s.tables[0].columns[0].storage_ty = db::Type::Integer(4);
                s.tables[0].columns[0].auto_increment = true;
                s
            },
            "column `users.id`: identity columns must be 64-bit",
        ),
    ];

    let hints = diff::RenameHints::new();
    for (previous, next, message) in &cases {
        let schema_diff = diff::Schema::from(previous, next, &hints);
        let err = validate_migration(&schema_diff, &Capability::AURORA_DSQL).unwrap_err();
        assert!(err.is_unsupported_feature(), "{err}");
        assert!(
            err.to_string().contains(message),
            "expected `{message}`, got: {err}"
        );

        // Plain PostgreSQL accepts every change.
        validate_migration(&schema_diff, &Capability::POSTGRESQL).unwrap();
    }
}

#[test]
fn allowed_changes_get_one_statement_per_breakpoint() {
    let previous = users();
    let mut next = schema(
        "people",
        vec![
            col(0, "id", db::Type::Integer(8), false),
            col(1, "full_name", db::Type::Text, false),
            col(2, "email", db::Type::Text, true),
        ],
        vec![index(0, "index_people_by_email", 2, db::IndexOp::Eq)],
    );
    next.tables[0].columns[1].nullable = true;
    next.tables[0].columns[0].auto_increment = true;

    let mut hints = diff::RenameHints::new();
    hints.add_table_hint(db::TableId(0), db::TableId(0));
    hints.add_column_hint(next.tables[0].columns[1].id, next.tables[0].columns[1].id);
    let schema_diff = diff::Schema::from(&previous, &next, &hints);

    validate_migration(&schema_diff, &Capability::AURORA_DSQL).unwrap();
    let migration = generate_migration(&schema_diff, &Capability::AURORA_DSQL);
    assert_eq!(
        migration.statements(),
        [
            "ALTER TABLE \"users\" RENAME TO \"people\";",
            "DROP INDEX \"index_users_by_name\";",
            "ALTER TABLE \"people\" ALTER COLUMN \"id\" ADD GENERATED BY DEFAULT AS IDENTITY (CACHE 65536);",
            "ALTER TABLE \"people\" ALTER COLUMN \"name\" DROP NOT NULL;",
            "ALTER TABLE \"people\" RENAME COLUMN \"name\" TO \"full_name\";",
            "ALTER TABLE \"people\" DROP COLUMN \"nick\";",
            "ALTER TABLE \"people\" ADD COLUMN \"email\" TEXT;",
            "CREATE INDEX ASYNC \"index_people_by_email\" ON \"people\" (\"email\");",
        ]
    );
}
