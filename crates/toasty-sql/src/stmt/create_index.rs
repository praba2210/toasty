use super::{DdlOptions, Statement};

use toasty_core::{
    schema::db::{Index, IndexId, IndexOp, TableId},
    stmt,
};

/// A `CREATE INDEX` statement.
#[derive(Debug, Clone)]
pub struct CreateIndex {
    /// Name of the index
    pub index: IndexId,

    /// Which table to index
    pub on: TableId,

    /// The columns to index
    pub columns: Vec<stmt::OrderByExpr>,

    /// When true, the index is unique
    pub unique: bool,

    /// When true, the index is built by a background job
    /// (`CREATE INDEX ASYNC`), from [`DdlOptions::async_index`].
    pub async_build: bool,
}

impl Statement {
    /// Creates a `CREATE INDEX` statement from a schema [`Index`].
    pub fn create_index(index: &Index) -> Self {
        Self::create_index_with(index, &DdlOptions::default())
    }

    /// Creates a `CREATE INDEX` statement rendered with driver [`DdlOptions`].
    pub fn create_index_with(index: &Index, options: &DdlOptions) -> Self {
        CreateIndex {
            index: index.id,
            on: index.on,
            columns: index
                .columns
                .iter()
                .map(|index_column| stmt::OrderByExpr {
                    expr: stmt::Expr::column(index_column.column),
                    order: match index_column.op {
                        IndexOp::Eq => None,
                        IndexOp::Sort(direction) => Some(direction),
                    },
                })
                .collect(),
            unique: index.unique,
            async_build: options.async_index,
        }
        .into()
    }
}

impl From<CreateIndex> for Statement {
    fn from(value: CreateIndex) -> Self {
        Self::CreateIndex(value)
    }
}
