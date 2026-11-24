use crate::{
    Result,
    db::Driver,
    schema::{db, diff},
};

use super::Snapshot;

/// A generated database migration and the schema snapshot it advances to.
///
/// This is the reusable core of migration generation. It deliberately does not
/// include filenames, history IDs, or persistence decisions; callers own those
/// policies.
#[derive(Debug)]
pub struct Generated {
    /// The driver-specific migration statements.
    pub migration: db::Migration,

    /// Snapshot of the schema after the migration is applied.
    pub snapshot: Snapshot,
}

/// Generate a database migration from `previous` to `next`.
///
/// Returns `Ok(None)` when the schemas are equivalent after applying
/// `rename_hints`, and an error when the driver cannot apply the change (see
/// [`Driver::validate_migration`]).
pub fn generate(
    driver: &dyn Driver,
    previous: &db::Schema,
    next: &db::Schema,
    rename_hints: &diff::RenameHints,
) -> Result<Option<Generated>> {
    let schema_diff = diff::Schema::from(previous, next, rename_hints);

    if schema_diff.is_empty() {
        return Ok(None);
    }

    driver.validate_migration(&schema_diff)?;

    Ok(Some(Generated {
        migration: driver.generate_migration(&schema_diff),
        snapshot: Snapshot::new(next.clone()),
    }))
}
