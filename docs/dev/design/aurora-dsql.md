# Aurora DSQL support

## Summary

Amazon Aurora DSQL speaks the PostgreSQL wire protocol but rejects much of the
DDL and some of the transaction control that Toasty's PostgreSQL driver emits.
This design adds an opt-in DSQL profile to the existing PostgreSQL driver,
`PostgreSQL::aurora_dsql()`. With the profile selected, every Toasty feature
either works on DSQL or is rejected with `unsupported_feature` before any
statement runs. DSQL's runtime limits — transaction size, duplicate data
during a unique index build, a migration failing partway — still surface as
DSQL errors. The design also adds a per-connection password provider for IAM
tokens and opt-in retry of optimistic-concurrency (OCC) conflicts. Plain
PostgreSQL users see one behavior change: committing a transaction that a
failed statement already aborted now returns an error instead of `Ok`.

## Motivation

[#1329] asks for Toasty on DSQL. Pointing today's PostgreSQL driver at a DSQL
cluster fails in these ways:

- `push_schema` fails on the first secondary index. DSQL accepts only
  `CREATE INDEX ASYNC`, and an index is not usable until its build job
  finishes: `ON CONFLICT (email)` fails with `42P10` while a unique index is
  still building.
- Migrations fail. DSQL allows one DDL statement per transaction and no DML in
  the same transaction as DDL; Toasty applies a migration and its history row
  in one transaction.
- Enum fields fail (`CREATE TYPE` is not supported), `Vec<scalar>` fields fail
  (no array columns), and auto-increment keys fail (identity columns require
  an explicit `CACHE`).
- `rust_decimal::Decimal` fields silently lose digits: a bare `NUMERIC` column
  on DSQL is `numeric(18,6)`.
- Nested transactions and multi-statement plans inside a transaction fail,
  because `SAVEPOINT` is rejected and aborts the transaction. A transaction
  started with any isolation level other than `REPEATABLE READ` fails at
  `BEGIN`.
- IAM auth tokens expire after 15 minutes by default, up to one week, and are
  checked only at connect time. The driver reads the password once from the
  URL, so connections the pool opens later fail to authenticate ([#1272]).
- DSQL uses optimistic concurrency. Conflicting writes, and DDL on other
  sessions, fail with `40001` at statement or commit time. Applications have
  no way to retry them short of wrapping every call.

A prototype of this design ran Toasty's integration suite against a live DSQL
cluster: 861 of 878 tests passed. All 17 failures are explained: savepoints
(8), `Vec` fields (6), isolation levels (2), and non-consecutive identity ids
(1, from tests run in parallel; the test passes unchanged when run serially).

## User-facing API

### Connecting

Build the PostgreSQL driver from a DSQL endpoint, select the DSQL profile, and
pass a password provider that returns a fresh IAM token:

```rust
use std::time::Duration;
use toasty::db::RetryPolicy;
use toasty_driver_postgresql::PostgreSQL;

let driver = PostgreSQL::new(
    "postgresql://admin@abc123.dsql.us-east-1.on.aws/postgres\
     ?sslmode=verify-full&sslrootcert=system",
)?
.aurora_dsql()
.password_provider(|| async { sign_dsql_token().await });

let mut db = toasty::Db::builder()
    .models(toasty::models!(crate::*))
    .pool_max_connection_lifetime(Some(Duration::from_secs(50 * 60)))
    .occ_retry(RetryPolicy::default())
    .build(driver)
    .await?;
```

`aurora_dsql()` is the only switch. Toasty never inspects the hostname, so a
DSQL cluster behind a custom domain or proxy works the same way, and a plain
PostgreSQL URL keeps today's behavior.

The password provider runs once for every new connection: the first one,
each connection the pool adds or replaces, and the connection `reset_db`
opens. It does not run when the pool hands out an existing connection.
Its result replaces any password in the URL. Signing the token is application
code — for example `aws_sdk_dsql::auth_token::AuthTokenGenerator` — so Toasty
does not depend on the AWS SDK. `pool_create_timeout` bounds how long the
provider may take.

DSQL closes connections after 60 minutes. Set
`pool_max_connection_lifetime` below that (50–55 minutes) so the pool retires
connections before the server does. A connection that reaches 60 minutes
anyway fails the statement running on it with `connection_lost`; the pool then
opens a new connection, with a fresh token from the provider.

Two driver options tune DSQL-only behavior:

```rust
let driver = PostgreSQL::new(url)?
    .aurora_dsql()
    .identity_cache(1)?                                // default 65_536
    .schema_job_timeout(Duration::from_secs(30 * 60))?; // default 5 minutes
```

`identity_cache` sets the `CACHE` of identity columns that `push_schema`
creates. DSQL accepts 1 or a value of at least 65,536. Migrations are generated
without a driver (see Migrations), so they always use `CACHE 65536`; edit the
migration file to change it. `schema_job_timeout` bounds the wait for each
asynchronous index build (see below) and must be greater than zero. Both
options return `invalid_driver_configuration` for an invalid value or when
called without `aurora_dsql()` first.

### Creating the schema

`db.push_schema()` works unchanged. On DSQL the driver runs each table and
each index as its own statement and creates indexes with
`CREATE [UNIQUE] INDEX ASYNC`. It then waits for each index build job to
finish. When `push_schema` returns `Ok`, every `#[unique]` and `#[index]` is
built and usable, so an upsert right after it succeeds.

### Types

Most fields map to the same column types as on PostgreSQL. The differences:

| Field | Column on DSQL |
|---|---|
| Unit enum (`#[derive(toasty::Embed)]`) with string labels | `TEXT` with `CHECK (col IN (...))` instead of a named enum type |
| `rust_decimal::Decimal` | `numeric(57,28)`, which holds every `Decimal` exactly |
| `#[column(type = numeric)]` without precision | Rejected before any DDL runs: DSQL reads bare `NUMERIC` as `numeric(18,6)` and rounds to 6 decimal places |
| `String` with `#[column(type = varchar(n))]` | `varchar(n)`; `n` above 65,535 is rejected (DSQL counts bytes) |
| `Vec<T>` of scalars | Rejected when the schema is built |
| `#[auto(increment)]` key | `bigint GENERATED BY DEFAULT AS IDENTITY (CACHE 65536)` |

Enums read and write the same Rust values; only the storage differs. Decimals
keep every value but read back with scale 28 (`1.5` reads as
`1.5000000000000000000000000000`), so `to_string()`, serde output, and
`scale()` differ from plain PostgreSQL.

Auto-increment keys must be stored as `bigint` (`i64`, `u64`, or `u32`,
which PostgreSQL stores as `bigint`); narrower ones (`i16`, `i32`, `u16`) are
rejected in `push_schema` and when a DSQL migration is generated, before any
statement runs. DSQL hands out identity values in blocks of `CACHE` per
connection, so ids are unique, and consecutive within one connection, but not
across connections: a row inserted on another connection may get id 65,537.
Prefer UUID keys, which also spread writes across DSQL's storage:

```rust
#[derive(Debug, toasty::Model)]
struct User {
    #[key]
    #[auto(uuid(v4))]
    id: uuid::Uuid,

    #[unique]
    email: String,

    balance: i64,
}
```

### Transactions

Transactions work as on PostgreSQL, with two limits.

DSQL has no savepoints, so a nested transaction returns `unsupported_feature`:

```rust
let mut tx = db.transaction().await?;
let nested = tx.transaction().await; // Err: nested transactions require savepoints
```

Operations that need several statements — creating a record with
associations, cascading deletes, `include()` — still work inside a
transaction. Their statements run directly in your transaction; if one of
these operations fails, `commit()` returns an error (see Edge cases).

DSQL runs every transaction at `REPEATABLE READ` (snapshot isolation). A
transaction built with another isolation level runs at `REPEATABLE READ`, and
Toasty logs a warning naming the requested level:

```rust
let mut tx = db
    .transaction_builder()
    .isolation(IsolationLevel::Serializable) // warns, runs at REPEATABLE READ
    .begin()
    .await?;
```

Under snapshot isolation, write skew is possible. Where two transactions must
not both act on the same read, read the rows with a raw SQL
`SELECT ... FOR UPDATE` in the transaction.

### Retrying conflicts

DSQL reports conflicts as `serialization_failure`, at commit or on a
statement, such as the first statement after another session changed the
schema. Two opt-in APIs retry them.

`Db::builder().occ_retry(policy)` retries any call made outside a transaction
— `create!`, `update`, `delete`, queries — when it fails with a serialization
failure. This covers calls on the `Db` and on a `Connection` from
`db.connection()`. The failed attempt did not commit, so re-running it is
safe. Calls made inside a transaction are never retried one by one.

`transaction_with_retry` runs a closure in a transaction and re-runs the whole
closure when the transaction hits a serialization failure, including at
commit:

```rust
db.transaction_with_retry(async |tx| {
    let mut user = User::get_by_email(tx, "a@example.com").await?;
    let balance = user.balance - 10;
    user.update().balance(balance).exec(tx).await?;
    Ok(())
})
.await?;
```

The closure returns `toasty::Result<T>` and may run more than once. Keep
effects outside the database — sending email, incrementing in-memory counters
— out of it, or make them safe to repeat. In a `Send` context, such as
`tokio::spawn`, only an `async move` closure that owns its captures compiles.
`Connection` has the same method. It uses the policy given to `occ_retry`, or
`RetryPolicy::default()` when none was set.

`RetryPolicy::default()` makes 3 retries with exponential backoff from 50 ms
to 5 s and jitter. Adjust it with setters:

```rust
let policy = RetryPolicy::default()
    .max_retries(5)
    .base_delay(Duration::from_millis(20))
    .max_delay(Duration::from_secs(2))
    .on_retry(|retry, err| tracing::info!(retry, %err, "retrying"));
```

When every attempt fails, the call returns `retries_exhausted`; the last
serialization failure is its source. Both APIs work with any driver that
reports `serialization_failure`, including plain PostgreSQL at `SERIALIZABLE`.

Only serialization failures are retried. A closure that catches the error of
a multi-statement operation and returns `Ok` gets `driver_operation_failed`
from the commit (see Edge cases), which is not retried. Raw SQL statements are
never retried, and a transaction started with raw SQL `BEGIN` is not seen as a
transaction by `occ_retry`.

### Migrations

Migrations are generated without a database connection, from a capability:
`toasty migrate generate --flavor aurora-dsql` (or `flavor = "aurora-dsql"`
in `Toasty.toml`) selects `Capability::AURORA_DSQL`. DSQL migrations contain
one statement per step, separated by `-- #[toasty::breakpoint]`, build indexes
with `CREATE [UNIQUE] INDEX ASYNC`, and give identity columns `CACHE 65536`
(`identity_cache` affects `push_schema` only).

`toasty migrate apply` and `toasty migrate reset` apply them. With the
`aurora-dsql` flavor (`--flavor` or `Toasty.toml`), these commands build the
PostgreSQL driver with `aurora_dsql()`, so they take the same DSQL path as an
application: each statement in its own transaction, a wait for each index
job, and `reset_db` dropping every table in the current schema. The command
takes the IAM token as the URL's password and has no password provider; it
opens its connections when it starts (`reset` opens another one, for
`reset_db`, after its confirmation prompt), so a token signed just before the
command is enough. Without that flavor, the commands
connect from the URL alone, as before.

An application can apply them itself instead, on a `Db` whose driver has
`aurora_dsql()` and the password provider, with embedded migrations:

```rust
static MIGRATIONS: toasty::migration::MigrationSet = toasty::embed_migrations!();

MIGRATIONS.apply(&db).await?;
```

DSQL cannot apply some schema changes. For the DSQL flavor, migration
generation rejects them with `unsupported_feature` before it writes any file,
naming the table and column:

| Change | Workaround |
|---|---|
| Change a column's type (including enum variants) | Recreate the table |
| Make a nullable column `NOT NULL` | Keep it nullable; enforce in code |
| Add a `NOT NULL` column | Declare it `Option<T>`; it stays nullable |
| Add an enum or auto-increment column | Recreate the table |
| Drop a primary-key column | Recreate the table |
| Any change that rebuilds a table | Create a new table and copy the data |

Supported changes include adding nullable columns, dropping columns, renaming
tables and columns, adding and dropping indexes, dropping `NOT NULL`, and
adding or removing auto-increment on an existing `bigint` column.

#### Before and after

`toasty::migration::generate` now checks that the target can apply the diff,
so it returns a `Result`. Code that calls it directly adds a `?`:

```rust
// Before
let Some(generated) = migration::generate(capability, &previous, &next, &hints) else {
    return Ok(());
};

// After
let Some(generated) = migration::generate(capability, &previous, &next, &hints)? else {
    return Ok(());
};
```

## Behavior

**Schema execution.** On the DSQL profile, `push_schema`, applying a
migration, and `reset_db` run each statement alone in autocommit mode. When a
statement starts an index build, the driver waits for the job, with one
`CALL sys.wait_for_job` bounded by `schema_job_timeout`, and continues only if
it succeeded. Applying a migration records it in `__toasty_migrations`
after all its statements succeed, in a separate transaction. `reset_db` drops
every table in the current schema, one statement each; DSQL has a single
database and no `CREATE DATABASE`.

**Schema-step retry.** A schema statement, job wait, or migration-history
read or insert that DSQL rejects with `40001` is retried: 3 retries, backoff
from 50 ms to 5 s with jitter, and a warning per retry. This is on by default
and needs no `occ_retry`. DSQL raises these conflicts even when one process
runs DDL serially, and a rejected autocommit statement did not apply. Other
errors and lost connections are not retried.

**Errors.**

| Situation | Error |
|---|---|
| Unsupported type (`Vec<T>`, `varchar` over 65,535) | `unsupported_feature` when the schema is built, before any statement runs |
| Auto-increment key not stored as `bigint`, `numeric` without precision, or an index column with a sort order | `unsupported_feature` from the DSQL check in `push_schema` or migration generation, before any statement runs |
| Unsupported schema change in a migration | `unsupported_feature` from migration generation; nothing written or run |
| Nested transaction | `unsupported_feature` |
| Index build job fails or exceeds `schema_job_timeout` | `driver_operation_failed` naming the job id, DSQL's details, and the statement; for a failed job, also the `DROP INDEX` statement for the `INVALID` index |
| `40001` during a schema step, after the last retry | `retries_exhausted`, with the last `40001` as source; when applying a migration, the error also names the migration, the step, and its SQL |
| `0A000` while applying a migration | `driver_operation_failed` naming the migration, the step, and its SQL, with a hint to regenerate the migration with `--flavor aurora-dsql` |
| `40001` outside a transaction | Retried when `occ_retry` is set, otherwise `serialization_failure` |
| `40001` inside a transaction | `serialization_failure`; `transaction_with_retry` re-runs the closure |
| `occ_retry` or `transaction_with_retry` attempts used up | `retries_exhausted`, with the last error as source |
| Isolation level other than `RepeatableRead` | No error; runs at `REPEATABLE READ` with a warning |
| Password provider fails | `driver_operation_failed`: `PostgreSQL password provider failed` |

Password provider errors use a fixed message and drop the provider's own error
text, which may contain a token. Tokens from the provider never appear in
`Debug` output, `Driver::url()`, traces, or errors. A static password in the
URL is masked in `Debug` only when it is the userinfo password
(`user:password@`); a `?password=` query value is not masked.

**Commit after an aborted transaction (all PostgreSQL users).** When a
statement in a PostgreSQL transaction fails, the server aborts the
transaction, and a later `COMMIT` rolls it back. Toasty returns `Ok` from
`commit()` in that case today, so code that ignored the statement error
believes its other writes were saved. With this design, `commit()` rolls back
and returns an error saying an earlier error aborted the transaction, on
PostgreSQL and DSQL. A rolled-back nested transaction
(`ROLLBACK TO SAVEPOINT`) clears the aborted state on PostgreSQL, as before.
The driver tracks this per connection: a server error inside a transaction
marks it aborted, and `BEGIN`, `COMMIT`, `ROLLBACK`, or `ROLLBACK TO
SAVEPOINT` clears the mark. This fix lands first, in its own PR with a
PostgreSQL test.

**Unchanged behavior.** Without `aurora_dsql()`, the PostgreSQL driver emits
the same SQL as today, apart from the commit fix above. SQLite, MySQL, Turso,
and DynamoDB are unchanged. `occ_retry` is off by default.

## Edge cases

**A migration fails halfway.** DSQL cannot run a migration's statements in one
transaction. If step 3 of 5 fails after its retries, steps 1 and 2 stay
applied and the migration is not recorded. The error names the migration, the
failing step, and its SQL. To recover, undo the applied steps by hand, fix the
cause, and apply the migrations again. Toasty does not resume from the failed
step.

**A migration rebuilds a unique index.** Renaming a table, or turning an
index into a unique one, drops the old index before the new `ASYNC` build
starts, in separate transactions. Until the build ends there is no valid
unique index: `ON CONFLICT` fails with `42P10`, and duplicates can get in and
make the build fail. Apply such migrations while writers are stopped. On
PostgreSQL the whole migration is one transaction, so this gap does not
exist; reordering the statements would change every backend's migrations.

**An index build fails.** DSQL leaves the index `INVALID`; a unique one still
rejects duplicate values. A build fails on duplicate data for a unique index,
or on existing rows whose key is over DSQL's key size limit. Toasty does not
drop the index; the error includes the `DROP INDEX` statement to run once the
data is fixed. The statements after the failed one did not run. Re-running
`push_schema` does not help, because its tables exist and it fails on the
first `CREATE TABLE`. When the failed index was the last statement, run it
from the error by hand and wait for its job (`CALL sys.wait_for_job`);
otherwise, in development, `reset_db` and push again. For a migration, the
"fails halfway" recovery applies.

**An index build exceeds `schema_job_timeout`.** The call fails, but the job
keeps running on the server. DSQL ignores cancel requests, so the driver drops
the waiting connection; it never returns that connection to the pool. The
index becomes usable when the job ends on its own, but the statements after
it did not run. Raise the timeout for large tables.

**Migrations not generated for DSQL.** Applying a migration does not re-check
it; generate DSQL migrations with `--flavor aurora-dsql`. A migration
generated for plain PostgreSQL, including one generated before you switched
to DSQL, runs as a single step. When it has more than one statement, or one
that DSQL refuses, DSQL rejects it whole and nothing is applied; when DSQL
reports it as unsupported, the error says to regenerate it with
`--flavor aurora-dsql`. A single statement DSQL accepts applies as written; a
`Decimal` column added that way is a bare `numeric`, which DSQL rounds to 6
decimal places.

**Transaction limits.** DSQL limits a transaction to 3,000 modified rows,
10 MiB of changes, and 5 minutes. Toasty does not split large writes; a
`create_many` over the limit fails with DSQL's error.

**Key size.** DSQL documents a 1 KiB limit on the combined key columns of a
primary key or a secondary index; the server currently enforces a somewhat
larger limit (about 1.9 KiB, which for a secondary index includes the primary
key). A `String` is stored as `text` by default, so a `#[unique]` or
`#[index]` string field is created fine, and a write whose key is over the
limit fails with DSQL's error (`54000 key size too large`). Adding an index to
a table that already holds such values fails the index build, and Toasty
reports it like any failed build.

**Text comparison.** DSQL uses the C collation, so `order_by` on strings sorts
by byte value, and case-insensitive matching (`ILIKE`) folds ASCII letters
only.

**Plans inside a transaction.** Without savepoints, when an operation that
would have used a savepoint fails inside your transaction, Toasty marks the
transaction failed whatever the cause, so `commit()` returns an error without
sending `COMMIT`, and the transaction is rolled back. This includes a
version-checked read-then-write (a `#[version]` update on a backend without
CTE updates; DSQL has them). A failed single-statement operation wrote
nothing.

## Driver integration

**`Capability::savepoints`.** A new `bool` field: whether the backend
supports `SAVEPOINT` inside a transaction. When it is `false`, the engine
rejects nested transactions with `unsupported_feature` and runs the steps of a
multi-statement plan, and of a read-modify-write, directly in the user's
transaction; when such an operation fails, the engine marks that transaction
failed. The mark is kept per connection, in the connection task that also
runs `COMMIT`, `ROLLBACK`, and the rollback on drop, so every driver gets the
same commit error. The in-tree capabilities set it to `true` for SQLite,
PostgreSQL, MySQL, MariaDB, and Turso, and `false` for DynamoDB and Aurora
DSQL. `Capability` is not `#[non_exhaustive]`, so an out-of-tree driver that
builds it field by field must add `savepoints`. A driver that spreads an
existing constant (`..Capability::POSTGRESQL`) inherits the value. Earlier
capability fields, such as `interactive_transactions` ([#1302]), were added
the same way and not marked as breaking.

**`Capability::AURORA_DSQL` and `StorageTypes::AURORA_DSQL`.** New constants:
`POSTGRESQL` without native enums, arrays, `Vec` operations, or arbitrary
precision decimals, and with no in-place column type change and
`savepoints: false`. The storage types cap `varchar` at 65,535 and use
`numeric(57,28)` for decimals. The PostgreSQL driver's `capability()` returns
this profile when `aurora_dsql()` is set.

**Migration generation.** Migrations are generated from a `Capability`, with
no driver involved, so the DSQL migration rules live in `toasty-sql` and apply
when the capability is `Capability::AURORA_DSQL` (matched by its
`driver_name`):

- `toasty_sql::validate_migration(&diff::Schema, &Capability) -> Result<()>`
  is new. It returns `unsupported_feature` naming the first change DSQL cannot
  apply, and `Ok(())` for every other capability.
- `toasty_sql::generate_migration` renders DSQL migrations one statement per
  breakpoint, with `ASYNC` indexes and `CACHE 65536`. The changes DSQL can
  make in place (`DROP NOT NULL`, adding or dropping an identity) render as
  `ALTER` statements, not as a table rebuild.
- `toasty::migration::generate(&Capability, ..) -> Result<Option<Generated>>`
  calls `validate_migration` before `generate_migration`.
- `toasty-cli` adds the `aurora-dsql` flavor (`Flavor::AuroraDsql`), and the
  schema dumper accepts it.

`push_schema` runs the same check, on the diff from an empty schema, so both
paths give the same messages. No `Driver` method is added; wrapper drivers
forward nothing new.

**`toasty_sql::stmt::DdlOptions`.** DDL settings that the PostgreSQL driver
needs for DSQL, passed to new entry points instead of read from `Capability`:

```rust
let options = DdlOptions { async_index: true, identity_cache: Some(65_536) };
let stmt = Statement::create_table_with(table, capability, &options);
let stmt = Statement::create_index_with(index, &options);
let stmts = MigrationStatement::from_diff_with(diff, capability, &options);
```

`DdlOptions::default()` renders today's SQL. The existing `create_table`,
`create_index`, and `from_diff` keep their signatures, and with default options
the `*_with` entry points produce the same statements, so SQL drivers that do
not need these settings change nothing. Code that builds
`toasty_sql::stmt::ColumnDef` or `CreateIndex` with a struct literal must set
the new public fields `identity_cache` and `async_build`; `None` and `false`
render today's SQL.

The remaining DSQL behavior — one statement per autocommit, waiting for index
jobs, schema-step retry, `reset_db`, isolation handling — is private to the
PostgreSQL driver. No new `Operation` variants or `Driver` methods, and no
changes for the SQLite, MySQL, Turso, or DynamoDB drivers beyond the new
`savepoints` field.

## Alternatives considered

**Detect DSQL from the hostname.** `*.dsql.*.on.aws` identifies most clusters,
but not those reached through a proxy, a custom domain, or a tunnel, and the
check would change behavior for a URL the user did not mark. Every AWS DSQL
adapter for other ORMs uses an explicit opt-in as well.

**A separate `toasty-driver-aurora-dsql` crate.** DSQL uses the same protocol,
query serializer, type encoding, and connection pool as PostgreSQL; the
differences are DDL execution and a capability profile. A separate crate would
duplicate the driver or need it to expose its internals.

**Public schema capabilities ([#1334]).** #1334 proposes a public
`SchemaCapabilities` value returned by a new driver method, with
driver-owned adapters for rendering and index readiness. This design keeps
those settings private to the PostgreSQL driver and adds one `Capability`
field that the query engine reads. That is a smaller public surface, and it
also covers query-side differences (enums, arrays, decimals, savepoints) that
#1334 leaves out. If #1334 lands, the driver's private settings become what
its method returns, with no change for users.

**A second `try_generate()` next to `generate`.** That avoids breaking
`toasty::migration::generate`, but callers of the old function would skip
validation and could write a migration DSQL cannot apply. DSQL runs each DDL
statement in its own transaction, so such a migration stops partway and leaves
the schema half-changed. The check needs the schema diff, which only exists at
generation time. Changing the return type makes every caller validate. Toasty
breaks API routinely before 1.0, and the only in-tree caller is `toasty-cli`.

**Validate migrations in the driver.** A `Driver` method that checks the diff
would keep DSQL's rules in the PostgreSQL driver, but migrations are generated
from a `Capability` with no driver: the CLI reads the schema from the user's
binary for the chosen `--flavor`. Such a method would have no caller, so the
rules live in `toasty-sql`, keyed by the capability.

**Always-on OCC retry.** The prototype retried every failed statement outside
a transaction automatically. Retrying changes timing and hides contention, and
applications on plain PostgreSQL do not expect it, so application retry is
opt-in through `occ_retry` and `transaction_with_retry`. Schema steps retry
by default (see Behavior), because a migration that fails on a transient
conflict leaves the schema half-applied.

**Reject isolation levels other than `REPEATABLE READ`.** That is accurate,
but it fails every transaction built with an isolation level, including code
shared with plain PostgreSQL. Running the transaction at `REPEATABLE READ`
with a warning matches the EF Core DSQL adapter. The cost is that a
`Serializable` request gets snapshot isolation; the warning and the guide say
so.

**Sign IAM tokens inside Toasty.** Building token signing into the driver
needs the AWS SDK as a dependency and Toasty-specific credential settings.
`password_provider` ([#1332]) keeps signing in application code and also
serves Aurora PostgreSQL IAM auth and other rotating credentials.

## Open questions

- **Coordination with [#1334].** This design overlaps #1334 on the DSQL
  profile, async indexes, one DDL per transaction, and migration preflight,
  and differs on schema-step retry (on by default here, opt-in in #1334) and
  breakpoint-separated migrations (accepted here, replaced by plan manifests
  in #1334). The two need one agreed shape. *Blocking acceptance.*
- **Migration resume and plan files.** Recording per-step progress so
  `migration apply` can continue after a partial failure, and versioned plan
  manifests, as in #1334's later phase. *Deferrable.*
- **An in-tree `dsql` feature for token signing.** An optional feature or
  adapter crate that provides a ready-made `password_provider` using the AWS
  SDK. *Deferrable.*
- **`Vec<T>` as JSONB.** DSQL has no arrays but supports `jsonb`; storing
  `Vec<scalar>` there would make those fields work. *Deferrable.*
- **Retry with transaction options.** `transaction_with_retry` cannot set
  isolation, read-only, or lock mode, and takes a custom policy only from
  `occ_retry`, which also turns on per-statement retry. That link stays; a
  `TransactionBuilder` variant (for example `run_with_retry`) taking its own
  options and policy would be a new method. *Deferrable.*

## Out of scope

- **Changing enum variants on existing tables.** Needs a column type change,
  which DSQL does not support.
- **Splitting writes to fit DSQL's 3,000-row transaction limit.** Applications
  batch large writes themselves.
- **Foreign keys.** Toasty does not emit foreign key constraints on any
  backend.

[#1272]: https://github.com/tokio-rs/toasty/issues/1272
[#1302]: https://github.com/tokio-rs/toasty/pull/1302
[#1329]: https://github.com/tokio-rs/toasty/issues/1329
[#1332]: https://github.com/tokio-rs/toasty/pull/1332
[#1334]: https://github.com/tokio-rs/toasty/pull/1334
