# Aurora DSQL

[Amazon Aurora DSQL] speaks the PostgreSQL wire protocol, so Toasty reaches it
through the [PostgreSQL driver](./postgresql.md). DSQL differs from PostgreSQL
in its DDL, its transaction control, and its concurrency model. Calling
`aurora_dsql()` on the driver selects a DSQL profile: every Toasty feature then
either works on DSQL or is rejected with an `unsupported_feature` error before
any statement runs.

[Amazon Aurora DSQL]: https://aws.amazon.com/rds/aurora/dsql/

## Connecting

Enable the `postgresql` feature as for PostgreSQL:

```toml
[dependencies]
toasty = { version = "{{toasty_version}}", features = ["postgresql"] }
toasty-driver-postgresql = { version = "{{toasty_version}}" }
```

Build the driver from the cluster endpoint, select the DSQL profile, and pass
it to `Db::builder().build()`:

```rust,ignore
# async fn generate_auth_token() -> Result<String, std::io::Error> { todo!() }
# async fn __example() -> toasty::Result<()> {
use std::time::Duration;
use toasty_driver_postgresql::PostgreSQL;

let driver = PostgreSQL::new(
    "postgresql://admin@abc123.dsql.us-east-1.on.aws/postgres\
     ?sslmode=verify-full&sslrootcert=system",
)?
.aurora_dsql()
.password_provider(|| async { generate_auth_token().await });

let mut db = toasty::Db::builder()
    .models(toasty::models!(crate::*))
    .pool_max_connection_lifetime(Some(Duration::from_secs(50 * 60)))
    .build(driver)
    .await?;
# Ok(())
# }
```

`aurora_dsql()` is the only switch. Toasty does not look at the host name, so a
cluster behind a custom domain or a proxy works the same way, and without the
call the driver behaves as plain PostgreSQL. `Db::builder().connect(url)` has
no way to select the profile; build the driver yourself. The `toasty` command
selects it with the `aurora-dsql` flavor (see [Migrations](#migrations)).

**TLS.** DSQL requires TLS. Use `sslmode=verify-full` with
`sslrootcert=system` to verify the server certificate against the operating
system's trust store (see [TLS modes](./postgresql.md#tls-modes)). Always set
`sslmode=verify-full`: without `sslmode`, the default `prefer` does not verify
the server, so the token goes to whichever server answers.

**Authentication tokens.** DSQL authenticates with IAM authentication tokens
in place of a password. A token is checked only when a connection opens, and
it expires (after 15 minutes by default), so a fixed password in the URL stops
working for the connections the pool opens later. `password_provider` takes a
function that returns a fresh token. It runs once for every connection the
driver opens: the first one, each connection the pool adds or replaces, and the
connection `reset_db` opens. It does not run when the pool reuses an open
connection. Its result replaces any password in the URL.

Toasty does not sign tokens itself. The provider is your code, typically a call
to the AWS SDK's DSQL auth token generator, which signs with your AWS
credentials and region. Load the SDK configuration and build the generator
once, and only sign in the provider, so that a new connection does not resolve
credentials again. This example needs the `aws-config` and `aws-sdk-dsql`
dependencies:

```rust,ignore
# async fn __example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
use std::sync::Arc;
use aws_sdk_dsql::auth_token::{AuthTokenGenerator, Config};
use toasty_driver_postgresql::PostgreSQL;

let sdk_config = Arc::new(aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await);
let generator = Arc::new(AuthTokenGenerator::new(
    Config::builder()
        .hostname("abc123.dsql.us-east-1.on.aws")
        .build()?,
));

let driver = PostgreSQL::new(
    "postgresql://app@abc123.dsql.us-east-1.on.aws/postgres\
     ?sslmode=verify-full&sslrootcert=system",
)?
.aurora_dsql()
.password_provider(move || {
    let sdk_config = sdk_config.clone();
    let generator = generator.clone();
    async move {
        let token = generator.db_connect_auth_token(&sdk_config).await?;
        Ok::<_, aws_sdk_dsql::error::BoxError>(token.to_string())
    }
});
# drop(driver);
# Ok(())
# }
```

This signs a token for a custom database role, `app` here, which needs only the
`dsql:DbConnect` IAM permission. The role must exist first: connect as `admin`
and create it, then map it to the IAM role your application runs as:

```sql
CREATE ROLE app WITH LOGIN;
AWS IAM GRANT app TO 'arn:aws:iam::<account>:role/<role>';
```

After the schema exists, give the role access to the tables. `ALL TABLES`
covers only the tables that exist, so run it again after a migration adds one:

```sql
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app;
```

Schema changes, `push_schema` and migrations, need the `admin` role: build a
second driver as above with `admin` in the URL and `db_connect_admin_auth_token`
in the provider (IAM permission `dsql:DbConnectAdmin`), and run the application
as `app`.

The provider returns `Result<String, E>` for any error type that converts
into `Box<dyn Error + Send + Sync>`. When it fails, connecting fails with
`driver_operation_failed` and the message `PostgreSQL password provider
failed`; the provider's own error is dropped, since it may contain a token.
`pool_create_timeout` bounds how long the provider and the connect may take.

**Connection lifetime.** DSQL closes every connection after 60 minutes. Set
`pool_max_connection_lifetime` below that, as above (50–55 minutes), so the
pool retires connections before the server does. A connection that reaches
60 minutes anyway fails the statement running on it with a "connection lost"
error; the pool then replaces the connection, with a fresh token from the
provider.

## Creating the schema

`db.push_schema()` works on DSQL. The driver runs each table and each index as
its own statement, creates indexes with `CREATE [UNIQUE] INDEX ASYNC`, and
waits for each index build job to finish. When `push_schema` returns `Ok`,
every `#[unique]` and `#[index]` is built and usable, so an upsert right after
it succeeds.

Two driver options tune schema creation. Both return an
`invalid_driver_configuration` error for an invalid value or when
`aurora_dsql()` was not called first:

```rust,ignore
# use std::time::Duration;
# use toasty_driver_postgresql::PostgreSQL;
# fn __example(url: &str) -> toasty::Result<()> {
let driver = PostgreSQL::new(url)?
    .aurora_dsql()
    .identity_cache(1)?                                // default 65_536
    .schema_job_timeout(Duration::from_secs(30 * 60))?; // default 5 minutes
# drop(driver);
# Ok(())
# }
```

- `schema_job_timeout` bounds the wait for each index build, in `push_schema`
  and when applying migrations. On a large table, raise it. When it runs out,
  the call fails but the build keeps running on the server.
- `identity_cache` sets the `CACHE` of the identity columns `push_schema`
  creates for auto-increment keys. DSQL accepts 1 or at least 65,536. It
  affects `push_schema` only: migrations are generated without a driver, so
  they always use `CACHE 65536`.

A schema statement that DSQL rejects with an optimistic-concurrency conflict
(`40001`), which DSQL raises even when only one process runs DDL, is retried
up to 3 times with backoff. When the retries run out, the error is
`retries_exhausted`.

`reset_db` drops every table in the current schema, since DSQL has one
database and no `CREATE DATABASE`.

## Migrations

The `aurora-dsql` flavor of the `toasty` command selects DSQL for migrations.
Pass `--flavor aurora-dsql` to `generate`, `snapshot`, `apply`, and `reset`,
or set it once in the `[migration]` section of `Toasty.toml` in your package
root (the first `toasty migrate generate --flavor aurora-dsql` creates the
file):

```toml
[migration]
path = "toasty"
prefix_style = "Sequential"
flavor = "aurora-dsql"
```

Generate a migration after changing your models:

```bash
toasty migrate generate --flavor aurora-dsql
```

DSQL runs one DDL statement per transaction, so a DSQL migration has one
statement per step, separated by `-- #[toasty::breakpoint]`. Indexes are built
with `CREATE INDEX ASYNC`, and identity columns get `CACHE 65536` whatever
the driver's `identity_cache` is; edit the migration file to use another cache
size.

Apply the migrations with `toasty migrate apply`, or drop every table and
apply them again with `toasty migrate reset`. With the `aurora-dsql` flavor,
these commands connect with the DSQL profile, as `aurora_dsql()` does. They
take the token as the password in the URL, so sign one first and
percent-encode it, since a token contains characters such as `&` and `/`.
With the AWS CLI:

```bash
export DSQL_HOST=abc123.dsql.us-east-1.on.aws
TOKEN=$(aws dsql generate-db-connect-admin-auth-token \
  --hostname "$DSQL_HOST" --region us-east-1)
PASSWORD=$(printf %s "$TOKEN" |
  python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.stdin.read(), safe=""))')
export DATABASE_URL="postgresql://admin:$PASSWORD@$DSQL_HOST/postgres?sslmode=verify-full&sslrootcert=system"

toasty migrate apply --flavor aurora-dsql
```

Put the token in the password part of the URL (`admin:<token>@`), as above,
not in a `password=` query parameter: the commands mask only the former when
they print the URL. An error about a malformed URL, such as one with an
unencoded token, can print the whole URL, token included.

To apply the migrations from your application instead, enable the
`migration` feature and use [`embed_migrations!`](./schema-management.md) on
a `Db` built with the DSQL driver as above:

```rust,ignore
static MIGRATIONS: toasty::migration::MigrationSet = toasty::embed_migrations!();

MIGRATIONS.apply(&db).await?;
```

Both ways, each statement runs on its own, and the driver waits for each
index build. The migration is recorded in `__toasty_migrations` after all its
statements succeed, so the command and the application see the same history.

DSQL cannot make some schema changes. `migrate generate --flavor aurora-dsql`
rejects them with `unsupported_feature`, naming the table and column, before it
writes any file:

| Change | Workaround |
|---|---|
| Change a column's type (including enum variants) | Recreate the table |
| Make a nullable column `NOT NULL` | Keep it nullable; enforce it in code |
| Add a `NOT NULL` column | Declare the field `Option<T>`; it stays nullable |
| Add an enum or auto-increment column | Recreate the table |
| Drop a primary-key column | Recreate the table |
| Any change that rebuilds a table | Create a new table and copy the data |

Supported changes include adding nullable columns, dropping columns, renaming
tables and columns, adding and dropping indexes, dropping `NOT NULL`, and
adding or removing auto-increment on an existing `bigint` column.

**A migration that fails partway stays partly applied.** DSQL cannot run a
migration's statements in one transaction. If step 3 of 5 fails, steps 1 and 2
stay applied and the migration is not recorded. The error names the migration,
the failing step, and its SQL. Undo the applied steps by hand, fix the cause,
and apply the migrations again; Toasty does not resume from the failed step.

A migration that rebuilds a unique index, such as one that renames its table
or turns an `#[index]` into `#[unique]`, drops the old index before it builds
the new one. Until the build ends, the table has no valid unique index: an
upsert fails, and duplicate values can get in and make the build fail. Apply
such a migration while nothing writes to the table.

## Types and unsupported features

Most fields map to the same columns as on [PostgreSQL](./postgresql.md#type-mapping).
The differences:

| Field | On DSQL |
|---|---|
| Embedded unit enum | `TEXT` with a `CHECK (col IN (...))` constraint, instead of a named enum type |
| `rust_decimal::Decimal` | `numeric(57,28)`, which holds every `Decimal` exactly |
| `#[column(type = numeric)]` without precision | Rejected: DSQL reads a bare `NUMERIC` as `numeric(18,6)` and rounds to 6 decimal places. Declare a precision, such as `numeric(38, 10)` |
| `String` with `#[column(type = varchar(n))]` | `varchar(n)`; `n` above 65,535 is rejected |
| [`Vec<scalar>` fields](./vec-scalar-fields.md) | Rejected: DSQL has no array columns |
| `#[auto(increment)]` key | `bigint GENERATED BY DEFAULT AS IDENTITY (CACHE 65536)`; the key must be `i64`, `u64`, or `u32` |
| Index column with a sort order | Rejected |

Enums read and write the same Rust values as on PostgreSQL; only the storage
differs. Decimals keep every value but read back with scale 28 (`1.5` reads
as `1.5000000000000000000000000000`), so `to_string()`, serde output, and
`scale()` differ from plain PostgreSQL.

An auto-increment key on `i16`, `i32`, or `u16` is rejected, since DSQL
identity columns must be `bigint`. DSQL hands out identity values in blocks of
`CACHE` per connection, so ids are unique, and consecutive on one connection,
but not across connections: a row inserted on another connection may get id
65,537. Prefer UUID keys, which also spread writes across DSQL's storage:

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

The rejected fields fail when the `Db` is built or in `push_schema`, before
any statement runs.

## Transactions

Transactions work as on PostgreSQL, with two limits.

**No nested transactions.** DSQL has no savepoints, so starting a transaction
inside a transaction returns `unsupported_feature` ("nested transactions
require savepoints, which Aurora DSQL does not support"):

```rust
# use toasty::Executor;
# async fn __example(mut db: toasty::Db) -> toasty::Result<()> {
let mut tx = db.transaction().await?;
let nested = tx.transaction().await; // Err on Aurora DSQL
# drop(nested);
# Ok(())
# }
```

Operations that need several statements — creating a record with
associations, cascading deletes, `include()` — still work inside a
transaction: their statements run directly in your transaction. If one of
these operations fails, Toasty marks the transaction failed, and `commit()`
returns an error and rolls the transaction back, even when your code ignored
the first error.

**One isolation level.** DSQL runs every transaction at `REPEATABLE READ`
(snapshot isolation). A transaction started with another isolation level
through `transaction_builder().isolation(...)` runs at `REPEATABLE READ`, and
Toasty logs a warning naming the requested level. Under snapshot isolation,
write skew is possible: where two transactions must not both act on the same
read, read the rows with a raw SQL `SELECT ... FOR UPDATE` in the transaction.

**Limits.** DSQL limits a transaction to 3,000 modified rows, 10 MiB of
changes, and 5 minutes. Toasty does not split large writes; a `create_many`
over the limit fails with DSQL's error.

## Optimistic concurrency

DSQL does not lock rows. When two transactions write the same row, or a
statement runs after another session changed the schema, DSQL rejects one of
them with SQLSTATE `40001` (the message carries DSQL's own code, such as
`OC000`), which Toasty reports as a serialization failure
(`Error::is_serialization_failure`). The failed attempt did not commit, so
running it again is safe. Toasty never retries on its own; two opt-in APIs do.

`occ_retry` on the builder retries every call made outside a transaction —
`create!`, updates, deletes, queries — on the `Db` and on a `Connection` from
`db.connection()`:

```rust
use toasty::db::RetryPolicy;

# async fn __example(models: toasty::ModelSet) -> toasty::Result<()> {
let mut builder = toasty::Db::builder();
builder.models(models).occ_retry(RetryPolicy::default());
# Ok(())
# }
```

Calls inside a transaction are never retried one by one. Instead,
`transaction_with_retry` runs a closure in a transaction and re-runs the whole
closure when it, or the commit, fails with a serialization failure:

```rust
# #[derive(Debug, toasty::Model)]
# struct User {
#     #[key]
#     #[auto(uuid(v4))]
#     id: uuid::Uuid,
#     #[unique]
#     email: String,
#     balance: i64,
# }
# async fn __example(mut db: toasty::Db) -> toasty::Result<()> {
db.transaction_with_retry(async |tx| {
    let mut user = User::get_by_email(&mut *tx, "a@example.com").await?;
    let balance = user.balance - 10;
    toasty::update!(user { balance: balance }).exec(&mut *tx).await?;
    Ok(())
})
.await?;
# Ok(())
# }
```

The closure returns `toasty::Result<T>` and may run more than once, so keep
effects outside the database (sending email, updating in-memory state) out of
it, or make them safe to repeat. Where the future must be `Send`, such as in
`tokio::spawn`, write an `async move` closure that owns what it captures.
`Connection` has the same method. Any error other than a serialization failure
rolls back and is returned at once.

A closure that catches the error of a multi-statement operation and returns
`Ok` anyway gets `driver_operation_failed` from the commit ("transaction was
aborted by an earlier error"), which is not retried. Return the error with `?`
so the retry sees it.

`transaction_with_retry` uses the policy given to `occ_retry`, or
`RetryPolicy::default()` when none is set: 3 retries, with exponential backoff
from 50 ms up to 5 s and random jitter. Adjust it with setters:

```rust
use std::time::Duration;
use toasty::db::RetryPolicy;

let policy = RetryPolicy::default()
    .max_retries(5)
    .base_delay(Duration::from_millis(20))
    .max_delay(Duration::from_secs(2))
    .on_retry(|retry, err| eprintln!("retry {retry}: {err}"));
```

When every attempt fails, the call returns an error for which
`is_retries_exhausted()` is true; its source is the last serialization failure.
Raw SQL statements are never retried, and a transaction started with a raw SQL
`BEGIN` is not seen as a transaction, so do not combine one with `occ_retry`.

## Other differences

**Key size.** DSQL documents a 1 KiB limit on the combined key columns of a
primary key or a secondary index; the server currently accepts somewhat more
(about 1.9 KiB, and a secondary index key includes the primary key). A `String`
is `text` by default, so a `#[unique]` or `#[index]` string field is created
fine, and a write whose key is over the limit fails with `54000 key size too
large`.

**Text comparison.** DSQL uses the C collation, so `order_by` on strings sorts
by byte value, and `.ilike()` folds ASCII letters only.

## Troubleshooting

**"Network is unreachable" when connecting.** An expired or invalid token can
show up this way on a host without IPv6. The client tries every address the
endpoint resolves to and reports the error from the last one, which on such a
host is the IPv6 attempt, not DSQL's "access denied" answer. Check that the
password provider returns a fresh token for the right endpoint, region, and
role. A custom role also has to be created `WITH LOGIN` and mapped to your IAM
role with `AWS IAM GRANT` (see [Connecting](#connecting)). For the `toasty`
command, sign a new token for the URL: it expires after 15 minutes by default.

**A statement fails with "connection lost" about an hour after connecting.**
DSQL closed the connection at 60 minutes. Set `pool_max_connection_lifetime`
below 60 minutes.

**An index build fails.** The error names the build job, DSQL's reason (such
as duplicate values for a unique index, or a key over the size limit), and the
statement. DSQL leaves the index `INVALID`, and a unique one still rejects
duplicate values. Toasty does not drop it, and the statements after it did not
run. Running `push_schema` again does not help: the tables exist, so it fails
on the first `CREATE TABLE`. Fix the data and run the `DROP INDEX "<name>"`
statement the error gives. Then run the failed `CREATE INDEX ASYNC` statement
from the error yourself, and wait for the job id it returns with
`CALL sys.wait_for_job('<job id>')`; this is enough only when it was the last
statement. Otherwise, in development, run `db.reset_db()` and `push_schema`
again. For a migration, see [a migration that fails partway](#migrations).

**An index build times out.** The build keeps running on the server and the
index becomes usable when it ends, but the statements after it did not run.
Raise `schema_job_timeout` for large tables. In development, `db.reset_db()`
and run `push_schema` again; for a migration, see
[a migration that fails partway](#migrations).

**A migration fails with SQLSTATE `0A000` (feature not supported).** It was
generated for plain PostgreSQL, for example before you switched to DSQL. The
error says to regenerate it; generate the migration again with
`--flavor aurora-dsql`. A plain PostgreSQL migration runs as one step, so DSQL
rejected it whole and nothing from it was applied.

**`retries_exhausted` from a schema step.** DSQL kept rejecting the step with
`40001`, usually because other sessions changed the schema at the same time.
Run the schema change again when no other DDL is running.
