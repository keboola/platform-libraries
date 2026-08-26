# Input Mapping

Input mapping library for Keboola Runner and Workspaces.
Downloads tables and files from Keboola Storage API: tables can be exported to CSV or Parquet, files are downloaded as-is. Creates manifests and tracks incremental file state.
Supports staging via Local, S3, Azure Blob Storage, and loading into Snowflake/BigQuery workspaces.

## Job log messages

A table or file load emits its messages at two levels, and the split is deliberate:

- **`info`** — the stable, greppable contract. These messages are unchanged and are only ever extended
  by appending a *new* message, never by rewriting an existing one: `Processing N local table exports.`,
  `Processing N S3 table exports.`, `Processing N ABS table exports.`, `Processed N workspace exports.`,
  `Fetched table <source>.`, `All tables were fetched.`, `Fetched file "<name>".`,
  `All files were fetched.`
- **`debug`** — every timing, size, throughput and Storage job/file identifier. This is the detail that
  makes a slow load self-explanatory. It is only visible when the consumer's log handler passes debug
  through; with an info-level handler the job log looks exactly as it did before.

A `local` load of two tables produces this interleaved sequence (`I` = info, `D` = debug):

```
I  Processing 2 local table exports.
D  Queued 2 table exports in 0.42 s.
D  Waiting for 2 storage jobs to finish.
D  2 storage jobs finished in 3.10 s.
D  Downloading 2 exported tables.
D  Fetching table in.c-bucket.t1 (export job 1323965640, file 1234567).
I  Fetched table in.c-bucket.t1.
D  Fetched table in.c-bucket.t1. Downloaded 120.5 MB in 12.34 s (9.8 MB/s), export job 1323965640, file 1234567.
D  Fetching table in.c-bucket.t2 (export job 1323965641, file 1234568).
I  Fetched table in.c-bucket.t2.
D  Fetched table in.c-bucket.t2. Downloaded 120.4 MB in 12.67 s (9.5 MB/s), export job 1323965641, file 1234568.
D  Downloaded 2 tables, 240.9 MB in 25.01 s (9.6 MB/s).
D  Wrote 2 table manifests in 0.01 s.
I  All tables were fetched.
D  All tables were fetched. 2 tables in 28.13 s.
```

Each debug line repeats the identifier it belongs to (table source, file name) so that it still reads on
its own — it is a standalone message, not a continuation of the info line above it.

The `S3` and `ABS` strategies emit the same phase lines without per-table download timing (they only
fetch file credentials and write manifests); their per-table debug line is
`Fetched table <source>. Export job <id>, file <id>.` The `Snowflake` and `BigQuery` workspace
strategies emit `Processed N workspace exports.` plus one `Fetched table <source>.` per table, with no
per-table debug detail — a single load job covers every table.

File downloads follow the same pattern: `Fetched file "<name>".` at info, and
`Fetched file "<name>". Downloaded S in X (T/s).` plus `All files were fetched. N files, S in X.` at
debug.

## Development

### Prepare local environment

Create `.env.local` file from this `.env` template and fill the required environment variables:

```shell
cp .env .env.local
```

### Prepare resources

You need to provide the following environment variables:

* `STORAGE_API_URL` - The Keboola Storage API URL (e.g., `https://connection.keboola.com`)
* `STORAGE_API_TOKEN` - A non-admin token with "Full Access" to Files, Components & Buckets and Trash
* `STORAGE_API_TOKEN_MASTER` - An admin user token from the same project (with role `admin`)

### Available composer commands

**Development commands:**
* `composer phpcs` - Check code style
* `composer phpcbf` - Automatically fix code style issues
* `composer phpstan` - Run static analysis

**Testing commands:**
* `composer tests` - Run tests with PHPUnit
* `composer paratests` - Run tests in parallel with Paratest

## License

MIT licensed, see [LICENSE](./LICENSE) file.
