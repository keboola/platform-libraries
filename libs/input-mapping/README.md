# Input Mapping

Input mapping library for Keboola Runner and Workspaces.
Downloads tables and files from Keboola Storage API: tables can be exported to CSV or Parquet, files are downloaded as-is. Creates manifests and tracks incremental file state.
Supports staging via Local, S3, Azure Blob Storage, and loading into Snowflake/BigQuery workspaces.

## Job log messages

A table or file load logs each phase as it finishes, so a slow load is self-explanatory from the job log
alone. A `local` load of two tables produces this sequence:

```
Processing 2 local table exports.
Queued 2 table exports in 0.42 s.
Waiting for 2 storage jobs to finish.
2 storage jobs finished in 3.10 s.
Downloading 2 exported tables.
Fetching table in.c-bucket.t1 (export job 1323965640, file 1234567).
Fetched table in.c-bucket.t1. Downloaded 126353408 bytes in 12.34 s, export job 1323965640, file 1234567.
Fetching table in.c-bucket.t2 (export job 1323965641, file 1234568).
Fetched table in.c-bucket.t2. Downloaded 126248960 bytes in 12.67 s, export job 1323965641, file 1234568.
Downloaded 2 tables, 252602368 bytes in 25.01 s.
Wrote 2 table manifests in 0.01 s.
All tables were fetched. 2 tables in 28.13 s.
```

Everything goes to `info` - job logs do not pass `debug` through.

Job logs are grepped for `Fetched table <source>.`, `All tables were fetched.`, `Processing N … table
exports.`, `Processed N workspace exports.`, `Fetched file "<name>".` and `All files were fetched.`, so
those messages always keep their original wording; the detail is appended after them.

The `S3` and `ABS` strategies emit the same phase lines without per-table download timing (they only
fetch file credentials and write manifests); their per-table line is
`Fetched table <source>. Export job <id>, file <id>.` The `Snowflake` and `BigQuery` workspace
strategies emit `Processed N workspace exports.` plus one `Fetched table <source>.` per table, with no
per-table detail - a single load job covers every table.

File downloads follow the same pattern: `Fetched file "<name>". Downloaded N bytes in X s.` and
`All files were fetched. N files, S bytes in X s.`

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
