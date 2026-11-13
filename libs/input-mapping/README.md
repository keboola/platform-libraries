# Input Mapping

Input mapping library for Keboola Runner and Workspaces.
Downloads tables and files from Keboola Storage API: tables can be exported to CSV or Parquet, files are downloaded as-is. Creates manifests and tracks incremental file state.
Supports staging via Local, S3, Azure Blob Storage, and loading into Snowflake/BigQuery workspaces.

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

## Table option `incremental`

`incremental: true` on a `storage.input.tables[]` item merges the table into the workspace table of the same
`destination`. If that table does not exist yet, it is created. If it exists, its columns (including
`_timestamp` unless `keep_internal_timestamp_column: false`) and its primary key must match the loaded table;
rows with a matching primary key are updated and the rest appended, and without a primary key all rows are
appended.

The option is meant for workspace input mapping. When the load cleans the workspace first (the workspace is
not preserved), the incremental destination tables survive the clean and the load merges into them; every
other table is dropped.

It cannot be combined with `overwrite: true` or with `load_type` `VIEW` / `CLONE` (nor the legacy
`use_view: true`), and file input mapping (`data/in`) rejects it. When the key is missing or `false`, the
table is loaded as a new workspace table.

## License

MIT licensed, see [LICENSE](./LICENSE) file.
