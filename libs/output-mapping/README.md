# Output Mapping

Output mapping library for Keboola Runner and Workspaces. Processes component outputs and uploads them to Keboola Storage API.

**Tables:**
- Local staging: Uploads tables as gzipped CSV files to Storage API
  - Slicing: Large CSV files are automatically split into multiple compressed chunks for parallel upload (via external slicer binary)
    - Requires: `output-mapping-slice` feature flag, default CSV format (`,` delimiter, `"` enclosure), no custom `columns` mapping
    - Sliced files must have `columns` or `schema` specified in configuration
- Workspace staging (Snowflake/BigQuery): Loads tables directly from workspace database objects (no file upload, no slicing)
- Direct-grant tables (`unload_strategy: direct-grant`): written by the component straight into Storage, so there is no load job. Output mapping refreshes their metadata through the workspace unload endpoint (`only-direct-grants`), including for a failed job - like a `write_always` table. A job which never calls `uploadTables()` (e.g. a terminated job) must call `TableLoader::refreshDirectGrantMetadata()` instead.
- Failed job (`isFailedJob`): only `write_always` tables are written, and a table which cannot be written is skipped with a warning instead of failing the output mapping.
- Descriptions: Table and column descriptions (`description`, `schema[].description` or `KBC.description` metadata) are stored in the native Storage description field. A table created by the run always gets its description; on an existing table the description is only stored when the table's `isDescriptionSystemManaged` flag is set, so a description managed by the user is never overwritten.
- Partition-aware import: `partition_aware_import: true` sends `partitionAwareImport` to the Storage API import, which limits incremental BigQuery deduplication to the destination partitions present in the imported rows. Omitted or `false` sends nothing.
- Partitioning and clustering (BigQuery only): `time_partitioning` (`type`, optional `field`, `expiration_ms`), `range_partitioning` (`field`, `range.start/end/interval`) and `clustering` (`fields`) are sent with the table definition when output mapping creates the table; only one of the two partitionings can be set. A value in the configuration overrides the manifest. An existing table is never altered: when its layout differs from the requested one, a warning is logged and the table is loaded as before. A table whose columns are unknown (no `columns`/`schema`) fails with a user error. On another backend the keys are not sent and a warning is logged. For a `direct-grant` table the keys are ignored with a warning.

**Files:**
- Uploads files as-is to Storage API File Storage (works with all staging types)

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

**Optional (for BigQuery-specific tests only):**
* `BIGQUERY_STORAGE_API_URL` - BigQuery Storage API URL (e.g., `https://connection.keboola.com`)
* `BIGQUERY_STORAGE_API_TOKEN` - A non-admin token with "Full Access" to Files, Components & Buckets and Trash from a project with BigQuery backend

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
