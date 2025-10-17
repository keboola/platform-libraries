# Output Mapping

Output mapping library for Keboola Runner and Workspaces. Processes component outputs and uploads them to Keboola Storage API.

**Tables:**
- Local staging: Uploads tables as gzipped CSV files to Storage API
  - Slicing: Large CSV files are automatically split into multiple compressed chunks for parallel upload (via external slicer binary)
    - Requires: `output-mapping-slice` feature flag, default CSV format (`,` delimiter, `"` enclosure), no custom `columns` mapping
    - Sliced files must have `columns` or `schema` specified in configuration
- Workspace staging (Snowflake/BigQuery): Loads tables directly from workspace database objects (no file upload, no slicing)

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
