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

## License

MIT licensed, see [LICENSE](./LICENSE) file.
