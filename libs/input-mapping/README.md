# Input Mapping [![Build Status](https://dev.azure.com/keboola-dev/input-mapping/_apis/build/status/keboola.input-mapping?branchName=master)](https://dev.azure.com/keboola-dev/input-mapping/_build/latest?definitionId=37&branchName=master)

Input mapping library for Docker Runner and Sandbox Loader. 
Library processes input mapping, exports data from Storage tables into CSV files and files from Storage file uploads. 
Exported files are stored in local directory.

## Development

Create `.env.local` file from this `.env` template and fill the missing envs:

```ini
cp .env .env.local
```

To run Synapse tests, set `RUN_SYNAPSE_TESTS=1` and supply a Storage API token to a project with [Synapse backend](https://keboola.atlassian.net/browse/PS-707). Synapse tests are by default skipped (unless the above env is set).

Run test suite:

```
composer ci
```

## License

MIT licensed, see [LICENSE](./LICENSE) file.
