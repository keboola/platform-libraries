# Input Mapping [![Build Status](https://dev.azure.com/keboola-dev/input-mapping/_apis/build/status/keboola.input-mapping?branchName=master)](https://dev.azure.com/keboola-dev/input-mapping/_build/latest?definitionId=37&branchName=master)

Input mapping library for Docker Runner and Sandbox Loader. Library processes input mapping, exports data from Storage tables into CSV files and files from Storage file uploads. Exported files are stored in local directory.

## Development

```
docker-compose build
docker-compose run --rm php composer install
```

Create `.env` file from this template

```ini
STORAGE_API_TOKEN=
STORAGE_API_URL=https://connection.keboola.com
RUN_SYNAPSE_TESTS=0
SYNAPSE_STORAGE_API_TOKEN=
SYNAPSE_STORAGE_API_URL=https://connection.eu-central-1.keboola.com
```

To run Synapse tests, set RUN_SYNAPSE_TESTS to 1 and supply a Storage API token to a project with [Synapse backend](https://keboola.atlassian.net/browse/PS-707). Synapse tests are by default skipped (unless the above env is set).

Run tests

```
docker-compose run --rm php composer ci

```