# Workspace Provider

[![Build Status](https://dev.azure.com/keboola-dev/wokspace-provider/_apis/build/status/keboola.workspace-provider?branchName=main)](https://dev.azure.com/keboola-dev/wokspace-provider/_build/latest?definitionId=69&branchName=main)

## Development
First start with creating `.env` file from `.env.dist`.
```bash
cp .env.dist .env
# edit .env to set variable values
```

To run tests, there is a separate service for each PHP major version (5.6 to 7.4).
For example, to run tests against PHP 5.6, run following:
```bash
docker-compose run --rm tests56
```

To develop locally, use `dev` service. Following will install Composer dependencies:
```bash
docker-compose run --rm dev composer install
```
