# Input Mapping [![Build Status](https://travis-ci.org/keboola/input-mapping.svg?branch=master)](https://travis-ci.org/keboola/input-mapping) [![Code Climate](https://codeclimate.com/github/keboola/input-mapping/badges/gpa.svg)](https://codeclimate.com/github/keboola/input-mapping) [![Test Coverage](https://codeclimate.com/github/keboola/input-mapping/badges/coverage.svg)](https://codeclimate.com/github/keboola/input-mapping/coverage)

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
```

Run tests

```
docker-compose run --rm php composer ci

```