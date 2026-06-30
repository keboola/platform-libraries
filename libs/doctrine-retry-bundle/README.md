# Doctrine Retry Bundle

## Installation
```bash
composer require keboola/doctrine-retry-bundle
```

## Usage
Include the following code in your `config/packages/doctrine.yaml` file:

```yaml
doctrine:
    dbal:
        options:
            x_connect_retries: 3
```            
            
## Environment
The tests connect to MySQL through a [Toxiproxy](https://github.com/Shopify/toxiproxy) instance
(used to simulate connection failures). Both are provided by the `dev-doctrine-retry-bundle`
Docker Compose service, so the simplest way to run them is:
```bash
docker compose run --rm dev-doctrine-retry-bundle composer ci
```

The following variables are required:
```
TEST_DATABASE_HOST     - MySQL host (e.g. mysql)
TEST_DATABASE_PORT     - MySQL port (e.g. 3306)
TEST_DATABASE_USER     - MySQL user (e.g. root)
TEST_DATABASE_PASSWORD - MySQL password
TEST_DATABASE_DB       - MySQL database name (e.g. testdatabase)
TEST_PROXY_HOST        - Toxiproxy host (e.g. toxiproxy); its API is reached at http://<host>:8474
```
