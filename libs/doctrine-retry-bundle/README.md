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
```
TEST_DATABASE_URL - database connection URL (mysql://user:secret@localhost/mydb)
```
