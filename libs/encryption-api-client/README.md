# Encryption API Client

## Installation
```bash
composer require keboola/encryption-api-php-client
```

## Usage

```php
use Keboola\EncryptionApiClient\Migrations;

$migrations = new Migrations(getenv('STORAGE_API_TOKEN'));

$resultMessage = $migrations->migrateConfiguration(
    sourceStorageApiToken: '...',
    destinationStack: 'connection.europe-west3.gcp.keboola.com',
    destinationStorageApiToken: '...', 
    componentId: 'keboola.data-apps',
    configId: '123456',
    branchId: '102',
    dryRun: true,
);
```

## License

MIT licensed, see [LICENSE](./LICENSE) file.
