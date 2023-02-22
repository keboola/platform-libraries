# Azure API Client

## Installation
```bash
composer require keboola/azure-api-client
```

## Usage
The simplest way to use API client is just by creating its instance. 

```php
use Keboola\AzureApiClient\Marketplace\MarketplaceApiClient;
use Monolog\Logger;

$logger = new Logger('azure-api-client');
$marketplaces = new MarketplaceApiClient([
    'logger' => $logger,
]);
```

### Authentication
By default, API client will try to authenticate using ENV variables `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` and
`AZURE_CLIENT_SECRET`. If some of them is not available, it'll fall back to Azure metadata API.

If you want to supply your own credentials, you can pass custom authenticator instance in the client options:
```php
use Keboola\AzureApiClient\Authentication\Authenticator\ClientCredentialsAuthenticator;
use Keboola\AzureApiClient\Marketplace\MarketplaceApiClient;

$logger = new Logger('azure-api-client');
$marketplaces = new MarketplaceApiClient([
    'authenticator' => new ClientCredentialsAuthenticator(
        'tenant-id',
        'client-id',
        'client-secret',
    ),
]);
```

Or can provide custom authentication token directly:
```php
use Keboola\AzureApiClient\Authentication\Authenticator\StaticTokenAuthenticator;
use Keboola\AzureApiClient\Marketplace\MarketplaceApiClient;

$logger = new Logger('azure-api-client');
$marketplaces = new MarketplaceApiClient([
    'authenticator' => new StaticTokenAuthenticator('my-token'),
]);
```

## License

MIT licensed, see [LICENSE](./LICENSE) file.
