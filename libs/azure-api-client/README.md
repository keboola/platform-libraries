# Azure API Client

## Installation
```bash
composer require keboola/azure-api-client
```

## Usage
The simplest way to use API client is just by creating its instance. 

```php
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Marketplace\MarketplaceApiClient;
use Monolog\Logger;

$logger = new Logger('azure-api-client');
$marketplaces = new MarketplaceApiClient(new ClientCredentialsAuth(
    logger: $logger,
));
```

### Authentication
By default, API client will try to authenticate using ENV variables `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` and
`AZURE_CLIENT_SECRET`. If some of them is not available, it'll fall back to Azure metadata API.

If you want to supply your own credentials, you can pass custom authenticator instance in the client options:

```php
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Authentication\Authenticator\ClientCredentialsAuth;
use Keboola\AzureApiClient\Marketplace\MarketplaceApiClient;

$marketplaces = new MarketplaceApiClient(new ApiClientConfiguration(
    authenticator: new ClientCredentialsAuth(
        'tenant-id',
        'client-id',
        'client-secret',
    ),
));
```

Or can provide custom authentication token directly:

```php
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Authentication\Authenticator\StaticBearerTokenAuth;
use Keboola\AzureApiClient\Marketplace\MarketplaceApiClient;

$marketplaces = new MarketplaceApiClient(new ApiClientConfiguration(
    authenticator: new StaticBearerTokenAuth('my-token'),
));
```

Or even use custom authentication header if needed:

```php
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Authentication\Authenticator\StaticBearerTokenAuth;
use Keboola\AzureApiClient\Marketplace\MarketplaceApiClient;

$marketplaces = new MarketplaceApiClient(new ApiClientConfiguration(
    authenticator: new CustomHeaderAuth('aeg-sas-key', 'XXXXXXXXXXXXXXXXXX0GXXX/nDT4hgdEj9DpBeRr38arnnm5OFg=='),
));
```

If even this is not enough for your use-case, you can implement your own
`Keboola\AzureApiClient\Authentication\Authenticator\RequestAuthenticatorFactoryInterface` and pass it as `authenticator`. 

## License

MIT licensed, see [LICENSE](./LICENSE) file.
