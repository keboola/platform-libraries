# Azure API Client

## Installation
```bash
composer require keboola/azure-api-client
```

## Usage

### Marketplaces

```php
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Marketplace\MarketplaceApiClient;
use Monolog\Logger;

$logger = new Logger('azure-api-client');
$marketplaces = new MarketplaceApiClient(new ClientCredentialsAuth(
    logger: $logger,
));
```

### Event Grid

> Event grid support only POST /events endpoint `EventGridApiClient::publishEvents

```php
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\EventGrid\EventGridApiClient
use Monolog\Logger;

$eventGrid = new EventGridApiClient(
    topicHostname: '<topic>.northeurope-1.eventgrid.azure.net',
    configuration: new ApiClientConfiguration(
        authenticator: new CustomHeaderAuth('aeg-sas-key', '<token>'),
    )
);
```

### Service Bus

> Service bus support only subset of endpoints:
> - There are no management endpoints
> - Supported are only endpoints for:
>   - sending messages `ServiceBusApiClient::sendMessage`
>   - deleting messages `ServiceBusApiClient::deleteMessage`
>   - peak message `ServiceBusApiClient::peekMessage`

```php
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\ServiceBus\ServiceBusApiClient
use Keboola\AzureApiClient\Authentication\Authenticator\SASTokenAuthenticatorFactory;
use Monolog\Logger;

$endpoint = 'https://<queue>.servicebus.windows.net:443/';
$serviceBus = new ServiceBusApiClient(
    serviceBusEndpoint: $endpoint,
    configuration: new ApiClientConfiguration(
        authenticator: new SASTokenAuthenticatorFactory(
            url: $endpoint,
            sharedAccessKeyName: 'RootManageSharedAccessKey',
            sharedAccessKey: '<sharedAccessKey>',
        ),
    )
);
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

Or use sas token if service support it:

```php
use Keboola\AzureApiClient\ServiceBus\ServiceBusApiClient;
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Authentication\Authenticator\SASTokenAuthenticatorFactory;

$endpoint = 'https://<queue>.servicebus.windows.net:443/';
$serviceBus = new ServiceBusApiClient(
    serviceBusEndpoint: $endpoint,
    configuration: new ApiClientConfiguration(
        authenticator: new SASTokenAuthenticatorFactory(
            url: $endpoint,
            sharedAccessKeyName: 'RootManageSharedAccessKey',
            sharedAccessKey: '<sharedAccessKey>',
        ),
    )
);
```

Or custom authentication header if needed:

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
