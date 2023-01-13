# Azure API Client

## Installation

```bash
composer require keboola/azure-api-client
```

## Usage

To create API client using PHP:
```php
# example.php
use Keboola\AzureApiClient\Authentication\AuthenticatorFactory;
use Keboola\AzureApiClient\AzureApiClientFactory;
use Keboola\AzureApiClient\GuzzleClientFactory;
use Keboola\AzureApiClient\Marketplace\MarketplaceApiClient;
use Monolog\Logger;

$logger = new Logger('azure-api-client');
$guzzleClientFactory = new GuzzleClientFactory($logger);
$authenticatorFactory = new AuthenticatorFactory();
$clientFactory = new AzureApiClientFactory($guzzleClientFactory, $authenticatorFactory, $logger);

$marketingApiClient = MarketplaceApiClient::create($clientFactory);
```

To create API client using Symfony container, just register class services and everything should auto-wire just fine:
```yaml
# services.yaml
services:
  Keboola\AzureApiClient\Authentication\AuthenticatorFactory:

  Keboola\AzureApiClient\AzureApiClientFactory:

  Keboola\AzureApiClient\GuzzleClientFactory:
  
  Keboola\AzureApiClient\Marketplace\MarketplaceApiClient:
    factory: ['App\Marketplace\Azure\MarketplaceApiClient\Marketplace\MarketplaceApiClient', 'create']

```

## License

MIT licensed, see [LICENSE](./LICENSE) file.
