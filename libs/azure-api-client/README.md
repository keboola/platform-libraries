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
use Keboola\AzureApiClient\ApiClientFactory\UnauthenticatedAzureApiClientFactory;
use Keboola\AzureApiClient\GuzzleClientFactory;
use Keboola\AzureApiClient\Marketplace\MarketplaceApiClient;
use Monolog\Logger;


## Create Authenticator for your service

### Static token
$authenticator = new StaticTokenCredentialsAuthenticator('myToken');

### Managed credentials or Environment credentials
$client = new UnauthenticatedAzureApiClientFactory(...);
#### Automated factory
$authenticator = (new AuthenticatorFactory($client, new \Psr\Log\NullLogger()))->createAuthenticator();
#### Manual initialization
$authenticator = new ClientCredentialsEnvironmentAuthenticator($client, $this->logger);
$authenticator = new ManagedCredentialsAuthenticator($client, $this->logger);

// Marketplace
$marketingApiClient = MarketplaceApiClient::create($authenticator);
$marketingApiClient = MeteringServiceApiClient::create($authenticator);

// Event grid
$eventGridApiClient = EventGridApiClient::create($authenticator, 'topic hostname');
```

To create API client using Symfony container, just register class services and everything should auto-wire just fine:
```yaml
# services.yaml
services:
  Keboola\AzureApiClient\Authentication\AuthenticatorFactory:

  Keboola\AzureApiClient\AzureApiClientFactory:

  Keboola\AzureApiClient\Marketplace\MarketplaceApiClient:
    factory: ['App\Marketplace\Azure\MarketplaceApiClient\Marketplace\MarketplaceApiClient', 'create']
```

## License

MIT licensed, see [LICENSE](./LICENSE) file.
