# Sandboxes Service API Client

## Installation
```bash
composer require keboola/sandboxes-service-api-client
```

## Usage

```php
use Keboola\SandboxesServiceApiClient\Sandboxes\SandboxesApiClient;
use Keboola\SandboxesServiceApiClient\ApiClientConfiguration;

new SandboxesApiClient(new ApiClientConfiguration(
    baseUrl: 'https://sandboxes-service.keboola.com',
    storageToken: '{storage-api-token}',
    userAgent: 'Keboola Sandboxes Service API PHP Client',
));

$result = $client->createSandbox([
    'componentId' => 'keboola.data-apps',
    'configurationId' => '123',
    'configurationVersion' => '4',
    'type' => 'streamlit',
]);

```

## License

MIT licensed, see [LICENSE](./LICENSE) file.
