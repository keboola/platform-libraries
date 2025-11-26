# Sandboxes Service API Client

## Installation
```bash
composer require keboola/sandboxes-service-api-client
```

## Usage

### Sandboxes API

```php
use Keboola\SandboxesServiceApiClient\Sandboxes\SandboxesApiClient;
use Keboola\SandboxesServiceApiClient\ApiClientConfiguration;

$client = new SandboxesApiClient(new ApiClientConfiguration(
    baseUrl: 'https://data-science.keboola.com',
    storageToken: '{storage-api-token}',
    userAgent: 'My App',
));

$result = $client->createSandbox([
    'componentId' => 'keboola.data-apps',
    'configurationId' => '123',
    'configurationVersion' => '4',
    'type' => 'streamlit',
]);
```

### Apps API

```php
use Keboola\SandboxesServiceApiClient\Apps\AppsApiClient;
use Keboola\SandboxesServiceApiClient\ApiClientConfiguration;

$client = new AppsApiClient(new ApiClientConfiguration(
    baseUrl: 'https://data-apps.keboola.com',
    storageToken: '{storage-api-token}',
    userAgent: 'My App',
));

// List all apps
$apps = $client->listApps();

// Get specific app
$app = $client->getApp('app-id');

// Update app state
$client->patchApp('app-id', [
    'desiredState' => 'running',
    'restartIfRunning' => true,
]);
```

## License

MIT licensed, see [LICENSE](./LICENSE) file.
