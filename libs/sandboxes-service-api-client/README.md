# Sandboxes Service API Client

## Installation
```bash
composer require keboola/sandboxes-service-api-client
```

## Usage

### Sandboxes API

```php
use Keboola\SandboxesServiceApiClient\Sandboxes\SandboxesApiClient;

$client = new SandboxesApiClient(
    baseUrl: 'https://data-science.keboola.com',
    token: '{storage-api-token}',
    userAgent: 'My App',
);

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

$client = new AppsApiClient(
    baseUrl: 'https://data-apps.keboola.com',
    token: '{storage-api-token}',
    userAgent: 'My App',
);

// List all apps
$apps = $client->listApps();

// Get specific app
$app = $client->getApp('app-id');

// Update app state
$client->patchApp('app-id', [
    'desiredState' => 'running',
    'restartIfRunning' => true,
]);

// Create new app (required fields only)
$app = $client->createApp([
    'type' => 'streamlit',
    'branchId' => '123', // use null for default branch
    'name' => 'My App',
]);

// Delete app
$client->deleteApp('app-id');
```

## License

MIT licensed, see [LICENSE](./LICENSE) file.
