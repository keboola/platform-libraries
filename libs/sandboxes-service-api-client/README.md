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

// Create new app (required fields only)
$app = $client->createApp([
    'type' => 'streamlit',
    'branchId' => '123', // use null for default branch
    'name' => 'My App',
]);

// Delete app
$client->deleteApp('app-id');
```

#### createApp() payload fields

- `type` (string, required) - App type, e.g. `streamlit`
- `branchId` (string|null, required) - Storage branch ID; use `null` for the default branch
- `name` (string, required) - App name
- `description` (string|null, optional) - Human-readable description of the app
- `config` (object|null, optional) - Component configuration passed to the app, may contain arbitrary keys

## License

MIT licensed, see [LICENSE](./LICENSE) file.
