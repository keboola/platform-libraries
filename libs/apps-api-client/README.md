# Apps API Client

PHP client for the Keboola Apps API (`/v2` App Deployments API of Sandboxes Service), built on top of
[`keboola/php-api-client-base`](../php-api-client-base).

The API is served on `https://apps.{hostnameSuffix}`, e.g. `https://apps.keboola.com`. Its OpenAPI spec is
`docs/swagger-apps.yaml` in `keboola/sandboxes-service` and is marked experimental.

Not to be confused with `Keboola\SandboxesServiceApiClient\Apps\AppsApiClient` in
`keboola/sandboxes-service-api-client`, which is the v1 data-apps API on `data-apps.{hostnameSuffix}`.

## Installation

```bash
composer require keboola/apps-api-client
```

## Usage

```php
use Keboola\AppsApiClient\AppsApiClient;

$client = new AppsApiClient('https://apps.keboola.com', $storageApiToken);
```

### Constructor options

```php
new AppsApiClient(
    string $baseUrl,
    string $storageToken,                   // sent in X-StorageApi-Token
    ?Psr\Log\LoggerInterface $logger = null,
    int $backoffMaxTries = 5,               // retries on 5xx / transport errors, never 4xx
    int $connectTimeout = 10,
    int $requestTimeout = 120,
    string $userAgent = 'Keboola Apps API PHP Client',
    null|Closure|GuzzleHttp\HandlerStack $requestHandler = null, // inject a mock handler in tests
);
```

### Apps

```php
$app = $client->getApp('123');
$app->state;                            // App::STATE_* — notDeployed, starting, running, suspended, disabled
$app->production->actualDeploymentId;   // deployment serving production now
$app->production->desiredDeploymentId;  // deployment being promoted to production

// wakes the deployment that serves production; 409 when the app has none
$app = $client->resumeApp('123');

// shortcut for restartDeployment() of the deployment that serves production; 409 when the app has none
$app = $client->restartApp('123');

// runs of the app, newest first; a restart opens a new run
$runs = $client->listRuns('123', deploymentId: $app->production->actualDeploymentId, limit: 1);
$runs[0]->state;                        // Run::STATE_* — starting, running, failed, finished
$runs[0]->failureReason?->message;      // why a failed run failed, safe to show
```

While a promotion is in progress, `state` describes the deployment being replaced. Compare
`production->desiredDeploymentId` with `production->actualDeploymentId` to see whether it has finished.

### Deployments

```php
// gives the deployment a new container and Storage token; for the deployment that serves
// production, it also starts the app
$app = $client->restartDeployment('123', $app->production->actualDeploymentId);

// stage the latest config version without running it
$deployment = $client->createDeployment('123');

// deploy a pinned config version and promote it to production
$deployment = $client->createDeployment('123', configVersion: '7', promote: true);

$deployment = $client->getDeployment('123', $deployment->id);
$deployment->runState;                  // Deployment::RUN_STATE_* — failed means it crashed or could not start
```

`createDeployment()` is not idempotent. Like every request, it is retried on 5xx, so a retry after a failure on
the server side can create a second deployment.

### Errors

Every failure throws `Keboola\AppsApiClient\Exception\AppsClientException`. The message is the `error`
field of the API response; `getStatusCode()` returns the HTTP status, or `null` without a response.
