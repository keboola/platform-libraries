# Keboola PHP API Client Base

Shared base for building PHP clients for **Keboola services** (Storage, Manage,
Vault, Git Service, Sandboxes, Sync Actions, Job Queue, …). It is **not** a
general-purpose HTTP client — it encodes Keboola platform conventions: the
Keboola authentication headers, retry behavior, JSON handling, and error
normalization that every Keboola service client needs.

Used by `keboola/vault-api-client`, `keboola/sandboxes-service-api-client`,
`keboola/git-service-api-client`, `keboola/sync-actions-client`,
`keboola/azure-api-client`, and new Keboola service clients.

## Installation

```bash
composer require keboola/php-api-client-base
```

## What it provides

- `ApiClient` — Guzzle wrapper with per-request auth, retry, logging, and
  response-to-model mapping.
- `ApiClientOptions` — retries, timeouts, logger, error resolver (no auth — the
  authenticator is a first-class `ApiClient` constructor argument).
- `Auth\RequestAuthenticatorInterface` + ready authenticators for the Keboola
  auth schemes: `StorageApiTokenAuthenticator` (`X-StorageApi-Token`),
  `ManageApiTokenAuthenticator` (`X-KBC-ManageApiToken`),
  `KeboolaServiceAccountAuthenticator` (projected SA token →
  `X-Kubernetes-Authorization`).
- `RetryDecider`, `Json`, `ResponseModelInterface`, `Exception\ClientException`.

## Building a Keboola service client

Compose an `ApiClient` inside your service facade and map responses to models:

```php
use GuzzleHttp\Psr7\Request;
use Keboola\ApiClientBase\ApiClient;
use Keboola\ApiClientBase\ApiClientOptions;
use Keboola\ApiClientBase\Auth\StorageApiTokenAuthenticator;
use Keboola\ApiClientBase\Json;
use Keboola\ApiClientBase\ResponseModelInterface;

final class WidgetModel implements ResponseModelInterface
{
    public function __construct(public readonly string $id) {}

    public static function fromResponseData(array $data): static
    {
        \assert(is_string($data['id']));
        return new self($data['id']);
    }
}

final class MyServiceClient
{
    private ApiClient $apiClient;

    public function __construct(
        string $baseUrl,
        StorageApiTokenAuthenticator $authenticator,
        ?ApiClientOptions $options = null,
    ) {
        $this->apiClient = new ApiClient($baseUrl, $authenticator, $options);
    }

    public function createWidget(string $name): WidgetModel
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request('POST', 'widgets', ['Content-Type' => 'application/json'], Json::encodeArray(['name' => $name])),
            WidgetModel::class,
        );
    }
}

$client = new MyServiceClient(
    'https://my-service.keboola.com',
    new StorageApiTokenAuthenticator($storageApiToken),
    new ApiClientOptions(backoffMaxTries: 3),
);
```

## Authentication

Pick the authenticator matching the service's scheme, or implement
`RequestAuthenticatorInterface` for a service-specific scheme (e.g. azure's
OAuth). `Content-Type` is set per request on calls with a body; the only Guzzle
default header is `User-Agent` (set via `ApiClientOptions::$userAgent`).

## License

MIT
