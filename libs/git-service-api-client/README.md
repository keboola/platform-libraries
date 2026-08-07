# Git Service API Client

PHP client for the Keboola Git Service API, built on top of
[`keboola/php-api-client-base`](../php-api-client-base).

## Installation

```bash
composer require keboola/git-service-api-client
```

## Usage

```php
use Keboola\GitServiceApiClient\GitServiceApiClient;

// Default: projected Kubernetes ServiceAccount token, sent in X-Kubernetes-Authorization.
// The token file is re-read on every request, so kubelet-rotated tokens are picked up
// automatically.
$client = new GitServiceApiClient('https://git-service.example.com');

// Manage API token (legacy) — sent in X-KBC-ManageApiToken.
$client = new GitServiceApiClient('https://git-service.example.com', $manageApiToken);
```

### Constructor options

```php
new GitServiceApiClient(
    string $baseUrl,
    ?string $manageToken = null,            // null => Kubernetes ServiceAccount token
    ?Psr\Log\LoggerInterface $logger = null,
    int $backoffMaxTries = 3,               // retries on 5xx / transport errors, never 4xx
    int $connectTimeout = 10,
    int $requestTimeout = 120,
    string $userAgent = 'Keboola Git Service PHP Client',
    null|Closure|GuzzleHttp\HandlerStack $requestHandler = null, // inject a mock handler in tests
);
```

### Repositories

```php
$repository = $client->createRepository('my-repo');
$repository = $client->getRepository('my-repo');
$client->deleteRepository('my-repo');
```

### Credentials

Credentials come in two shapes, and which fields are valid depends on the type. `NewCredential` exposes a
named constructor per type so an invalid combination cannot be built:

```php
use Keboola\GitServiceApiClient\KeyPermission;
use Keboola\GitServiceApiClient\NewCredential;

// SSH key — public key required
$created = $client->createCredential('my-repo', NewCredential::sshKey(
    'my-user',
    'ssh-rsa AAAA...',
    KeyPermission::ReadWrite,
));

// HTTP token — no public key; the generated secret is on the returned object
$created = $client->createCredential('my-repo', NewCredential::httpToken(
    'my-user',
    KeyPermission::ReadOnly,
));

/** @var list<Keboola\GitServiceApiClient\Model\Credential> $credentials */
$credentials = $client->listCredentials('my-repo');
$credential = $client->getCredential('my-repo', $credentialId);
$client->deleteCredential('my-repo', $credentialId);
```

`createCredential()` returns `Model\CreatedCredential`, which carries the secret; subsequent reads return
`Model\Credential` without it.

### Commits and refs

```php
$commits = $client->listCommits('my-repo', 'main', page: 1, limit: 30); // Model\CommitList

/** @var list<Keboola\GitServiceApiClient\Model\GitRef> $refs */
$refs = $client->listRefs('my-repo');
```

## Errors

All failures throw `Keboola\GitServiceApiClient\Exception\GitServiceClientException`, a subclass of the base
client's exception. It exposes `getStatusCode(): ?int` and `getResponseBody(): ?string`; the message is
extracted from the API response by `GitServiceErrorMessageResolver`.

## Development

Run everything through the library's Docker Compose service:

```bash
docker compose run --rm dev-git-service-api-client composer install
docker compose run --rm dev-git-service-api-client composer ci   # validate + phpcs + phpstan + phpunit
```

No environment variables are required — the test suite runs against a mocked Guzzle handler.

## License

MIT licensed, see [LICENSE](./LICENSE) file.
