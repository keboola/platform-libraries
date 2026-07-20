# Keboola Query Service API PHP Client

PHP client for the Keboola Query Service API, built on [`keboola/php-api-client-base`](https://github.com/keboola/php-api-client-base).

## Installation

```shell
composer require keboola/query-api-php-client
```

## Usage

```php
<?php

use Keboola\QueryApi\Client;

$client = new Client(
    'https://query.keboola.com',   // base URL
    'your-storage-api-token',      // X-StorageApi-Token
);

// Submit a query job
$response = $client->submitQueryJob('main', 'workspace-123', [
    'statements' => ['SELECT * FROM table1'],
    'transactional' => true,
]);
$queryJobId = $response->getQueryJobId();

// Poll until it finishes
$status = $client->waitForJobCompletion($queryJobId);

// Read results for the first completed statement
$statementId = $status->getStatements()[0]->getId();
$results = $client->getJobResults($queryJobId, $statementId);
foreach ($results->getData() as $row) {
    // ...
}

// Cancel if needed
$client->cancelJob($queryJobId, ['reason' => 'User requested cancellation']);
```

### Constructor options

```php
new Client(
    string $baseUrl,
    string $storageToken,
    ?string $runId = null,                 // sent as X-KBC-RunId on every request
    ?Psr\Log\LoggerInterface $logger = null,
    int $backoffMaxTries = 3,              // retries on 5xx / transport errors, never 4xx
    int $connectTimeout = 10,
    int $requestTimeout = 120,
    string $userAgent = 'Keboola Query API PHP Client',
    null|Closure|GuzzleHttp\HandlerStack $requestHandler = null, // inject a mock handler in tests
);
```

## Errors

All failures throw `Keboola\QueryApi\Exception\ClientException` (a subclass of the base client's
exception). It exposes `getStatusCode(): ?int` and `getResponseBody(): ?string`; the message is
taken from the API response's `exception` field when present.

## Development

Run inside the library's docker service:

```shell
composer install
composer ci        # validate + phpcs + phpstan + phpunit
```

Functional tests (`tests/Functional/`) require `STORAGE_API_TOKEN`, `HOSTNAME_SUFFIX`
(the Query Service URL is `https://query.{HOSTNAME_SUFFIX}`) and `STORAGE_API_URL`.

## License

MIT — see [LICENSE](./LICENSE).
