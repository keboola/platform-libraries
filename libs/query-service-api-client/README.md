# Keboola Query Service API PHP Client

[![Build Status](https://dev.azure.com/keboola-dev/Platform%20Libraries/_apis/build/status%2Fkeboola.platform-libraries?repoName=keboola%2Fplatform-libraries&branchName=main)](https://dev.azure.com/keboola-dev/Platform%20Libraries/_build/latest?definitionId=120&repoName=keboola%2Fplatform-libraries&branchName=main)

PHP client for Keboola Query Service API.

## Installation

```shell
composer require keboola/query-service-api-client
```

## Usage

```php
<?php

use Keboola\QueryApi\Client;

$client = new Client([
    'url' => 'https://query.keboola.com',
    'token' => 'your-storage-api-token'
]);

// Submit a query job
$response = $client->submitQueryJob('main', 'workspace-123', [
    'statements' => ['SELECT * FROM table1'],
    'transactional' => true
]);

$queryJobId = $response['queryJobId'];

// Get job status
$status = $client->getJobStatus($queryJobId);

// Get job results
$results = $client->getJobResults($queryJobId, $statementId);

// Cancel job
$client->cancelJob($queryJobId, ['reason' => 'User requested cancellation']);

// Health check
$health = $client->healthCheck();
```

## Configuration Options

The client constructor accepts the following configuration options:

- `url` (required): Query Service API URL (e.g., `https://query.keboola.com`)
- `token` (required): Storage API token
- `backoffMaxTries` (optional): Number of retry attempts for failed requests (default: 3)
- `userAgent` (optional): Additional user agent string to append
- `handler` (optional): Custom Guzzle handler stack

**Note**: The `healthCheck()` endpoint does not require authentication and will work without a valid token.

## API Methods

- `submitQueryJob(string $branchId, string $workspaceId, array $requestBody): array`
- `getJobStatus(string $queryJobId): array`
- `getJobResults(string $queryJobId, string $statementId): array`
- `cancelJob(string $queryJobId, array $requestBody = []): array`
- `healthCheck(): array`

## Development

### Running Tests

#### Unit Tests
Run unit tests:
```shell
vendor/bin/phpunit tests/ClientTest.php
```

#### Functional Tests
Functional tests require environment variables to be set:

- `STORAGE_API_TOKEN` - Storage API authentication token
- `QUERY_API_URL` - Query Service API endpoint URL  
- `STORAGE_API_URL` - Storage API endpoint URL

Run functional tests:
```shell
vendor/bin/phpunit tests/Functional/
```

#### All Tests
Run all tests:
```shell
composer run tests
```

### Code Quality

Run code style check:
```shell
composer run phpcs
```

Fix code style issues:
```shell
composer run phpcbf
```

Run static analysis:
```shell
composer run phpstan
```

Run all CI checks. Check [Github Workflows](./.github/workflows) for more details
```shell
composer run ci
```
