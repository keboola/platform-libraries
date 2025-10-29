# Keboola Query Service API PHP Client

[![Build Status](https://dev.azure.com/keboola-dev/Platform%20Libraries/_apis/build/status%2Fkeboola.platform-libraries?repoName=keboola%2Fplatform-libraries&branchName=main)](https://dev.azure.com/keboola-dev/Platform%20Libraries/_build/latest?definitionId=120&repoName=keboola%2Fplatform-libraries&branchName=main)

PHP client for Keboola Query Service API.

## Installation

```shell
composer require keboola/query-service-api-client
```

## Usage

### Basic Usage - Submit and Wait for Results

```php
<?php

use Keboola\QueryApi\Client;

$client = new Client([
    'url' => 'https://query.keboola.com',
    'token' => 'your-storage-api-token'
]);

// Execute query and wait for results (recommended)
$response = $client->executeWorkspaceQuery('main', 'workspace-123', [
    'statements' => ['SELECT * FROM table1'],
    'transactional' => true
]);

// Get all results
foreach ($response->getResults() as $statementResult) {
    $data = $statementResult->getData();
    $columns = $statementResult->getColumns();
    echo "Fetched " . $statementResult->getNumberOfRows() . " rows\n";
}
```

### Advanced Usage - Manual Control

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

$queryJobId = $response->getQueryJobId();

// Get job status
$statusResponse = $client->getJobStatus($queryJobId);
$status = $statusResponse->getStatus();

// Get job results for first statement
$statements = $statusResponse->getStatements();
$statementId = $statements[0]->getId();
$resultsResponse = $client->getJobResults($queryJobId, $statementId);
$results = $resultsResponse->getData();

// Cancel job if needed
$cancelResponse = $client->cancelJob($queryJobId, ['reason' => 'User requested cancellation']);
$cancelledJobId = $cancelResponse->getQueryJobId();
```

## Configuration Options

The client constructor accepts two parameters: `$config` array and `$options` array.

### Config Array (required)
- `url` (required): Query Service API URL (e.g., `https://query.keboola.com`)
- `token` (required): Storage API token

### Options Array (optional)
- `backoffMaxTries` (optional): Number of retry attempts for failed requests (default: 3, range: 0-100)
- `userAgent` (optional): Additional user agent string to append
- `logger` (optional): PSR-3 logger instance for request/response logging
- `runId` (optional): Run ID to include in request headers
- `handler` (optional): Custom Guzzle handler stack

Example with options:
```php
$client = new Client(
    ['url' => 'https://query.keboola.com', 'token' => 'your-token'],
    ['backoffMaxTries' => 5]
);
```

## API Methods

All public methods return dedicated Response objects with typed getters:

- `submitQueryJob(string $branchId, string $workspaceId, array $requestBody): SubmitQueryJobResponse`
  - `getQueryJobId(): string`
- `getJobStatus(string $queryJobId): JobStatusResponse`
  - `getQueryJobId(): string`
  - `getStatus(): string` - Returns 'submitted', 'running', 'completed', 'failed', or 'canceled'
  - `getStatements(): Statement[]`
  - `getCreatedAt(): string`
  - `getChangedAt(): string`
  - `getCanceledAt(): ?string`
  - `getCancellationReason(): ?string`
  - `getActorType(): string`
- `getJobResults(string $queryJobId, string $statementId): JobResultsResponse`
  - `getData(): array` - Returns data rows as associative arrays (column names as keys)
  - `getColumns(): array`
  - `getStatus(): string`
  - `getNumberOfRows(): int`
  - `getRowsAffected(): int`
  - `getMessage(): ?string`
- `cancelJob(string $queryJobId, array $requestBody = []): CancelJobResponse`
  - `getQueryJobId(): string`
- `executeWorkspaceQuery(string $branchId, string $workspaceId, array $requestBody, int $maxWaitSeconds = 30): WorkspaceQueryResponse`
  - `getQueryJobId(): string`
  - `getStatus(): string`
  - `getStatements(): Statement[]`
  - `getResults(): JobResultsResponse[]` - Returns results for all completed statements
- `waitForJobCompletion(string $queryJobId, int $maxWaitSeconds = 30): JobStatusResponse`
  - Waits for job completion and returns the final status

### Statement Object

Each `Statement` object provides the following getters:
- `getId(): string`
- `getStatus(): string`
- `getQuery(): string`
- `getQueryId(): ?string`
- `getSessionId(): ?string`
- `getNumberOfRows(): ?int`
- `getRowsAffected(): ?int`
- `getError(): ?string`
- `getCreatedAt(): ?string`
- `getExecutedAt(): ?string`
- `getCompletedAt(): ?string`

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
- `HOSTNAME_SUFFIX` - Hostname suffix (e.g., `keboola.com`) - Query Service URL will be constructed as `https://query.{HOSTNAME_SUFFIX}`
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
