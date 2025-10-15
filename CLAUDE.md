# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Structure

This is a **monorepo** containing 20+ PHP libraries for the Keboola platform. All libraries are located in the `libs/` directory. Each library is independently versioned and published but developed together for easier testing and cross-library changes.

### Core Libraries

**Data Movement:**
- `input-mapping` - Downloads tables/files from Storage API (CSV/Parquet support)
- `output-mapping` - Uploads data to Storage API with import handling
- `staging-provider` - Configures staging factories for local/workspace storage
- `slicer` - Data slicing utilities

**API Clients:**
- `api-bundle` - Symfony bundle for Keboola API applications with authentication
- `service-client` - Generic service client for Keboola services
- `query-service-api-client` - Query Service API client
- `sync-actions-api-php-client` - Sync Actions API client
- `sandboxes-service-api-client` - Sandboxes Service API client
- `azure-api-client` - Azure API client
- `vault-api-client` - Vault API client

**Infrastructure:**
- `k8s-client` - Kubernetes client wrapper
- `messenger-bundle` - Symfony Messenger bundle with Azure Service Bus support
- `doctrine-retry-bundle` - Database retry logic for Doctrine
- `logging-bundle` - Logging configuration bundle

**Utilities:**
- `configuration-variables-resolver` - Resolves configuration variables
- `permission-checker` - Permission checking utilities
- `key-generator` - Key generation utilities
- `settle` - Settlement utilities for async operations
- `php-test-utils` - Shared testing utilities

### Key Architecture Patterns

**Staging System:** Libraries use a three-tier staging architecture:
1. **Staging** - The actual storage location (local filesystem, S3, Azure Blob, workspace databases)
2. **Provider** - Lazy initialization wrapper around staging (`ProviderInterface`)
3. **Strategy Factory** - Selects appropriate staging provider based on configuration

Input/output mapping libraries use `staging-provider` to configure their strategy factories for different environments (local development, cloud storage, database workspaces).

**Monorepo Dependencies:** Libraries reference each other using path repositories:
```json
"repositories": {
    "libs": {
        "type": "path",
        "url": "../../libs/*"
    }
}
```
Dependencies between libraries use `"*@dev"` to always use the local version.

## Development Workflow

### Running Commands in Docker

Each library has a dedicated Docker Compose service named `dev-{library-name}`. To work on a specific library:

```bash
# Enter library container
docker compose run --rm dev-input-mapping bash

# Inside container - install dependencies
composer install

# Run tests
composer tests          # PHPUnit
composer paratests      # Parallel tests (faster)

# Run code quality checks
composer phpcs          # Check code style
composer phpcbf         # Fix code style automatically
composer phpstan        # Static analysis

# Run all checks
composer check          # Validation + phpcs + phpstan
composer ci             # Full CI suite (check + tests)
```

**Important:** Do NOT run `composer install` from the host machine. Always use the Docker container for the specific library you're working on.

### Environment Variables

Before working on any library:

- Open `libs/<library>/README.md` and note the required variables.
- Create a `./.env` file in the repository root and define those variables there.
- Docker Compose automatically passes environment variables from the root `.env` file to containers for libraries that need them.

**When are these needed?** Environment variables are required for running functional/integration tests that interact with real Keboola services. Most libraries work without them for unit tests.

**Which libraries need environment variables?**
- `input-mapping`, `output-mapping`, `configuration-variables-resolver` - Storage API credentials
- `staging-provider` - Storage API credentials
- `query-service-api-client` - Query Service and Storage API credentials
- `sync-actions-api-php-client` - Storage API token and hostname suffix
- `doctrine-retry-bundle` - MySQL database connection for testing

Each library documents its specific environment variable requirements in its own `README.md` (`libs/<library-name>/README.md`).

### Running Single Tests

```bash
# Run specific test file
docker compose run --rm dev-input-mapping bash -c "vendor/bin/phpunit tests/Functional/DownloadFilesTest.php"

# Run specific test method
docker compose run --rm dev-input-mapping bash -c "vendor/bin/phpunit --filter testTableManifest tests/Functional/DownloadTablesTest.php"
```

## Commit Message Format

This project uses [Conventional Commits v1.0.0](https://www.conventionalcommits.org/en/v1.0.0/).

**CRITICAL:** Since this is a monorepo, commit messages MUST include the library name:

Format: `<type>(<library-name>): <description>`

Examples:
- `feat(input-mapping): add support for Parquet format downloads`
- `fix(output-mapping): resolve memory leak in large file uploads`
- `refactor(staging-provider): extract workspace credential handling`

## Code Quality Standards

### PHP_CodeSniffer
- All libraries use `keboola/coding-standard` (extends PSR-12)
- Configuration in `phpcs.xml` per library
- Maximum line length: 120 characters
- Files must end with single blank line (LF)
- Multi-line function calls require trailing comma
- Use statements must be sorted alphabetically
- Do NOT use `final` keyword for classes that need mocking in tests

### PHPStan
- Maximum level static analysis (`level: max`)
- Configuration in `phpstan.neon` per library
- Includes PHPUnit and Symfony extensions

### Testing with PHPUnit

**Data Providers:**
- MUST use `yield` statements with `Generator` return type
- Use named array keys for 3+ parameters

```php
public static function simpleProvider(): Generator
{
    yield 'case 1' => ['value1', 'value2'];
    yield 'case 2' => ['value3', 'value4'];
}

public static function complexProvider(): Generator
{
    yield 'with valid input' => [
        'inputValue' => 'test',
        'expectedResult' => 'processed',
        'shouldThrowException' => false,
    ];
}
```

**Mocking Standards:**
- ALWAYS specify expected call count: `expects($this->once())`
- Never use `method()` without `expects()` - makes tests unreliable
- For multiple calls with different parameters, use `willReturnCallback` pattern

```php
// Good - verifies call count
$mock->expects($this->once())
    ->method('getData')
    ->willReturn('value');

// Bad - no verification
$mock->method('getData')->willReturn('value');
```

### Deprecated Patterns

**Do NOT use `withConsecutive()`** (removed in PHPUnit 10+). Use `willReturnCallback` instead:

```php
$expectedCalls = [
    ['arg1a', 'arg1b'],
    ['arg2a', 'arg2b'],
];

$mock->expects(self::exactly(count($expectedCalls)))
    ->method('someMethod')
    ->willReturnCallback(function (...$args) use (&$expectedCalls) {
        $expected = array_shift($expectedCalls);
        self::assertSame($expected, $args);
    });
```

## CI/CD

- Azure Pipelines configuration in `azure-pipelines.yml`
- Each library has its own pipeline that runs on changes
- Libraries are independently published to GitHub repositories

## PHP Version Support

Libraries support different PHP versions based on their needs:
- Most libraries: PHP 8.2+
- api-bundle: PHP 8.1+
- Some bundles support PHP 8.1-8.4 range

Check each library's `composer.json` for specific requirements.

## Additional Notes

- Use `docker compose` (V2), not `docker-compose`
- Never include `composer.lock` files (monorepo uses `"lock": false`)
