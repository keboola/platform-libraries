# Platform Libraries

A monorepo containing 20+ PHP libraries for the Keboola platform. All libraries are located in the `libs/` directory. Each library is independently versioned and published but developed together for easier testing and cross-library changes.

## Libraries

### Data Movement
- **input-mapping** - Downloads tables/files from Storage API (CSV/Parquet support)
- **output-mapping** - Uploads data to Storage API with import handling
- **staging-provider** - Configures staging factories for local/workspace storage
- **slicer** - Data slicing utilities

### API Clients
- **api-bundle** - Symfony bundle for Keboola API applications with authentication
- **service-client** - Generic service client for Keboola services
- **query-service-api-client** - Query Service API client
- **sync-actions-api-php-client** - Sync Actions API client
- **sandboxes-service-api-client** - Sandboxes Service API client
- **azure-api-client** - Azure API client
- **vault-api-client** - Vault API client

### Infrastructure
- **k8s-client** - Kubernetes client wrapper
- **messenger-bundle** - Symfony Messenger bundle with Azure Service Bus support
- **doctrine-retry-bundle** - Database retry logic for Doctrine
- **logging-bundle** - Logging configuration bundle

### Utilities
- **configuration-variables-resolver** - Resolves configuration variables
- **permission-checker** - Permission checking utilities
- **key-generator** - Key generation utilities
- **settle** - Settlement utilities for async operations
- **php-test-utils** - Shared testing utilities

## Development

### Environment Setup

#### Docker Compose Environment Variables

Before working on any library:

- Open `libs/<library>/README.md` and note the required variables.
- Create a `./.env` file in the repository root and define those variables there.
- Start the dev container for that library and follow its README commands (e.g., `docker compose run --rm dev-<library> bash`).

### Running Tests for a Library

Each library has a dedicated Docker Compose service. To work on a specific library:

```bash
# Enter library container
docker compose run --rm dev-<library> bash

# Inside container - install dependencies and run tests
composer install
composer tests          # PHPUnit
composer paratests      # Parallel tests (faster)
```

### Code Quality Checks

```bash
# Inside library container
composer phpcs          # Check code style
composer phpcbf         # Fix code style automatically
composer phpstan        # Static analysis
composer check          # Validation + phpcs + phpstan
composer ci             # Full CI suite (check + tests)
```

### Running Single Tests

```bash
# Run specific test file
docker compose run --rm dev-input-mapping bash -c "vendor/bin/phpunit tests/Functional/DownloadFilesTest.php"

# Run specific test method
docker compose run --rm dev-input-mapping bash -c "vendor/bin/phpunit --filter testTableManifest tests/Functional/DownloadTablesTest.php"
```

## Contributing

### Commit Message Format

This project uses [Conventional Commits v1.0.0](https://www.conventionalcommits.org/en/v1.0.0/).

**IMPORTANT:** Since this is a monorepo, commit messages MUST include the library name:

**Format:** `<type>(<library-name>): <description>`

**Examples:**
- `feat(input-mapping): add support for Parquet format downloads`
- `fix(output-mapping): resolve memory leak in large file uploads`
- `docs(service-client): update README with new endpoint examples`

## License

MIT licensed, see [LICENSE](./LICENSE) file.
