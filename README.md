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

**IMPORTANT**: This monorepo currently requires TWO environment files with different names due to historical reasons. This will be unified in the future.

You need to create both files in the repository root:
1. **`.env`** - Required by some libraries' test bootstraps
2. **`.env.local`** - Required by other libraries' test bootstraps

Both files should contain the same environment variables. Each library that requires environment variables documents its specific requirements in its own README (`libs/<library-name>/README.md`).

#### Docker Compose Environment Variables

When running tests via Docker Compose (`docker compose run --rm dev-<library> bash`), environment variables from the root `.env` file are automatically passed to containers for libraries that need them (configured in `docker-compose.yml`).

#### Current State Issues (TO BE FIXED)

Due to inconsistent historical configuration:
- Some libraries look for `.env` (e.g., input-mapping, query-service-api-client)
- Others look for `.env.local` (e.g., output-mapping, staging-provider)
- This requires maintaining both files with the same content

**TODO**: Unify all libraries to use consistent env file naming convention.

### Running Tests for a Library

Each library has a dedicated Docker Compose service. To work on a specific library:

```bash
# Enter library container
docker compose run --rm dev-input-mapping bash

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
