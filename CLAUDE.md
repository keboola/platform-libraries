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

CI runs on **GitHub Actions**: test the affected libraries on every push to any branch, then publish them to their standalone read-only repos.

### Flow

1. **Trigger** — `ci.yml` runs `on: push` to **all branches** (`branches: ['**']`); there is **no `pull_request` trigger** and no main-only gating. PR status checks still work because the push-triggered run attaches to the PR head SHA. An orchestrator-level `concurrency: { group: ci-${{ github.ref }}, cancel-in-progress: true }` cancels superseded runs on the same ref.
2. **`detect-changes`** — runs `bin/ci/affected-libraries.php` **natively** (via `shivammathur/setup-php`, no Docker, so the gate stays fast). It diffs `github.event.before..HEAD`, maps changed `libs/*` dirs to packages, builds the `*@dev` reverse-dependency graph, and outputs `libs` (JSON array of affected dirs = changed ∪ transitive dependents). **Fallback to all 22** when shared infra changes (`Dockerfile`, `docker-compose*`, `.github/**`, `bin/**`, `.dockerignore`, root `composer.json`/`composer.lock`), on a zero-SHA/first-push/force-push, or on any error.
3. **Test fan-out** — **22 explicit jobs**, one per library, each `if: contains(needs.detect-changes.outputs.libs, '"<lib>"')` + `uses: ./.github/workflows/lib-<lib>.yml` + `secrets: inherit`. These stay explicit (22 separate files) — see gotchas: a reusable-workflow `uses:` **cannot** be driven by a matrix.
4. **`tests-result`** — barrier job (`needs:` all 22, `if: always()`) that fails if any test job's result is not `success`/`skipped`. **This is the single required status check** for branch protection.
5. **`publish`** — **one matrix job** (`matrix.lib: ${{ fromJSON(needs.detect-changes.outputs.libs) }}`, `fail-fast: false`) that runs on every green build (`needs.tests-result.result == 'success'`, guarded by `libs != '[]'`) and splits/publishes each affected library to its standalone repo. Runs on any branch, so pushing a dev branch makes `composer require keboola/<lib>:dev-<branch>` work in dependents. Only the branch being built plus the (prefix-stripped) tags are pushed — not every monorepo branch (see gotchas).

### Components

- **Per-library test workflows** — `.github/workflows/lib-<lib>.yml` (`on: workflow_call`). Most run a single `composer ci` job in `dev-<lib>`. Special cases: `input-mapping`/`output-mapping` have multi-suite jobs (cs / AWS / Azure / BigQuery) + a `concurrency: { group: <lib>-lock, cancel-in-progress: false }` mutex against shared Storage projects; `k8s-client`/`messenger-bundle` provision Terraform; `logging-bundle` runs a Symfony 6.4/7.2 matrix on `dev81`/`dev83`. All per-lib workflows carry the same `<lib>-lock` concurrency group; all steps and (for the multi-suite ones) jobs have human-readable names.
- **Tag releases** — `.github/workflows/release.yml` (`on: push: tags: ['*/*']`). **One job** that derives the library from the tag (`<lib>/<version>` → `<lib>`), skips cleanly if it is not a library dir, and publishes via the same composite action. **No tests run on a tag** (the code was already tested on the branch); tag → publish only. The `<lib>/` prefix is stripped, so `output-mapping/1.2.3` becomes `1.2.3` in the standalone repo.
- **`split-library` composite action** — `.github/actions/split-library`. Takes a single `library` input (+ `app-id`/`private-key`), resolves the target repo name internally (the **5 dir≠repo exceptions** live in its `case`: `git-service-api-client→git-service-php-api-client`, `key-generator→php-key-generator`, `query-service-api-client→query-service-api-php-client`, `sandboxes-service-api-client→sandboxes-service-api-php-client`, `vault-api-client→vault-api-php-client`; all others repo == dir), mints a short-lived GitHub App installation token scoped to that one repo via `actions/create-github-app-token`, and calls `bin/split-repo.sh`.
- **`bin/split-repo.sh`** — `git clone --mirror` of the checkout → `git filter-repo --subdirectory-filter libs/<lib>` (rewrites/strips tag prefix) → pushes the prefix-stripped tags plus **only the single build branch** (its optional 5th arg; tag builds push tags only). **Not** `git push --mirror`. Publish/release jobs check out with `fetch-depth: 0` + `fetch-tags: true`.
- **CI tooling** — `bin/ci/` is a standalone composer project (`AffectedLibrariesResolver` + `affected-libraries.php` CLI) with its own PHPUnit/PHPStan/phpcs config. Run its checks with `docker compose run --rm dev82 bash -c 'cd bin/ci && composer ci'`.

### Gotchas (why it is built this way)

- **A reusable-workflow `uses:` cannot use any context** — not `matrix`, not `vars`. So the 22 test jobs must be 22 explicit `uses:` jobs; they cannot be collapsed into a matrix without merging them into one shared `lib-common.yml` (deliberately not done — config stays per-library). Publish/release *can* be a matrix only because they call a **composite action in `steps:`**, where `matrix` is available.
- **Publishing pushes only the build branch + tags — never `--mirror`/all branches.** The standalone repos have branch protection the publish App cannot bypass, so a full mirror push (which touches every monorepo branch, including unrelated dev branches) is rejected with `GH006: Protected branch update failed`. `split-repo.sh` takes the branch to push as its 5th arg; the `split-library` action passes `GITHUB_REF_NAME` on branch builds and nothing on tag builds. **Side effect:** branches deleted in the monorepo are **not** pruned from the standalone repos. (Note: `git filter-repo` itself removes the `origin` remote and promotes remote-tracking refs — do not hand-manipulate refs before it runs, or `git remote add origin` later fails with "remote origin already exists".)
- **`logging-bundle` sets `config.policy.advisories.block=false`** in its `composer.json`. Composer ≥2.10 blocks installing advisory-affected package versions by default; a fresh CI image pulls the latest Composer, which made the Symfony 7.2 resolution unsatisfiable (symfony/yaml, monolog-bridge, cache were all flagged). This restores the pre-2.10 install behaviour; scoped to logging-bundle CI, not inherited by consumers.
- **All actions run on Node 24** — `actions/checkout@v6`, `docker/setup-buildx-action@v4`, `actions/create-github-app-token@v3`, `shivammathur/setup-php@v2`. Keep them off the deprecated Node 20 runtime when adding new steps.
- **Adding a new library** requires: a `lib-<lib>.yml`, a test job + a `tests-result` `needs:` entry in `ci.yml`, and — only if its standalone repo name differs from the dir — an entry in the `split-library` action's `case`.

### Required CI configuration (provisioned by repo admin)

Non-sensitive values are stored as **repository variables** (read via `vars.*`); sensitive tokens and secret keys are stored as **repository secrets** (read via `secrets.*`, passed to reusable workflows with `secrets: inherit`).

**Repository variables** (`vars.*`) — non-sensitive URLs, host suffix, AWS access key IDs and the publish App ID:
- `STORAGE_API_URL_AWS`, `STORAGE_API_URL_AZURE`, `STORAGE_API_URL_GCP`
- `HOSTNAME_SUFFIX_GCP`
- `OUTPUT_MAPPING__BIGQUERY_STORAGE_API_URL`
- `K8S_CLIENT_TERRAFORM_AWS_ACCESS_KEY_ID`
- `MESSENGER_BUNDLE_TERRAFORM_AWS_ACCESS_KEY_ID`
- `SPLIT_APP_ID` (GitHub App ID used to mint publish tokens)

**Repository secrets** (`secrets.*`) — Storage API tokens, Terraform secret keys and the publish App private key:
- Storage tokens: `INPUT_MAPPING__*`, `OUTPUT_MAPPING__STORAGE_API_TOKEN_*` / `OUTPUT_MAPPING_*__STORAGE_API_TOKEN_*`, `OUTPUT_MAPPING__BIGQUERY_STORAGE_API_TOKEN`, `VARIABLES_RESOLVER__*`, `STAGING_PROVIDER__STORAGE_API_TOKEN_AWS`, `QUERY_SERVICE__STORAGE_API_TOKEN_GCP` (also used by `php-storage-names-sanitizer`), `SYNC_ACTIONS_CLIENT__STORAGE_API_TOKEN_GCP`, `PHP_TEST_UTILS__TEST_STORAGE_API_TOKEN_SNOWFLAKE`.
- Terraform secret keys: `K8S_CLIENT_TERRAFORM_AWS_SECRET_ACCESS_KEY`, `MESSENGER_BUNDLE_TERRAFORM_AWS_SECRET_ACCESS_KEY`.
- Publishing: `SPLIT_APP_PRIVATE_KEY` (GitHub App private key; the App must be installed with push rights to all 22 target repos).

## PHP Version Support

Libraries support different PHP versions based on their needs:
- Most libraries: PHP 8.2+
- api-bundle: PHP 8.1+
- Some bundles support PHP 8.1-8.4 range

Check each library's `composer.json` for specific requirements.

## Additional Notes

- Use `docker compose` (V2), not `docker-compose`
- Never include `composer.lock` files (monorepo uses `"lock": false`)
