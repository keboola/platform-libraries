# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`README.md` documents `TestEnvVarsTrait` and the assertion traits with examples. Root `CLAUDE.md` has the
monorepo conventions. The fixture system — the larger half of the library — is not in the README and is
covered here.

## Commands

Docker service `dev-php-test-utils` (PHP **8.4**).

```bash
docker compose run --rm dev-php-test-utils composer ci    # validate + phpcs + phpstan + tests
docker compose run --rm dev-php-test-utils vendor/bin/phpunit --filter testFixtureCache tests/Fixtures/FixtureCacheTest.php
```

Requires `HOSTNAME_SUFFIX` and `TEST_STORAGE_API_TOKEN_SNOWFLAKE` (a **master** token to a Snowflake
project) in the repo-root `.env`. Note the env var name differs from every other library's
`STORAGE_API_TOKEN`.

Because this library is a dev dependency of other services, changes here can break their test suites
without breaking anything in production.

## Architecture

### `Fixtures\` — declarative Storage/database fixtures for Symfony app tests

`FixtureAwareTestCase` extends Symfony's `WebTestCase` and is annotated `#[Group('slow-tests')]`, so
consuming projects can exclude the whole family with `--exclude-group slow-tests`.

A test method declares what it needs with attributes, and the base class reflects over them in `setUp()`:

- `#[FixtureBackend(BackendType::…)]` selects the backend; absent means `BackendType::SNOWFLAKE`.
- `#[ReusableFixtures]` marks the fixture as shareable across test methods.

`FixtureCache` keys reusable fixtures by `(fixtureName, backend)` and non-reusable ones additionally by
`(methodName, dataName)` — the extra dimensions exist so data-provider cases don't collide. It registers a
shutdown function to tear fixtures down; a fatal error therefore leaks real Storage resources, which is the
usual cause of orphaned buckets in the test project.

Fixture classes implement `Dynamic\FixtureInterface` (`BranchFixture`, `ConfigurationFixture`,
`StorageTablesFixture`, `SharedCodeFixture`, `VariableFixture`, …) and capabilities are mixed in via
`FixtureTraits\` (`EntityManagerTrait`, `KernelBrowserTrait`, `StorageApiAwareTrait`, `StorageTokenTrait`).
`FixtureAwareTestCase::initializeTrait()` inspects which trait a fixture uses and injects the matching
dependency — so **adding a trait requires a corresponding branch there**, or the fixture silently gets no
dependency.

### `TestEnvVarsTrait`

`getRequiredEnv()` / `getOptionalEnv()` return `non-empty-string` types, which is why callers can pass them
straight into `non-empty-string`-typed constructors without an assertion. `overrideEnv()` writes to both
`$_ENV` and `putenv()` because the two are read by different libraries — don't replace it with either one
alone.
