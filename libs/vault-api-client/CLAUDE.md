# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`README.md` documents the public API and constructor options. Root `CLAUDE.md` has the monorepo conventions.

## Commands

Docker service `dev-vault-api-client` (PHP 8.2); no environment variables — the suite runs against mocked
Guzzle handlers.

```bash
docker compose run --rm dev-vault-api-client composer ci   # validate + phpcs + phpstan + phpunit + infection
docker compose run --rm dev-vault-api-client vendor/bin/phpunit --filter testListVariables tests/Variables/VariablesApiClientTest.php
```

`composer ci` includes **Infection with `--min-covered-msi=90`**, reading coverage from `/tmp/build-logs`
written by `composer phpunit` — run the tests before Infection. `composer phpcs` scans `.` with
`--ignore=vendor,cache,Kernel.php`.

## Architecture

A thin facade over `keboola/php-api-client-base` (`*@dev`, the local path version): the constructor composes
a private `ApiClient` with `StorageApiTokenAuthenticator`, `VaultErrorMessageResolver` and
`exceptionClass: VaultClientException::class`; every public method is a `sendRequestAndMapResponse()` call.
A base-client constructor change breaks this library at install time.

Points worth knowing:

- `DEFAULT_BACKOFF_MAX_TRIES = 10` here, versus 3 in most sibling clients. Vault is on the hot path of
  configuration resolution, so transient failures are retried harder — don't "normalize" it downward.
- `Variable::FLAG_*` constants are the source of truth for variable flags and are enforced through the
  `array<Variable::FLAG_*>` phpstan type on `createVariable()`, not at runtime.
- Scoped listing (`listScopedVariablesForBranch()`) is a distinct endpoint from `listVariables()` +
  `ListOptions` paging — branch scoping is server-side, not a filter applied to a full list.

### Downstream

`configuration-variables-resolver` depends on this client (`*@dev`) and resolves vault placeholders through
it, so behavioural changes here surface as configuration-resolution changes for components. CI pulls that
library into the matrix on any change here.
