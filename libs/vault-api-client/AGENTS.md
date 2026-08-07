# AGENTS.md

Guidance for AI coding agents working on the `vault-api-client` library.

`README.md` documents the public API and constructor options. Root `AGENTS.md` has the monorepo conventions.

## Contributing — this repository is a mirror; pull requests go to the monorepo

`vault-api-client` is developed in the
**[keboola/platform-libraries](https://github.com/keboola/platform-libraries)**
monorepo, under `libs/vault-api-client/`. It is published to the standalone
**[keboola/vault-api-php-client](https://github.com/keboola/vault-api-php-client)** repository only so
that Composer can install it — that repository is a **read-only mirror**. CI re-splits the monorepo
subdirectory into it on every green build and force-pushes the result, so any commit made there is
overwritten and lost.

- **Open pull requests against `keboola/platform-libraries`, never against `keboola/vault-api-php-client`.**
  A pull request on the mirror cannot be merged and will be closed.
- If the checkout you are in has no `libs/` directory at its root, you are in the mirror. Stop, clone
  `keboola/platform-libraries`, and make the change in `libs/vault-api-client/` there.
- Commit messages are Conventional Commits scoped to the library: `fix(vault-api-client): …`.
- A release is a `vault-api-client/<version>` tag pushed in the monorepo; the mirror's tag is derived
  from it with the `vault-api-client/` prefix stripped.
- Monorepo-wide conventions (Docker-based dev workflow, coding standards, CI layout) are in the monorepo's
  root `AGENTS.md`.

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
