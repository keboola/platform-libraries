# AGENTS.md

Guidance for AI coding agents working on the `php-api-client-base` library.

See `README.md` for what the library provides and how to build a service client on top of it, and the root
`AGENTS.md` for monorepo conventions.

## Contributing — this repository is a mirror; pull requests go to the monorepo

`php-api-client-base` is developed in the
**[keboola/platform-libraries](https://github.com/keboola/platform-libraries)**
monorepo, under `libs/php-api-client-base/`. It is published to the standalone
**[keboola/php-api-client-base](https://github.com/keboola/php-api-client-base)** repository only so that
Composer can install it — that repository is a **read-only mirror**. CI re-splits the monorepo subdirectory
into it on every green build and force-pushes the result, so any commit made there is overwritten and lost.

- **Open pull requests against `keboola/platform-libraries`, never against `keboola/php-api-client-base`.**
  A pull request on the mirror cannot be merged and will be closed.
- If the checkout you are in has no `libs/` directory at its root, you are in the mirror. Stop, clone
  `keboola/platform-libraries`, and make the change in `libs/php-api-client-base/` there.
- Commit messages are Conventional Commits scoped to the library: `fix(php-api-client-base): …`.
- A release is a `php-api-client-base/<version>` tag pushed in the monorepo; the mirror's tag is derived
  from it with the `php-api-client-base/` prefix stripped.
- Monorepo-wide conventions (Docker-based dev workflow, coding standards, CI layout) are in the monorepo's
  root `AGENTS.md`.

## Commands

Docker service `dev-php-api-client-base` (PHP 8.2); no environment variables — everything is unit-tested
against mocked Guzzle handlers.

```bash
docker compose run --rm dev-php-api-client-base composer ci   # validate + phpcs + phpstan + phpunit
docker compose run --rm dev-php-api-client-base vendor/bin/phpunit --filter testRetry tests/RetryDeciderTest.php
```

`composer phpcs` here scans `.` with `--ignore=vendor,cache`, not `src tests`.

## Blast radius

This is the transport layer for every Keboola service client in the monorepo — `vault-api-client`,
`sandboxes-service-api-client`, `git-service-api-client`, `sync-actions-api-php-client`,
`query-service-api-client` (plus `azure-api-client`, which mirrors the same design without depending on it).
CI's affected-libraries resolver pulls all of them in on any change here, so treat `ApiClient`'s
constructor, `ApiClientOptions` and `Auth\RequestAuthenticatorInterface` as a compatibility boundary.

Most consumers depend on `*@dev` (the path repository) rather than a released version, so a signature
change breaks them at `composer install` time, not at review time.

This package is also pulled in **transitively**: `keboola/storage-api-client` requires it as `^1.0` since
v18.10. That collides with the monorepo layout — the `../../libs/*` path repository is canonical and higher
priority, so it shadows Packagist with `dev-<current branch>`, which no `^1.x` constraint can satisfy, and
the library fails to install. Libraries hitting this pin the path version explicitly in their own
`repositories` block (see `input-mapping`, `output-mapping`):

```json
"options": { "versions": { "keboola/php-api-client-base": "1.1.2" } }
```

Keep that pin in step with the released version when a new one is tagged. Two things that look like fixes
but are not:

- A `dev-main` **branch alias** here. CI checks out only the pull request branch, so the path repository
  reports `dev-<branch>` and the alias never applies — it passes locally and fails in CI.
- Requiring `keboola/php-api-client-base: "*@dev"` in the affected library. Stability flags only take
  effect in the **root** `composer.json`, so the dev version is then rejected by every downstream consumer
  (`output-mapping`, `job-runner`) under their own `minimum-stability: stable`.

## Design invariants to preserve

- **Constructor args vs options is a semantic split, not style.** `ApiClient` constructor arguments
  (authenticator, `errorMessageResolver`, `retryableStatusCodes`) describe *the service's API contract* and
  are set by the service facade; `ApiClientOptions` carries *caller* preferences (retries, timeouts,
  logger) only. Don't migrate one into the other for convenience.
- The authenticator is required with no implicit default — `NoAuthAuthenticator` exists so that
  "unauthenticated" is an explicit choice.
- `KeboolaServiceAccountAuthenticator`'s file read is deliberately not a plain `file_get_contents`: it
  re-reads per request for kubelet rotation, clears PHP's stat cache and retries with bounded backoff to
  survive the rotation race. Simplifying it reintroduces intermittent auth failures in production.
- `RetryDecider` must never retry 4xx.
