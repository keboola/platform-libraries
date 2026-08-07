# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

See `README.md` for what the library provides and how to build a service client on top of it, and the root
`CLAUDE.md` for monorepo conventions.

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
