# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`README.md` covers installation, the `x_connect_retries` option and the test environment variables. Root
`CLAUDE.md` has the monorepo conventions.

## Commands

Docker service `dev-doctrine-retry-bundle` (PHP **8.4**). It depends on the `mysql` and `toxiproxy`
services and hard-codes all `TEST_DATABASE_*` / `TEST_PROXY_HOST` values in `docker-compose.yml`, so no
`.env` entries are needed — but the MySQL healthcheck must pass before the container starts, which makes
the first run noticeably slow.

```bash
docker compose run --rm dev-doctrine-retry-bundle composer ci      # validate + phpcs + phpstan + phpunit
docker compose run --rm dev-doctrine-retry-bundle vendor/bin/phpunit --filter testRetriesOnConnectionFailure tests/Database/ConnectionWithRetryTest.php
```

Tests connect to MySQL **through Toxiproxy** and inject faults via its API (`http://toxiproxy:8474`) to
simulate connection failures — that is the only way to exercise the retry path. A leftover toxic from an
aborted run will make unrelated tests fail; recreate the `toxiproxy` container if results look impossible.

This library requires PHP `^8.4`, `doctrine/dbal ^4.2` and Symfony `^7.4|^8.0` — the newest stack in the
monorepo. Don't copy version constraints from sibling libraries here.

## Architecture

Four classes, and the interesting part is the compiler pass, not the retry itself.

`Database\Retry\Middleware` is a DBAL driver middleware that wraps the driver in `Database\Retry\Driver`,
which overrides **only `connect()`** and routes it through a `keboola/retry` `RetryProxy`. Queries are
deliberately *not* retried — replaying a statement after a mid-transaction failure is unsafe; only
establishing the connection is.

`DependencyInjection\Compiler\ConfigureDbalRetryProxyPass` is what makes the bundle zero-config: at compile
time it walks every connection in the `doctrine.connections` parameter, reads
`driverOptions.x_connect_retries` from the connection definition, skips the connection entirely when it is
absent or `0`, and otherwise **prepends** a per-connection retry middleware to the DBAL `Configuration`
service's `setMiddlewares()` call. Prepending matters: the retry wrapper must be outermost so it sees
failures from every other middleware.

Because it rewrites an existing `setMiddlewares()` method call (remove, then re-add with the new list), a
change in how DoctrineBundle registers middlewares will silently drop this bundle's behaviour rather than
error — `tests/Database/ConnectionWithRetryTest.php` is the guard against that, and
`tests/FailingRetryProxy.php` is the seam it uses to force the failure path.
