# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`README.md` documents the public API. Root `CLAUDE.md` has the monorepo conventions.

## Commands

Docker service `dev-sync-actions-api-php-client` (PHP **8.4**).

```bash
docker compose run --rm dev-sync-actions-api-php-client composer ci   # validate + phpcs + phpstan + phpunit
docker compose run --rm dev-sync-actions-api-php-client vendor/bin/phpunit tests/SyncActionsApiClientTest.php   # offline
docker compose run --rm dev-sync-actions-api-php-client vendor/bin/phpunit tests/ClientFunctionalTest.php       # real service
```

`tests/ClientFunctionalTest.php` needs `STORAGE_API_TOKEN` and `HOSTNAME_SUFFIX` in the repo-root `.env`
(the service URL is derived from the suffix); everything else runs against a mocked handler.
`composer phpcs` scans `.` with `--ignore=vendor`.

The package name is `keboola/sync-actions-client` while the directory and standalone repo are
`sync-actions-api-php-client` — the README's `git clone` instructions predate the monorepo; develop it here.

## Architecture

A thin facade over `keboola/php-api-client-base` (`*@dev`, the local path version), following the same
shape as the other service clients: private `ApiClient`, `SyncActionsErrorMessageResolver`, and a
service-specific `Exception\SyncActionsClientException`.

The one deliberate departure from the house style: **`Model\ActionResponse::$data` is a `stdClass`, not a
typed model or array.** Sync actions return whatever the invoked component chose to emit, so the payload
cannot be modelled — callers inspect it themselves. Don't "fix" this by decoding to an array; consumers
depend on the object shape.

`ActionData` is the request-side value object (component id, action name, optional configuration);
`Model\ListActionsResponse` wraps the discovery endpoint's collection.
