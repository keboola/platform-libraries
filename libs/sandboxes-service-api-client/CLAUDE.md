# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`README.md` documents both clients' public methods with examples. Root `CLAUDE.md` has the monorepo
conventions.

## Commands

Docker service `dev-sandboxes-service-api-client` (PHP 8.2), with a `mockserver` sidecar.

```bash
docker compose run --rm dev-sandboxes-service-api-client composer ci   # validate + phpcs + phpstan + tests + infection
docker compose run --rm dev-sandboxes-service-api-client vendor/bin/phpunit --filter testListApps tests/Apps/AppsApiClientTest.php
```

`composer ci` includes **Infection with `--min-covered-msi=90`**, reading coverage from `/tmp/build-logs`
written by `composer tests`. `composer phpcs` scans `.` with `--ignore=vendor,cache,Kernel.php`.

Tests drive the `mockserver` service through `tests/Mockserver.php` rather than a Guzzle mock handler, so
they exercise real HTTP; `tests/ReflectionPropertyAccessTestCase.php` is used to assert on the private
`ApiClient` composed inside each facade.

## Architecture

Two independent clients over **two different services**, sharing only the transport base
(`keboola/php-api-client-base`, `*@dev`) and `SandboxesErrorMessageResolver`:

- `Sandboxes\SandboxesApiClient` → the data-science / sandboxes service
- `Apps\AppsApiClient` → the data-apps service

They take different base URLs and are constructed separately; there is no umbrella client, and no shared
state. Adding an endpoint means picking the right one rather than a generic router.

`Sandboxes\Legacy\` holds the older model shapes (`Sandbox`, `SandboxCredentials`, `Project`,
`PersistentStorage`, `SandboxSizeParameters`). They are still returned by the sandboxes endpoints and are
namespaced `Legacy` to mark them as frozen — new work should add models outside that namespace rather than
extending these.

All failures surface as `Exception\SandboxesServiceClientException`, with message text produced by
`SandboxesErrorMessageResolver` — the single place that knows the services' error body shape.
