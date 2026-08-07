# AGENTS.md

Guidance for AI coding agents working on the `sandboxes-service-api-client` library.

`README.md` documents both clients' public methods with examples. Root `AGENTS.md` has the monorepo
conventions.

## Contributing — this repository is a mirror; pull requests go to the monorepo

`sandboxes-service-api-client` is developed in the
**[keboola/platform-libraries](https://github.com/keboola/platform-libraries)**
monorepo, under `libs/sandboxes-service-api-client/`. It is published to the standalone
**[keboola/sandboxes-service-api-php-client](https://github.com/keboola/sandboxes-service-api-php-client)**
repository only so that Composer can install it — that repository is a **read-only mirror**. CI
re-splits the monorepo subdirectory into it on every green build and force-pushes the result, so any
commit made there is overwritten and lost.

- **Open pull requests against `keboola/platform-libraries`, never against
  `keboola/sandboxes-service-api-php-client`.** A pull request on the mirror cannot be merged and will
  be closed.
- If the checkout you are in has no `libs/` directory at its root, you are in the mirror. Stop, clone
  `keboola/platform-libraries`, and make the change in `libs/sandboxes-service-api-client/` there.
- Commit messages are Conventional Commits scoped to the library: `fix(sandboxes-service-api-client): …`.
- A release is a `sandboxes-service-api-client/<version>` tag pushed in the monorepo; the mirror's tag
  is derived from it with the `sandboxes-service-api-client/` prefix stripped.
- Monorepo-wide conventions (Docker-based dev workflow, coding standards, CI layout) are in the monorepo's
  root `AGENTS.md`.

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
