# AGENTS.md

Guidance for AI coding agents working on the `sync-actions-api-php-client` library.

`README.md` documents the public API. Root `AGENTS.md` has the monorepo conventions.

## Contributing — this repository is a mirror; pull requests go to the monorepo

`sync-actions-api-php-client` is developed in the
**[keboola/platform-libraries](https://github.com/keboola/platform-libraries)**
monorepo, under `libs/sync-actions-api-php-client/`. It is published to the standalone
**[keboola/sync-actions-api-php-client](https://github.com/keboola/sync-actions-api-php-client)**
repository only so that Composer can install it — that repository is a **read-only mirror**. CI
re-splits the monorepo subdirectory into it on every green build and force-pushes the result, so any
commit made there is overwritten and lost.

- **Open pull requests against `keboola/platform-libraries`, never against
  `keboola/sync-actions-api-php-client`.** A pull request on the mirror cannot be merged and will be closed.
- If the checkout you are in has no `libs/` directory at its root, you are in the mirror. Stop, clone
  `keboola/platform-libraries`, and make the change in `libs/sync-actions-api-php-client/` there.
- Commit messages are Conventional Commits scoped to the library: `fix(sync-actions-api-php-client): …`.
- A release is a `sync-actions-api-php-client/<version>` tag pushed in the monorepo; the mirror's tag
  is derived from it with the `sync-actions-api-php-client/` prefix stripped.
- Monorepo-wide conventions (Docker-based dev workflow, coding standards, CI layout) are in the monorepo's
  root `AGENTS.md`.

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
