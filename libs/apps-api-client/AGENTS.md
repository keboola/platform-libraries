# AGENTS.md

Guidance for AI coding agents working on the `apps-api-client` library.

`README.md` documents the constructor, the endpoints and the error type. Root `AGENTS.md` has the monorepo
conventions.

## Contributing — this repository is a mirror; pull requests go to the monorepo

`apps-api-client` is developed in the
**[keboola/platform-libraries](https://github.com/keboola/platform-libraries)**
monorepo, under `libs/apps-api-client/`. It is published to the standalone
**[keboola/apps-api-php-client](https://github.com/keboola/apps-api-php-client)** repository
only so that Composer can install it — that repository is a **read-only mirror**. CI re-splits the
monorepo subdirectory into it on every green build and force-pushes the result, so any commit made there
is overwritten and lost.

- **Open pull requests against `keboola/platform-libraries`, never against
  `keboola/apps-api-php-client`.** A pull request on the mirror cannot be merged and will be closed.
- If the checkout you are in has no `libs/` directory at its root, you are in the mirror. Stop, clone
  `keboola/platform-libraries`, and make the change in `libs/apps-api-client/` there.
- Commit messages are Conventional Commits scoped to the library: `fix(apps-api-client): …`.
- A release is an `apps-api-client/<version>` tag pushed in the monorepo; the mirror's tag is
  derived from it with the `apps-api-client/` prefix stripped.
- Monorepo-wide conventions (Docker-based dev workflow, coding standards, CI layout) are in the monorepo's
  root `AGENTS.md`.

## Not the sandboxes client

`sandboxes-service-api-client` also has an `Apps\AppsApiClient` and an `Apps\App` model. Those talk to the
v1 data-apps API on `data-apps.{hostnameSuffix}`. This library is the v2 API on `apps.{hostnameSuffix}`; the
short class names are the same, so check the namespace of an import.

## Commands

Docker service `dev-apps-api-client` (PHP 8.2); no environment variables.

```bash
docker compose run --rm dev-apps-api-client bash -c 'composer install && composer ci'
docker compose run --rm dev-apps-api-client vendor/bin/phpunit --filter testGetApp tests/AppsApiClientTest.php
```

## Conventions when adding endpoints

A thin facade over `keboola/php-api-client-base` (`*@dev`, the local path version), covering only the
endpoints a consumer needs — add more as consumers need them. The API is `/v2` of Sandboxes Service: the
load balancer adds the `/v2` prefix on `apps.{hostnameSuffix}`, so request paths here start at `apps/`, as in
`docs/swagger-apps.yaml` in `keboola/sandboxes-service`.

- Every public method is one `sendRequestAndMapResponse()` call on the one private `ApiClient`.
- Response types live in `Model\` and implement `ResponseModelInterface::fromResponseData()`, nested ones
  included. `fromResponseData()` describes `$data` with a PHPDoc array shape and has no `Assert` calls: a wrong
  type fails on the typed constructor (`TypeError`), which `ApiClient` wraps in `AppsClientException`. A
  missing nullable field becomes `null`. States are plain strings with `STATE_*` / `RUN_STATE_*` / `BUILD_STATE_*` constants, not enums:
  the API is experimental, and a new state must not break response mapping.
- `AppsApiErrorMessageResolver` is the single place that knows the error body shape (`{"error": "..."}`).
