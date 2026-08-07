# AGENTS.md

Guidance for AI coding agents working on the `query-service-api-client` library.

`README.md` documents the public API, constructor options, error behaviour and the env vars functional
tests need. Root `AGENTS.md` has the monorepo conventions.

## Contributing — this repository is a mirror; pull requests go to the monorepo

`query-service-api-client` is developed in the
**[keboola/platform-libraries](https://github.com/keboola/platform-libraries)**
monorepo, under `libs/query-service-api-client/`. It is published to the standalone
**[keboola/query-service-api-php-client](https://github.com/keboola/query-service-api-php-client)**
repository only so that Composer can install it — that repository is a **read-only mirror**. CI
re-splits the monorepo subdirectory into it on every green build and force-pushes the result, so any
commit made there is overwritten and lost.

- **Open pull requests against `keboola/platform-libraries`, never against
  `keboola/query-service-api-php-client`.** A pull request on the mirror cannot be merged and will be closed.
- If the checkout you are in has no `libs/` directory at its root, you are in the mirror. Stop, clone
  `keboola/platform-libraries`, and make the change in `libs/query-service-api-client/` there.
- Commit messages are Conventional Commits scoped to the library: `fix(query-service-api-client): …`.
- A release is a `query-service-api-client/<version>` tag pushed in the monorepo; the mirror's tag is
  derived from it with the `query-service-api-client/` prefix stripped.
- Monorepo-wide conventions (Docker-based dev workflow, coding standards, CI layout) are in the monorepo's
  root `AGENTS.md`.

## Commands

Docker service `dev-query-service-api-client` (PHP **8.4** — the highest in the monorepo; `readonly`
promotion, typed class constants and property hooks are available here but not in most sibling libraries).

```bash
docker compose run --rm dev-query-service-api-client composer ci   # validate + phpcs + phpstan + tests
docker compose run --rm dev-query-service-api-client vendor/bin/phpunit tests/Phpunit          # offline
docker compose run --rm dev-query-service-api-client vendor/bin/phpunit tests/Functional       # needs a real project
```

The suite is split by directory rather than by phpunit suites: `tests/Phpunit/` is fully mocked,
`tests/Functional/` requires `STORAGE_API_TOKEN`, `STORAGE_API_URL` and `HOSTNAME_SUFFIX`.

Note the package name is `keboola/query-api-php-client` and the standalone repo is
`query-service-api-php-client`, neither of which matches the directory name — the mapping lives in the
`split-library` composite action's `case` (see root `AGENTS.md`).

## Architecture

`Client` is the only entry point; it composes a private `ApiClient` from `keboola/php-api-client-base`
(here a **released** `^1.1.1`, not `*@dev` like the sibling clients) with `StorageApiTokenAuthenticator`,
`QueryApiErrorMessageResolver` and `exceptionClass: ClientException::class`.

Two details that aren't obvious from the README:

- **`runId` is implemented as a Guzzle middleware**, not a per-request header: the constructor pushes a
  `Middleware::mapRequest` onto the handler stack that stamps `X-KBC-RunId` on everything. That is also why
  `$requestHandler` accepts either a `Closure` or a full `HandlerStack` — a test handler passed as a closure
  is wrapped, a `HandlerStack` is used as-is so the middleware can be appended.
- **Polling constants live on `Client`**: `DEFAULT_MAX_WAIT_SECONDS = 30`, `DEFAULT_MAX_POLL_WAIT_MS = 1000`.
  `waitForJobCompletion()` is bounded by these, so a long-running query needs an explicit override rather
  than relying on the client to wait indefinitely.

The query lifecycle is three separate calls — submit → poll status → fetch results per statement — because
a job may contain several statements, each with its own result set. `Response\Statement` carries the
per-statement id; `PaginationHelper` and `ResultHelper` handle paging and row shaping over
`JobResultsResponse`.
