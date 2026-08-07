# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`README.md` documents the public API, constructor options, error behaviour and the env vars functional
tests need. Root `CLAUDE.md` has the monorepo conventions.

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
`split-library` composite action's `case` (see root `CLAUDE.md`).

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
