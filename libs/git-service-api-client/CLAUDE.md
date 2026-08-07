# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`README.md` documents the constructor, the endpoints and the error type. Root `CLAUDE.md` has the monorepo
conventions.

## Commands

Docker service `dev-git-service-api-client` (PHP 8.2); no environment variables.

```bash
docker compose run --rm dev-git-service-api-client composer ci   # validate + phpcs + phpstan + phpunit
docker compose run --rm dev-git-service-api-client vendor/bin/phpunit --filter testListCommits tests/GitServiceApiClientTest.php
```

`composer phpcs` scans `.` with `--ignore=vendor,cache`, not `src tests`. This library has no `vendor/` in
the working tree — run `composer install` in the container first.

## Conventions when adding endpoints

A thin facade over `keboola/php-api-client-base` (`*@dev`, the local path version): the constructor picks an
authenticator, composes a private `ApiClient` with `GitServiceErrorMessageResolver` and
`exceptionClass: GitServiceClientException::class`, and every public method is one
`sendRequestAndMapResponse()` call. Because the dependency is `*@dev`, a base-client constructor change
breaks this library at install time.

- Response types live in `Model\` and implement `ResponseModelInterface::fromResponseData()`. Collection
  endpoints get a wrapper type (`CommitList`, `CredentialListWrapper`, `GitRefListWrapper`) because the API
  wraps collections in an envelope — don't return bare arrays from the transport layer.
- Request-side value objects live at the root. `NewCredential` is the pattern to follow for any endpoint
  whose body is a discriminated union: a private constructor validating the type/field combination, plus a
  named factory per variant, so an invalid request cannot be constructed.
- `GitServiceErrorMessageResolver` is the single place that knows the service's error body shape.
