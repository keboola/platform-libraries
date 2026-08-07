# AGENTS.md

Guidance for AI coding agents working on the `git-service-api-client` library.

`README.md` documents the constructor, the endpoints and the error type. Root `AGENTS.md` has the monorepo
conventions.

## Contributing — this repository is a mirror; pull requests go to the monorepo

`git-service-api-client` is developed in the
**[keboola/platform-libraries](https://github.com/keboola/platform-libraries)**
monorepo, under `libs/git-service-api-client/`. It is published to the standalone
**[keboola/git-service-php-api-client](https://github.com/keboola/git-service-php-api-client)** repository
only so that Composer can install it — that repository is a **read-only mirror**. CI re-splits the
monorepo subdirectory into it on every green build and force-pushes the result, so any commit made there
is overwritten and lost.

- **Open pull requests against `keboola/platform-libraries`, never against
  `keboola/git-service-php-api-client`.** A pull request on the mirror cannot be merged and will be closed.
- If the checkout you are in has no `libs/` directory at its root, you are in the mirror. Stop, clone
  `keboola/platform-libraries`, and make the change in `libs/git-service-api-client/` there.
- Commit messages are Conventional Commits scoped to the library: `fix(git-service-api-client): …`.
- A release is a `git-service-api-client/<version>` tag pushed in the monorepo; the mirror's tag is
  derived from it with the `git-service-api-client/` prefix stripped.
- Monorepo-wide conventions (Docker-based dev workflow, coding standards, CI layout) are in the monorepo's
  root `AGENTS.md`.

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
