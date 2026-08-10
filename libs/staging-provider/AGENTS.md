# AGENTS.md

Guidance for AI coding agents working on the `staging-provider` library.

`README.md` documents the staging types, `StagingProvider`'s four slots, the `WorkspaceProvider` API and the
test environment variables. Root `AGENTS.md` has the monorepo conventions.

## Contributing — this repository is a mirror; pull requests go to the monorepo

`staging-provider` is developed in the
**[keboola/platform-libraries](https://github.com/keboola/platform-libraries)**
monorepo, under `libs/staging-provider/`. It is published to the standalone
**[keboola/staging-provider](https://github.com/keboola/staging-provider)** repository only so that Composer
can install it — that repository is a **read-only mirror**. CI re-splits the monorepo subdirectory
into it on every green build and force-pushes the result, so any commit made there is overwritten and lost.

- **Open pull requests against `keboola/platform-libraries`, never against `keboola/staging-provider`.**
  A pull request on the mirror cannot be merged and will be closed.
- If the checkout you are in has no `libs/` directory at its root, you are in the mirror. Stop, clone
  `keboola/platform-libraries`, and make the change in `libs/staging-provider/` there.
- Commit messages are Conventional Commits scoped to the library: `fix(staging-provider): …`.
- A release is a `staging-provider/<version>` tag pushed in the monorepo; the mirror's tag is derived
  from it with the `staging-provider/` prefix stripped.
- Monorepo-wide conventions (Docker-based dev workflow, coding standards, CI layout) are in the monorepo's
  root `AGENTS.md`.

## Commands

Docker service `dev-staging-provider` (PHP 8.2).

```bash
docker compose run --rm dev-staging-provider composer ci   # validate + phpcs + phpstan + tests
docker compose run --rm dev-staging-provider vendor/bin/phpunit --filter testCreateNewWorkspace tests/Workspace/WorkspaceProviderTest.php
```

## Constraints to preserve

**`Staging\StagingType` is a cross-library contract.** Both mapping libraries `match` exhaustively on it in
their `Staging\StrategyFactory` constructors, so adding a case here is a breaking change that must be
handled in `input-mapping/src/Staging/StrategyFactory.php` and
`output-mapping/src/Staging/StrategyFactory.php`. The exhaustive match is the intended tripwire — don't add
a `default` arm to silence it. Note the two libraries support different subsets: input handles all five
non-`None` types, output rejects `S3` and `Abs`.

**`getExistingWorkspace()` carries a conditional return type**
(`($credentialsData is null ? WorkspaceInterface : WorkspaceWithCredentialsInterface)`). That annotation is
what makes callers type-safe; keep it in sync with the signature. Also note the merge order — supplied
credentials are the base and API data overwrites them.

**Credentials are modelled as separate types**, not a nullable getter. Preserve that split when adding
methods: returning `WorkspaceWithCredentialsInterface` is a promise that credentials are actually present.

`cleanupWorkspace()` swallows 404 only; every other `ClientException` propagates. Tests rely on that,
because a re-run after a failed teardown must not error.

## Tests

Everything except `tests/Workspace/WorkspaceProviderFunctionalTest.php` is unit-level and runs offline; that
one creates real Connection workspaces, so a crashed run can leave workspaces behind in the test project.
