# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`README.md` documents the staging types, `StagingProvider`'s four slots, the `WorkspaceProvider` API and the
test environment variables. Root `CLAUDE.md` has the monorepo conventions.

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
