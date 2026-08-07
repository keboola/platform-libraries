# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`README.md` covers usage and how to add a checker; the root `CLAUDE.md` has the monorepo conventions.

## Commands

Docker service `dev-permission-checker` (PHP 8.2); no environment variables.

```bash
docker compose run --rm dev-permission-checker composer ci   # validate + phpcs + phpstan + phpunit + infection
docker compose run --rm dev-permission-checker vendor/bin/phpunit --filter testCanRunJob tests/Check/JobQueue/CanRunJobTest.php
```

`composer ci` includes **Infection with `--min-covered-msi=100`** — mutation coverage must be perfect on
covered code, so a new branch in a checker needs a test that actually distinguishes it. This is the usual
reason CI fails here while PHPUnit passes locally. `composer phpcs` scans `.`, not `src tests`.

## Architecture

Two distinct `StorageApiToken` types are in play, and confusing them is the main trap:

- `Keboola\StorageApiBranch\StorageApiToken` — the external token, what callers pass to
  `PermissionChecker::checkPermissions()`.
- `Keboola\PermissionChecker\StorageApiToken` — this library's own adapter, built by
  `PermissionChecker` via `fromTokenInterface()` and what every checker receives.

The adapter exists so checkers depend only on a narrow, stable view (`Role`, `Feature`, `TokenPermission`,
`BranchType`) rather than on the Storage client's token class. Add derived accessors to the adapter, not to
checkers.

Each permission is a class under `Check/<Service>/` implementing `PermissionCheckInterface` — one class per
action, grouped by consuming service (`JobQueue`, `Vault`, `Scheduler`, `OAuth`, `SandboxesService`,
`RunnerSyncApi`, `Notification`, `EditorService`). Checkers are self-contained: no registry, no
configuration, and any extra data they need comes through their own constructor. Adding one means a new
class plus a test under the mirroring `tests/Check/…` path — nothing else to register.

Failures throw `Exception\PermissionDeniedException` with a message that is **shown to end users**, so
message wording is part of the API and several tests assert on it exactly.

`BranchType` (default vs dev) is a first-class checker input: a permission that is allowed in production may
be denied on a development branch or vice versa, so most checkers take it in their constructor.
