# AGENTS.md

Guidance for AI coding agents working on the `permission-checker` library.

`README.md` covers usage and how to add a checker; the root `AGENTS.md` has the monorepo conventions.

## Contributing — this repository is a mirror; pull requests go to the monorepo

`permission-checker` is developed in the
**[keboola/platform-libraries](https://github.com/keboola/platform-libraries)**
monorepo, under `libs/permission-checker/`. It is published to the standalone
**[keboola/permission-checker](https://github.com/keboola/permission-checker)** repository only so that
Composer can install it — that repository is a **read-only mirror**. CI re-splits the monorepo subdirectory
into it on every green build and force-pushes the result, so any commit made there is overwritten and lost.

- **Open pull requests against `keboola/platform-libraries`, never against `keboola/permission-checker`.**
  A pull request on the mirror cannot be merged and will be closed.
- If the checkout you are in has no `libs/` directory at its root, you are in the mirror. Stop, clone
  `keboola/platform-libraries`, and make the change in `libs/permission-checker/` there.
- Commit messages are Conventional Commits scoped to the library: `fix(permission-checker): …`.
- A release is a `permission-checker/<version>` tag pushed in the monorepo; the mirror's tag is derived
  from it with the `permission-checker/` prefix stripped.
- Monorepo-wide conventions (Docker-based dev workflow, coding standards, CI layout) are in the monorepo's
  root `AGENTS.md`.

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
