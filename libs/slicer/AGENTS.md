# AGENTS.md

Guidance for AI coding agents working on the `slicer` library.

`README.md` shows how consumers wire the installer into their `composer.json`. Root `AGENTS.md` has the
monorepo conventions.

## Contributing — this repository is a mirror; pull requests go to the monorepo

`slicer` is developed in the **[keboola/platform-libraries](https://github.com/keboola/platform-libraries)**
monorepo, under `libs/slicer/`. It is published to the standalone
**[keboola/slicer](https://github.com/keboola/slicer)** repository only so that Composer can install it
— that repository is a **read-only mirror**. CI re-splits the monorepo subdirectory into it on every
green build and force-pushes the result, so any commit made there is overwritten and lost.

- **Open pull requests against `keboola/platform-libraries`, never against `keboola/slicer`.** A pull
  request on the mirror cannot be merged and will be closed.
- If the checkout you are in has no `libs/` directory at its root, you are in the mirror. Stop, clone
  `keboola/platform-libraries`, and make the change in `libs/slicer/` there.
- Commit messages are Conventional Commits scoped to the library: `fix(slicer): …`.
- A release is a `slicer/<version>` tag pushed in the monorepo; the mirror's tag is derived from it with
  the `slicer/` prefix stripped.
- Monorepo-wide conventions (Docker-based dev workflow, coding standards, CI layout) are in the monorepo's
  root `AGENTS.md`.

## Commands

Docker service `dev-slicer` — uses the **generic `dev` image**, not a pinned PHP version. `composer.json`
requires PHP `>=8.1`, the lowest floor in the monorepo, because this package is installed into consumers
that may run older runtimes.

```bash
docker compose run --rm dev-slicer composer ci   # validate + phpcs + phpstan + tests
docker compose run --rm dev-slicer vendor/bin/phpunit --filter testGetPlatformName tests/MachineTypeResolverTest.php
```

`composer ci` does **not** run Infection here even though `infection.json.dist` exists — the `infection`
script is defined but only `build` (phpcs + phpstan + tests) is wired into `ci`.

## What this library actually is

Not a slicing implementation — an **installer**. `Slicer::installSlicer()` is meant to be called from a
consumer's `pre-autoload-dump` composer script; it downloads the platform-appropriate release binary of
[`keboola/processor-split-table`](https://github.com/keboola/processor-split-table/releases) to
`bin/slicer` inside this package, and `Slicer::getBinaryPath()` is how consumers locate it.

Consequences:

- `output-mapping` declares this in its `pre-autoload-dump`; installing its dependencies with
  `--no-scripts` leaves `bin/slicer` missing and its slicing tests fail with a missing-binary error.
- The download happens at install time and reaches the network, so a Composer install in an offline or
  egress-restricted environment fails here.

`MachineTypeResolver` maps `php_uname('m')` / `PHP_OS` onto the release asset naming — `macos|win|linux`,
`amd64|arm64`, `.exe` suffix on Windows — and throws for anything that isn't 64-bit. `UrlResolver` builds
the release URL and `Downloader` fetches it. Adding platform support means a new arm in the resolver plus a
matching release asset upstream; the resolver is unit-tested with synthetic uname strings, so it needs no
real platform to cover.
