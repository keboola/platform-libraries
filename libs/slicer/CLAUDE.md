# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`README.md` shows how consumers wire the installer into their `composer.json`. Root `CLAUDE.md` has the
monorepo conventions.

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
