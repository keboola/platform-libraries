# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`README.md` states what the bundle does (adds Datadog APM metadata to logs, no configuration). Root
`CLAUDE.md` has the monorepo conventions.

## Commands

Docker service `dev-logging-bundle` — note it uses the **generic `dev` image**, not a pinned PHP version,
and `composer.json` declares no `php` constraint. CI runs a Symfony **6.4 / 7.2 matrix across `dev81` and
`dev83`**, so a change that compiles locally can still fail on the other combination.

```bash
docker compose run --rm dev-logging-bundle composer ci   # validate + phpcs + phpstan + tests
docker compose run --rm dev-logging-bundle vendor/bin/phpunit tests/KeboolaLoggingBundleFunctionalTest.php
```

`composer.json` sets `config.policy.advisories.block=false`. That is deliberate: Composer ≥2.10 blocks
advisory-affected versions by default, which made the Symfony 7.2 resolution unsatisfiable in CI. Don't
remove it without re-checking that matrix.

## Architecture

Three classes. `DependencyInjection\KeboolaLoggingExtension::load()` checks
`function_exists('DDTrace\current_context')` and registers `Monolog\DataDogContextProcessor` (tagged
`monolog.processor`) **only when the ddtrace extension is present** — that runtime check is why the bundle
needs no configuration and is safe to install everywhere.

The processor writes `extra.dd.trace_id` / `extra.dd.span_id`, which is the exact shape Datadog's log/trace
correlation expects; renaming those keys silently breaks correlation without failing anything.

Because the real extension isn't available in the test image, `tests/datadogStubs.php` provides a stub
`DDTrace\current_context()` so the functional test can exercise the "extension present" branch. The
"extension absent" branch is what runs in every other library's environment, so keep both covered.
