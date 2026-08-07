# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`README.md` shows the usage. Root `CLAUDE.md` has the monorepo conventions.

## Commands

Docker service `dev-settle` (PHP **8.4**; the `composer.json` constraint is `^8.2`, and the dev PHP version
comes from the `*dev84` anchor in `docker-compose.yml`).

```bash
docker compose run --rm dev-settle composer ci   # validate + phpcs + phpstan + tests + infection
docker compose run --rm dev-settle vendor/bin/phpunit --filter testSettle tests/SettleTest.php
```

`composer ci` includes **Infection with `--min-covered-msi=90`**, reading coverage from `build/logs`
written by `composer tests`. `phpstan.neon.dist` (not `phpstan.neon`) is the config file here.

## Architecture

`Settle::settle()` polls `getCurrentValue()` until `comparator()` accepts the result, then **returns the
matched value** — it is not a void wait, and callers use the return value. On exhaustion it throws a plain
`RuntimeException` whose message embeds the JSON-encoded last value.

Two details that matter when changing it:

- The backoff is `min(2 ** $attempt, $maxAttemptsDelay)` seconds, i.e. exponential **capped** by
  `maxAttemptsDelay` — that constructor argument is a per-attempt ceiling, not a total timeout. The value is
  checked before the delay, so `maxAttempts` attempts means `maxAttempts - 1` sleeps.
- The `@template TValue` annotation on `settle()` is what propagates the polled value's type to the caller
  and into the comparator. Keep the generic intact; widening it to `mixed` costs every consumer its types.

`Comparator\` (`IsSame`, `IsTrue`, `InArray`) holds ready-made invokable comparators for the common cases;
`SettleFactory` is the wiring seam that binds the logger so `Settle` itself stays free of construction
concerns. Every attempt is logged at debug level with the encoded current value — that means **the polled
value ends up in logs**, so don't settle on secrets.
