# AGENTS.md

Guidance for AI coding agents working on the `settle` library.

`README.md` shows the usage. Root `AGENTS.md` has the monorepo conventions.

## Contributing — this repository is a mirror; pull requests go to the monorepo

`settle` is developed in the **[keboola/platform-libraries](https://github.com/keboola/platform-libraries)**
monorepo, under `libs/settle/`. It is published to the standalone
**[keboola/settle](https://github.com/keboola/settle)** repository only so that Composer can install it
— that repository is a **read-only mirror**. CI re-splits the monorepo subdirectory into it on every
green build and force-pushes the result, so any commit made there is overwritten and lost.

- **Open pull requests against `keboola/platform-libraries`, never against `keboola/settle`.** A pull
  request on the mirror cannot be merged and will be closed.
- If the checkout you are in has no `libs/` directory at its root, you are in the mirror. Stop, clone
  `keboola/platform-libraries`, and make the change in `libs/settle/` there.
- Commit messages are Conventional Commits scoped to the library: `fix(settle): …`.
- A release is a `settle/<version>` tag pushed in the monorepo; the mirror's tag is derived from it with
  the `settle/` prefix stripped.
- Monorepo-wide conventions (Docker-based dev workflow, coding standards, CI layout) are in the monorepo's
  root `AGENTS.md`.

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
