# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

The README is a two-line summary; the root `CLAUDE.md` has the monorepo conventions. Everything below comes
from the source.

## Commands

Docker service `dev-configuration-variables-resolver` (PHP 8.2), with a `mockserver` sidecar.

```bash
docker compose run --rm dev-configuration-variables-resolver composer ci    # validate + phpcs + phpstan + tests
docker compose run --rm dev-configuration-variables-resolver composer build # same, without composer validate
docker compose run --rm dev-configuration-variables-resolver vendor/bin/phpunit --filter testResolve tests/VariablesResolverTest.php
```

Required in the repo-root `.env`: `STORAGE_API_URL`, `STORAGE_API_TOKEN`, `STORAGE_API_TOKEN_MASTER`.
Vault interactions in unit tests go through `tests/Mockserver.php` against the `mockserver` service;
`tests/VariablesResolverFunctionalTest.php` hits real Storage.

## Architecture

Two independent substitution mechanisms, composed in a fixed order.

`UnifiedConfigurationResolver::resolveConfiguration()` is the entry point and does exactly one thing:
**shared code first, then variables**. Shared-code snippets may themselves contain variable placeholders,
so reversing the order silently stops resolving them.

`VariablesResolver::resolveVariables()` then runs two resolvers, again order-sensitive:

1. `VariablesResolver\VaultVariablesResolver` — vault variables for the branch, rendered by
   `VariablesRenderer\RegexRenderer`.
2. `VariablesResolver\ConfigurationVariablesResolver` — `keboola.variables` configuration values, rendered
   by `VariablesRenderer\MustacheRenderer`.

The two renderers are **not interchangeable**: vault values are substituted with a regex because Mustache
would choke on (and re-interpret) the surrounding configuration content, while configuration variables go
through real Mustache. Both return a `RenderResults` carrying replaced and missing names.

Missing placeholders are only reported **after both passes** — a name unresolved by vault may still be
provided by configuration variables. `VariablesResolver` aggregates `missingVariables` from both and throws
a single `Exception\UserException` listing them. Do not make either resolver throw on its own.

`ResolveResults` returns the rendered configuration plus the list of replaced variable names, which callers
log; values are deliberately not included so secrets don't reach logs.

`VariablesResolver::create()` is the wiring factory (Storage `Components` client from the *branch* client,
vault `VariablesApiClient`); `UnifiedConfigurationResolverFactory` builds the outer resolver with the
branch id and variable-values selection. Branch id, `variableValuesId` and `variableValuesData` are
constructor state of the resolver, not per-call arguments — construct a new resolver per branch.

`ComponentsClientHelper` is the only place that talks to the Storage Components API (fetching
`keboola.variables` / `keboola.shared-code` configurations); keep API access there so tests can stub one
seam.
