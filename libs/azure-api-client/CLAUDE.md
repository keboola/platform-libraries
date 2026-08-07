# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`README.md` covers the authenticator options and usage. Root `CLAUDE.md` has the monorepo conventions.

## Commands

Docker service `dev-azure-api-client` (PHP **8.1** — the oldest in the monorepo; no `readonly` classes,
no enums in constant expressions, no `json_validate`). It depends on the `mockserver` sidecar.

```bash
docker compose run --rm dev-azure-api-client composer ci   # validate + phpcs + phpstan + tests + infection
docker compose run --rm dev-azure-api-client vendor/bin/phpunit --filter testResolveSubscription tests/Marketplace/MarketplaceApiClientTest.php
```

`composer ci` includes **Infection with `--min-covered-msi=90`**, so a change that adds an untested branch
fails CI even when PHPUnit is green. `composer tests` writes coverage to `/tmp/build-logs` (Infection reads
it from there — running Infection alone without a prior `composer tests` will not work).
`composer phpcs` scans `.` with `--ignore=vendor,cache,Kernel.php`.

`ApiClientFunctionalTest` and the Marketplace tests drive the `mockserver` service via `tests/Mockserver.php`
rather than a Guzzle mock handler, so they exercise real HTTP.

## Architecture

Unlike the other Keboola service clients in this monorepo, this one does **not** build on
`keboola/php-api-client-base` — Azure's OAuth token flow doesn't fit that library's per-request
authenticator model. `ApiClient`, `Json`, `RetryDecider` and `ResponseModelInterface` here are deliberate
parallels of the base library's classes. Keep them aligned in spirit, but don't try to merge them.

### Authentication is two-layered

- **Public authenticators** (`Authentication\Authenticator\`) are what callers pass:
  `ClientCredentialsAuth`, `ManagedCredentialsAuth`, `StaticBearerTokenAuth`, `CustomHeaderAuth`.
- **`Internal\`** holds the machinery: a `BearerTokenResolver` obtains and caches a token *per Azure
  resource*, and `BearerTokenAuthenticatorFactory` turns it into a per-request authenticator. Tokens are
  resource-scoped, which is why the resolver is keyed by resource string rather than being a single token.

`Internal\SystemAuthenticatorResolver` is the default when no authenticator is configured: it reads
`AZURE_TENANT_ID` / `AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET` from the environment and falls back to the
Azure instance metadata service (managed identity) when any are missing. The choice is made lazily on first
token request and then cached for the object's lifetime — so changing env vars mid-process has no effect.

### Marketplace clients

`Marketplace\MarketplaceApiClient` (SaaS subscription resolve/activate) and
`Marketplace\MeteringServiceApiClient` (usage events) are separate clients against separate Azure APIs that
happen to share the authentication stack. `Marketplace\Resources` holds the resource identifiers the token
resolver is keyed on. Batch usage reporting returns partial success — `ReportUsageEventsBatchResult` carries
both `UsageEventResult` and `UsageEventError` entries, so callers must inspect the result rather than relying
on the absence of an exception.
