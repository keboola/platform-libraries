# AGENTS.md

Guidance for AI coding agents working on the `azure-api-client` library.

`README.md` covers the authenticator options and usage. Root `AGENTS.md` has the monorepo conventions.

## Contributing — this repository is a mirror; pull requests go to the monorepo

`azure-api-client` is developed in the
**[keboola/platform-libraries](https://github.com/keboola/platform-libraries)**
monorepo, under `libs/azure-api-client/`. It is published to the standalone
**[keboola/azure-api-client](https://github.com/keboola/azure-api-client)** repository only so that Composer
can install it — that repository is a **read-only mirror**. CI re-splits the monorepo subdirectory
into it on every green build and force-pushes the result, so any commit made there is overwritten and lost.

- **Open pull requests against `keboola/platform-libraries`, never against `keboola/azure-api-client`.**
  A pull request on the mirror cannot be merged and will be closed.
- If the checkout you are in has no `libs/` directory at its root, you are in the mirror. Stop, clone
  `keboola/platform-libraries`, and make the change in `libs/azure-api-client/` there.
- Commit messages are Conventional Commits scoped to the library: `fix(azure-api-client): …`.
- A release is a `azure-api-client/<version>` tag pushed in the monorepo; the mirror's tag is derived
  from it with the `azure-api-client/` prefix stripped.
- Monorepo-wide conventions (Docker-based dev workflow, coding standards, CI layout) are in the monorepo's
  root `AGENTS.md`.

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
