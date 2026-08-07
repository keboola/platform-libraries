# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`README.md` shows the usage. Root `CLAUDE.md` has the monorepo conventions.

## Commands

Docker service `dev-service-client` (PHP 8.3); zero runtime dependencies and no environment variables.

```bash
docker compose run --rm dev-service-client composer ci   # validate + phpcs + phpstan + tests + infection
docker compose run --rm dev-service-client vendor/bin/phpunit --filter testInternalUrl tests/ServiceClientTest.php
```

`composer ci` includes **Infection with `--min-covered-msi=95`**, reading coverage from `/tmp/build-logs`
written by `composer tests`. Every new `Service` case needs a test that pins both its public and internal
URL, or mutation coverage drops below the threshold.

## Architecture

The whole library is the `Service` enum: it maps each Keboola service to a public subdomain
(`getPublicSubdomain()`) and an in-cluster Kubernetes service name (`getInternalServiceName()`), and
`ServiceClient` just formats them — `https://<subdomain>.<hostnameSuffix>` or
`http://<internal-name>.svc.cluster.local`. Note the protocol difference: internal URLs are plain HTTP.

**This enum is the canonical list of Keboola services** and is consumed across the platform (directly, and
via `api-bundle`'s preconfigured `ServiceClient`). Adding a service means adding the case plus both match
arms plus the `get<Name>ServiceUrl()` convenience method; the matches are exhaustive, so a missing arm is a
static-analysis failure rather than a runtime surprise.

Some services intentionally have no public DNS (`GIT_SERVICE`, `QUEUE_INTERNAL_API`) and their
`getPublicSubdomain()` arm **throws** rather than returning a value. Callers that may run in either mode
must therefore pass the DNS type explicitly instead of relying on the default. Keep that pattern for new
internal-only services — a placeholder subdomain would silently produce a URL that resolves to nothing.

`ServiceDnsType` is accepted as either the enum or its string value in the constructor, because consumers
wire it from environment variables (see `api-bundle`'s `default_service_dns_type`).
