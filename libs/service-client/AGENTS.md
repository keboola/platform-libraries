# AGENTS.md

Guidance for AI coding agents working on the `service-client` library.

`README.md` shows the usage. Root `AGENTS.md` has the monorepo conventions.

## Contributing — this repository is a mirror; pull requests go to the monorepo

`service-client` is developed in the
**[keboola/platform-libraries](https://github.com/keboola/platform-libraries)**
monorepo, under `libs/service-client/`. It is published to the standalone
**[keboola/service-client](https://github.com/keboola/service-client)** repository only so that Composer
can install it — that repository is a **read-only mirror**. CI re-splits the monorepo subdirectory
into it on every green build and force-pushes the result, so any commit made there is overwritten and lost.

- **Open pull requests against `keboola/platform-libraries`, never against `keboola/service-client`.**
  A pull request on the mirror cannot be merged and will be closed.
- If the checkout you are in has no `libs/` directory at its root, you are in the mirror. Stop, clone
  `keboola/platform-libraries`, and make the change in `libs/service-client/` there.
- Commit messages are Conventional Commits scoped to the library: `fix(service-client): …`.
- A release is a `service-client/<version>` tag pushed in the monorepo; the mirror's tag is derived from
  it with the `service-client/` prefix stripped.
- Monorepo-wide conventions (Docker-based dev workflow, coding standards, CI layout) are in the monorepo's
  root `AGENTS.md`.

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
