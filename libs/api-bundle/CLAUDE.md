# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

See `README.md` for the public API (configuration reference, auth attributes, token exchange, the
`AuthenticatorTestTrait` helpers) and the root `CLAUDE.md` for monorepo conventions. This file only adds
what neither covers.

## Commands

Docker service `dev-api-bundle` (PHP 8.3); no environment variables required.

```bash
docker compose run --rm dev-api-bundle composer ci    # validate + phpcs + phpstan + tests
docker compose run --rm dev-api-bundle vendor/bin/phpunit --filter testAuthenticate tests/Security/AttributeAuthenticatorTest.php
```

`composer.json` allows PHP 8.1+ (lower than the monorepo default) — keep syntax compatible even though CI
runs 8.3.

## How the authentication wiring fits together

`Security\AttributeAuthenticator` is the only Symfony authenticator; the per-scheme classes are not
firewall authenticators themselves. It:

1. asks `Util\ControllerReflector` for `AuthAttributeInterface` attributes on the resolved controller —
   `supports()` is false when there are none, so unguarded routes never enter this path;
2. resolves a `TokenAuthenticatorInterface` from an injected service locator **keyed by the attribute
   class name**;
3. runs `extractCredential()` → `authenticateToken()` → `authorizeToken()`, returning on the first success.

Consequences worth knowing:

- Attributes are OR'd and failures are accumulated, but only the **last** error is thrown — a controller
  with several attributes reports the last scheme's message, which can be misleading when debugging a 401.
- Because the locator is keyed by attribute class, a missing optional client package surfaces as
  `Service "…\ApplicationTokenAuth" not found … smaller service locator` rather than a clear error.

Adding a scheme means four coordinated pieces: an attribute implementing `AuthAttributeInterface`, an
authenticator implementing `TokenAuthenticatorInterface`, a token implementing `TokenInterface`, and a
locator entry in `Resources/config/api_bundle.php`.

`StorageApiToken::getTokenType()` (`AuthType::STORAGE_TOKEN` / `AuthType::BEARER`) records the transport the
request actually used, so a bearer-carried value is never re-sent as an `X-StorageApi-Token`.

## Storage client options merging

`keboola_api.storage_client_options` values are **merged onto** the bundle-built base via
`ClientOptions::addValuesFrom()` — never reconstruct the Connection URL resolution when touching
`StorageClientApiFactory`. `StorageClientApiFactoryResolver` implements the nullable-argument behaviour
(`?StorageClientApiFactory` yields null when the request authenticated via a non-Storage scheme).

## Request mapping

`RequestMapper\ArgumentResolver` + `DataMapper` hydrate controller arguments via `cuyz/valinor-bundle`.
New mapper attributes implement `RequestMapperAttributeInterface`; the resolver discovers them generically,
so no additional DI registration is needed.
