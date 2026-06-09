# Design: `keboola/php-api-client-base`

**Date:** 2026-06-09
**Status:** Proposed
**Location:** `platform-libraries/libs/php-api-client-base/`

## 1. Summary

The platform-libraries API clients (`git-service`, `vault`, `azure`, `sandboxes-service`,
`sync-actions`) each independently re-implement the same HTTP-client skeleton: a Guzzle
wrapper, a configuration object, a retry decider, a JSON helper, a response-model
contract, a client exception, and an auth abstraction. None of them depend on a shared
`keboola/*` base — the only common dependencies are `guzzlehttp/guzzle` and PSR packages.
The utility classes are near-identical across clients (verified by diff): `RetryDecider`,
`Json`, and `ResponseModelInterface` differ only cosmetically.

This document proposes extracting that skeleton into a new shared library,
`keboola/php-api-client-base` (namespace `Keboola\ApiClientBase\`), and migrating all five
clients onto it.

## 2. Goals

- Eliminate the duplicated transport + auth plumbing across the five clients.
- Provide a single, well-tested place to fix retry/JSON/error-handling behavior.
- Ship the genuinely-common authenticators (storage token, manage token, service account)
  so they stop being copy-pasted.
- Keep each client's domain surface (facade classes + `Model/` DTOs) intact — that is what
  most consumer code touches.

## 3. Non-goals

- No new transport features beyond what the clients already do (no streaming, no async).
- No attempt to unify the *domain* layer (each service's facade + models stay
  service-specific).
- Azure's resource-scoped OAuth / managed-identity machinery is **not** moved into the
  base — only its leaf authenticators adopt the base interface (see §7).

## 4. Decisions (locked)

| Decision | Choice | Rationale |
|---|---|---|
| Scope | Base lib **+ migrate all 5 clients** | Max dedup; the mechanics are already near-identical. |
| Auth contract | **Request decorator**: `__invoke(RequestInterface): RequestInterface` | The flexible superset; already used by 4 of 5 clients (vault, azure, sandboxes, sync-actions). Only git-service moves off its header-map. |
| Backward compatibility | **Clean major, no shims** | Shared classes move into the base namespace; clients become thin facades; consumers update `use` imports for config + auth on a major version bump. |
| Packaging | **One base package, batteries included** (Approach 1) | Single dependency per client; removes the duplicated authenticators too, not just the plumbing. |
| Facade ↔ base relationship | **Composition** | Each facade *has-an* `ApiClient`; matches git-service today and avoids PHP single-inheritance constraints. |
| Name / namespace | `keboola/php-api-client-base` / `Keboola\ApiClientBase\` | Explicit "base" framing. |

git-service is treated as the reference ("latest & greatest"): strictest null-safety in
`RetryDecider`, `final` `Json` with docblocks, generic `sendRequestAndMapResponse()`, and a
cleanly factored auth package.

## 5. Package layout

```
platform-libraries/libs/php-api-client-base/
  composer.json                            # keboola/php-api-client-base
  phpstan.neon
  azure-pipelines.tests.yml
  src/
    ApiClient.php                          # Guzzle wrapper — the core
    ApiClientConfiguration.php             # readonly config DTO
    RetryDecider.php                       # retry middleware decider (configurable codes)
    Json.php                               # encodeArray()/decodeArray()
    ResponseModelInterface.php             # fromResponseData(array): self
    Exception/
      ClientException.php                  # base exception
    Auth/
      RequestAuthenticatorInterface.php    # __invoke(RequestInterface): RequestInterface
      StorageApiTokenAuthenticator.php     # X-StorageApi-Token
      ManageApiTokenAuthenticator.php      # X-KBC-ManageApiToken
      KeboolaServiceAccountAuthenticator.php  # projected SA token file -> X-Kubernetes-Authorization
  tests/
    ...
```

**composer.json** requires: `php ^8.2`, `guzzlehttp/guzzle ^7.8`, `psr/http-message`,
`psr/log`, `webmozart/assert`. Dev deps, `phpstan.neon`, and `azure-pipelines.tests.yml`
follow git-service's existing setup (`keboola/coding-standard`, phpstan + extensions,
phpunit with clover/junit coverage output).

## 6. Core components

### 6.1 `ApiClientConfiguration`

```php
namespace Keboola\ApiClientBase;

final class ApiClientConfiguration
{
    public function __construct(
        public readonly ?RequestAuthenticatorInterface $authenticator = null,
        public readonly string $userAgent = 'Keboola PHP API Client',
        public readonly int $backoffMaxTries = 5,
        /** @var list<int> extra non-5xx status codes to retry, e.g. [429] for azure */
        public readonly array $retryableStatusCodes = [],
        public readonly int $connectTimeout = 10,
        public readonly int $requestTimeout = 120,
        public readonly null|Closure|HandlerStack $requestHandler = null,
        public readonly LoggerInterface $logger = new NullLogger(),
        /** Optional per-service mapper from an error response to a message; see §6.4 */
        public readonly ?Closure $errorMessageResolver = null,
    ) {}
}
```

There is **no `defaultHeaders` option**. `User-Agent` is the only header set as a Guzzle
default; `Content-Type: application/json` is set per request by facades on body-bearing
calls (POST/PUT/PATCH). This matches the prevailing pattern (vault, azure, sandboxes-service
already set only `User-Agent` as a default) and avoids stamping `Content-Type` onto bodyless
GET/DELETE requests. A client that genuinely needs a blanket header can use the
`requestHandler` escape hatch.

### 6.2 `ApiClient`

```php
public function __construct(?string $baseUrl = null, ?ApiClientConfiguration $configuration = null)
```

Builds a single Guzzle client. `baseUrl` is normalized with a trailing slash when provided;
it is nullable to accommodate azure (which constructs without a base URI and uses absolute
request URIs).

Handler stack pushed in git-service's proven order — **auth → retry → log** — so a retried
request re-runs the auth decorator (rotating SA tokens / per-request token resolution keep
working):

1. `Middleware::mapRequest($configuration->authenticator)` when an authenticator is set.
2. `Middleware::retry(new RetryDecider($backoffMaxTries, $logger, $retryableStatusCodes))`
   when `backoffMaxTries > 0`.
3. `Middleware::log($logger, new MessageFormatter(...))`.

Guzzle client options: `base_uri` (normalized), `handler` (the stack), `headers` =
`['User-Agent' => $configuration->userAgent]`, `connect_timeout`, `timeout`.

Public API (unchanged from git-service):

```php
public function sendRequest(RequestInterface $request): void;

/**
 * @template T of ResponseModelInterface
 * @param class-string<T> $responseClass
 * @return ($isList is true ? list<T> : T)
 */
public function sendRequestAndMapResponse(
    RequestInterface $request,
    string $responseClass,
    array $options = [],
    bool $isList = false,
): mixed;
```

Both wrap a private `doSendRequest()` that catches `RequestException`/`GuzzleException` and
normalizes them to `ClientException` (see §6.4).

### 6.3 `RetryDecider`, `Json`, `ResponseModelInterface`

- **`RetryDecider`** — git-service's strict-null version (the cleanest of the five).
  Constructor gains `array $retryableStatusCodes = []`. Decision: retry on a transport error
  **or** `code >= 500` **or** `code ∈ retryableStatusCodes`; never retry other 4xx. Azure
  passes `[429]`. Logs a warning per retry.
- **`Json`** — git-service's `final class Json` verbatim: `encodeArray(array): string`,
  `decodeArray(string): array`, both with `JSON_THROW_ON_ERROR`.
- **`ResponseModelInterface`** — `fromResponseData(array<string, mixed> $data): self`, verbatim.

### 6.4 Error handling

Base `Exception\ClientException` (extends `RuntimeException`). `ApiClient` converts a failed
response into a `ClientException` carrying the HTTP status code. Message extraction:

- Default: best-effort — try to JSON-decode the body and read a message from common keys
  (`error`, then `message`); fall back to the trimmed Guzzle exception message.
- Override: if `ApiClientConfiguration::$errorMessageResolver` is set, it is called with the
  decoded body (or raw string) to produce the message. This covers services with a specific
  error shape (e.g. git-service's `{code, error}`).

Per-client error-shape confirmation is a migration checklist item (§9).

## 7. Auth

```php
namespace Keboola\ApiClientBase\Auth;

interface RequestAuthenticatorInterface
{
    public function __invoke(RequestInterface $request): RequestInterface;
}
```

This signature is byte-identical to the `RequestAuthenticatorInterface` vault and azure
already declare, so those two migrate by changing a `use` line.

Shipped authenticators — all `final readonly`, `#[SensitiveParameter]` on tokens, Webmozart
assertions:

| Authenticator | Header | Replaces today's |
|---|---|---|
| `StorageApiTokenAuthenticator` | `X-StorageApi-Token` | vault / sandboxes-service / sync-actions copies |
| `ManageApiTokenAuthenticator` | `X-KBC-ManageApiToken` | git-service `ManageApiTokenAuth` |
| `KeboolaServiceAccountAuthenticator` | `X-Kubernetes-Authorization: Bearer <token>` (re-reads the projected token file on every call; default path `/var/run/secrets/connection.keboola.com/serviceaccount/token`) | git-service `KeboolaServiceAccountAuth` |

Each authenticator returns a request with the header applied (decorator form). The base
`ApiClient` runs the authenticator via `Middleware::mapRequest`, so it is invoked per
request — file-backed / OAuth-backed authenticators re-resolve their token automatically.

**Azure keeps its entire auth subtree** — `RequestAuthenticatorFactoryInterface`,
`SystemAuthenticatorResolver`, `BearerTokenResolver`, `BearerTokenAuthenticatorFactory`,
`ClientCredentialsAuth`, `ManagedCredentialsAuth`, `StaticBearerTokenAuth`,
`CustomHeaderAuth`, and the token Models. Only the leaf authenticators implement the base
`RequestAuthenticatorInterface`. Azure's resource-scoped factory stays azure-internal; the
azure facade resolves a per-resource authenticator and passes it into the base config at
construction (collapsing azure's current lazy `authenticate($resource)` step into
resolve-then-construct). Token caching remains inside azure's resolver and is unaffected.

## 8. Migration plan (clean major, no shims)

Each client keeps its **facade(s) + `Model/` DTOs** and drops the duplicated plumbing. Each
gets a major version bump; consumers update `use` imports for config + auth.

| Client | Drops (→ base) | Keeps | Consumer-facing break |
|---|---|---|---|
| **git-service** | ApiClient, config, RetryDecider, Json, ResponseModelInterface, Exception, `Auth/*` | `GitServiceApiClient`, `Model/*`, enums (`CredentialType`, `KeyPermission`), `NewCredential` | `auth:` → `authenticator:`; `ManageApiTokenAuth` → base `ManageApiTokenAuthenticator`; auth contract flips header-map → decorator |
| **vault** | plumbing + `Authentication/*` | `Variables/VariablesApiClient`, `Variables/Model/*` | `use` change for the interface + `StorageApiTokenAuthenticator` |
| **sandboxes-service** | plumbing + `Authentication/StorageTokenAuthenticator` | `Apps/*`, `Sandboxes/*` (incl. `Legacy/*`) | swap to base `StorageApiTokenAuthenticator` (gains the interface it lacked) |
| **sync-actions** | plumbing + flat `StorageApiTokenAuthenticator`; gains `Json` | `ActionData`, `Model/*`, facade | biggest reshape — flat `Client.php` → facade-over-`ApiClient`; add per-request `Content-Type` on POST |
| **azure** | ApiClient, config, RetryDecider, Json, ResponseModelInterface, Exception; leaf authenticators adopt base interface | **entire auth factory subtree**, `Marketplace/*`, token Models | 429 rule → `retryableStatusCodes: [429]`; resource factory rewired to feed base config at construction |

### Monorepo wiring

- Clients depend on the base via the monorepo's sibling-lib mechanism (path repository /
  `*` constraint), consistent with how other libs reference each other.
- Add a `php-api-client-base/` entry and tag prefix `php-api-client-base/` to
  `platform-libraries/azure-pipelines.tags.yml` so it splits to its own read-only repo
  (`keboola/php-api-client-base`), plus its own `azure-pipelines.tests.yml`.

## 9. Testing strategy

- **Base lib (test-first, per `/tdd`):** unit tests with Guzzle `MockHandler`:
  - each authenticator sets the expected header on the request;
  - retry matrix — 5xx retried, generic 4xx not retried, configured codes (e.g. 429) retried,
    transport errors retried, `backoffMaxTries = 0` disables retry;
  - `sendRequestAndMapResponse` maps single objects and lists; invalid JSON / mapping failure
    raise `ClientException`;
  - error normalization — default extractor and custom `errorMessageResolver`;
  - `Json` round-trip and throw-on-error behavior.
  - Coverage target matches git-service's bar (clover + junit output).
- **Each migrated client:** its existing test suite must stay green after the
  namespace/auth swap — that is the regression gate. Adjust only namespaces and auth
  construction in tests.

## 10. Open risks / verification checklist

1. **sync-actions** uses a flat `Client.php` rather than a facade-over-`ApiClient`; it is the
   largest structural change and has not yet been read in full.
2. **Per-client error-response shapes** must be audited to wire `errorMessageResolver`
   correctly (git-service `{code, error}` is known; others TBD).
3. **azure's `authenticate($resource)` lifecycle** (lazy, resource-scoped) must be reshaped to
   resolve-then-construct without breaking token caching.
4. **Nullable `baseUrl`** is a concession to azure — confirm the four internal-service
   facades always pass a non-empty base URL.
5. **git-service write methods** — confirm every body-bearing method sets `Content-Type`
   per request before the default is removed (`createRepository` does; verify the rest).

## 11. Rollout order

1. Build and fully test `php-api-client-base` (no client touched yet).
2. Migrate the simplest header-token clients first to validate the base in anger:
   **vault** → **sandboxes-service**.
3. Migrate **git-service** (auth contract flip).
4. Migrate **sync-actions** (structural reshape).
5. Migrate **azure** (auth factory rewiring) last.

Each client migration is its own PR + major release.
