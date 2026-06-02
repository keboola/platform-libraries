# Storage API Token Exchange (Connection programmatic tokens)

## Summary

This feature lets a Keboola API application (any service using `api-bundle`) accept the new
Connection **programmatic bearer tokens** (`kbc_at_*` access tokens and `kbc_pat_*` personal
access tokens) on endpoints that today require a legacy Storage API token.

Instead of routing all traffic through a central proxy (as described in the
[`auth-bridge-proxy` RFC](https://github.com/keboola/go-monorepo/blob/main/docs/rfcs/2026-05-18-auth-bridge-proxy.md)),
each service performs the exchange itself: it calls a dedicated **internal Connection
resolver endpoint**, authenticating with its own projected Kubernetes ServiceAccount JWT,
hands over the user's bearer token, and receives the matching **legacy Storage token**. From
that point on the request is handled exactly as if the caller had sent the legacy Storage
token, so existing controllers (`#[StorageApiTokenAuth]`, `#[CurrentUser] StorageApiToken`)
keep working unchanged.

> **Temporary location.** The resolver HTTP client lives inside `api-bundle` for now as a
> working demonstration. Its natural home is the Manage API client
> (`keboola/kbc-manage-api-php-client`) as a `ManageApiClient::resolveStorageToken()` method,
> because the endpoint is a `/manage/...` route authenticated by a synthetic Manage token.
> The client is hidden behind `StorageTokenResolverInterface` so the move is a one-line swap
> of the wired implementation. See [Follow-up](#follow-up).

## Connection resolver contract

Implemented in [keboola/connection#7403](https://github.com/keboola/connection/pull/7403).

```
POST /manage/internal/auth-bridge/resolve-storage-token
```

Request:

```http
POST /manage/internal/auth-bridge/resolve-storage-token HTTP/1.1
X-Kubernetes-Authorization: Bearer <projected service-account JWT>   # caller (the service) identity
X-Subject-Token: Bearer kbc_at_... | kbc_pat_...                      # the user token to resolve
Content-Type: application/json

{ "projectId": 123 }
```

Response `200`:

```json
{
  "storageToken": "<legacy-storage-token>",
  "projectId": 123,
  "tokenId": "12345",
  "userId": "67890",
  "expiresAt": "2026-05-18T12:30:00+00:00"
}
```

Errors:

| Status | Meaning |
| --- | --- |
| `400` | Missing / invalid `projectId`. |
| `401` | Subject token missing, malformed, expired, revoked, or otherwise invalid. |
| `403` | Subject token cannot access the project, **or** the caller is not Kubernetes-authenticated / lacks the required scope. |

The caller must present a Kubernetes ServiceAccount JWT that Connection maps to a synthetic
Manage token holding the scope `internal:auth-bridge:resolve-storage-token`.

## Request flow

```
Client ──Authorization: Bearer kbc_at_*, X-KBC-ProjectId: 123──► Service (api-bundle)
                                                                    │
  AuthBridgeStorageTokenResolver:                                   │
    read SA JWT from /var/run/secrets/.../token (re-read per call)  │
    POST {connection}/manage/internal/auth-bridge/                  ▼
         resolve-storage-token                              Connection
    X-Kubernetes-Authorization: Bearer <SA JWT>                     │ validate subject token,
    X-Subject-Token: Bearer kbc_at_*                                │ project scope, decrypt
    { "projectId": 123 }                                   ◄────────┘ legacy Storage token
                                                                    │
  StorageApiTokenFactory::createFromResolvedToken:                  │
    clone request, remove Authorization,                            ▼
    set X-StorageApi-Token = <legacy token>                  Storage API
    verifyToken() ─────────────────────────────────────────►        │ tokenInfo
                                                            ◄────────┘
  => new StorageApiToken(tokenInfo, legacyToken)  →  #[CurrentUser] StorageApiToken
```

Why the second `verifyToken()` call: the resolver returns only minimal metadata, not the full
token info (features, owner, backend, ...) that `StorageApiToken` carries and that downstream
code relies on. Re-verifying the resolved legacy token yields exactly the object the legacy
flow produces. There is no result caching in v1 (see [Caching](#caching)).

## Integration

The shared core (resolver + `StorageApiToken` construction) is exposed in **two** ways.

### 1. Transparent mode (extends `#[StorageApiTokenAuth]`)

Enabled per service via bundle config. When on, `#[StorageApiTokenAuth]` controllers also
accept `kbc_at_*` / `kbc_pat_*` tokens — no controller changes.

```yaml
keboola_api:
  storage_token_exchange:
    enabled: true
```

```php
#[StorageApiTokenAuth]
class MyController
{
    public function __invoke(#[CurrentUser] StorageApiToken $token): Response
    {
        // Accepts X-StorageApi-Token (legacy) OR Authorization: Bearer kbc_at_/kbc_pat_
        // (+ X-KBC-ProjectId). Both end up as a StorageApiToken backed by a legacy token.
    }
}
```

A legacy token never triggers an exchange (only the `kbc_at_` / `kbc_pat_` prefixes do), so
existing legacy traffic is unaffected even when the switch is on.

### 2. Explicit mode (`#[ConnectionTokenAuth]`)

A dedicated attribute, independent of the `enabled` switch, for opt-in per controller. Often
declared next to `#[StorageApiTokenAuth]` so the endpoint accepts both token kinds.

```php
#[StorageApiTokenAuth]   // legacy X-StorageApi-Token
#[ConnectionTokenAuth]   // kbc_at_/kbc_pat_ -> exchanged to a legacy token
class MyController
{
    public function __invoke(#[CurrentUser] StorageApiToken $token): Response { /* ... */ }
}
```

Both attributes accept the same `features` argument with identical semantics.

## Project id

The new programmatic tokens are not project-scoped on their own, so the request must say which
project to resolve. It is taken from the `X-KBC-ProjectId` header (matching Connection's
`BearerTokenAuthenticator::PROJECT_ID_HEADER`). The header name is configurable. A programmatic
token without a valid project id is rejected with `400`.

## Configuration

```yaml
keboola_api:
  storage_token_exchange:
    enabled: false                                                   # transparent mode for #[StorageApiTokenAuth]
    service_account_token_path: '/var/run/secrets/connection.keboola.com/serviceaccount/token'
    project_id_header: 'X-KBC-ProjectId'
    connection_dns_type: 'internal'                                  # internal | public
```

- `enabled` only controls transparent mode. `#[ConnectionTokenAuth]` works regardless.
- The ServiceAccount token is read from the projected file on every resolver call so
  kubelet-rotated tokens are picked up automatically. A missing / empty / unreadable file
  fails fast when a programmatic token is presented.
- Internal resolver calls default to in-cluster (`internal`) DNS.

## Error mapping (resolver -> client)

| Resolver outcome | Response to the original client |
| --- | --- |
| `400` invalid / missing project id | `400` |
| `401` invalid subject token | `401` |
| `403` subject token cannot access project | `403` |
| `5xx` / timeout / network error | `502` / `504` |

**Known limitation.** Connection returns `401`/`403` both for subject-token problems (client
fault) and for our ServiceAccount identity problems (deployment misconfiguration). The bundle
cannot tell them apart from the bare status code, so it forwards `401`/`403` to the client. A
ServiceAccount misconfiguration therefore surfaces as a blanket `401`/`403` on every request
and is caught by a post-deploy smoke test. A richer Connection error contract would let us map
our-identity failures to `502` instead; tracked as a future improvement.

## Security

- Plaintext subject tokens and resolved legacy tokens are never logged and never placed in
  exception messages (`#[SensitiveParameter]` on token arguments, sanitized error messages).
- The ServiceAccount JWT is read per request (rotation) and validated as non-empty.
- Resolver calls use internal DNS by default.
- No result caching in v1, so a revoked subject token stops working immediately.

## Caching

Not implemented in v1. Each authenticated request performs one resolver call plus one
`verifyToken` call. The resolver is hidden behind an interface so a short-lived
(`(subjectTokenHash, projectId)`-keyed, 5-30 s TTL, invalidate-on-401/403) cache can be added
later without touching the authenticators, per the RFC guidance.

## Components

`Keboola\ApiBundle\AuthBridge` (the part intended to move to the Manage API client):

| Class | Responsibility |
| --- | --- |
| `StorageTokenResolverInterface` | `resolve(int $projectId, string $subjectToken): ResolvedStorageToken`. Stable seam. |
| `AuthBridgeStorageTokenResolver` | Guzzle implementation calling the Connection resolver endpoint. |
| `KubernetesServiceAccountTokenProvider` | Reads the projected SA token file per call. |
| `ResolvedStorageToken` | Immutable resolver response DTO. |
| `ProgrammaticToken` | `kbc_at_` / `kbc_pat_` prefix detection. |
| `Exception\*` | Typed resolver failures (`Unauthorized`, `ProjectAccessDenied`, `InvalidRequest`, `Unavailable`). |

`Keboola\ApiBundle\Security`:

| Class | Responsibility |
| --- | --- |
| `StorageApiToken\StorageApiTokenFactory` | Builds `StorageApiToken` from a request or from a resolved legacy token. |
| `StorageApiToken\StorageApiTokenAuthenticator` | Transparent mode: exchanges programmatic tokens when enabled. |
| `ConnectionToken\ConnectionTokenAuthenticator` | Explicit mode for `#[ConnectionTokenAuth]`. |
| `Attribute\ConnectionTokenAuth` | Controller attribute. |

## Infrastructure prerequisites

These live outside `api-bundle` but are required for the exchange to work:

1. **Connection k8s-auth mapping** — every consuming service's ServiceAccount subject must be
   mapped to the `internal:auth-bridge:resolve-storage-token` scope, per stack. Without it the
   resolver returns `403`.
2. **Projected ServiceAccount token** with audience `keboola-connection` mounted at the
   configured path (already present for services using `#[ApplicationTokenAuth]` via the SA
   header).
3. **Callers (UI / clients)** send `Authorization: Bearer kbc_at_/kbc_pat_` and
   `X-KBC-ProjectId` instead of a legacy Storage token.

## Follow-up

- Move the resolver into `keboola/kbc-manage-api-php-client` as
  `ManageApiClient::resolveStorageToken()` and wire that implementation behind
  `StorageTokenResolverInterface`; delete `AuthBridgeStorageTokenResolver` from `api-bundle`.
- Optionally add the short-lived resolver cache described above.

## Testing

`Keboola\ApiBundle\Test\AuthenticatorTestTrait::setupFakeConnectionToken()` stubs the resolver
(and the Storage client verification) so controllers guarded by `#[ConnectionTokenAuth]` or by
transparent `#[StorageApiTokenAuth]` can be exercised in `KernelTestCase` tests without
reaching Connection or Storage API.
