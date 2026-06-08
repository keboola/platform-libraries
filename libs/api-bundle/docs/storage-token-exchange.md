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

The exchange is **transparent and always on**: `#[StorageApiTokenAuth]` accepts both legacy
`X-StorageApi-Token` and programmatic `Authorization: Bearer kbc_at_/kbc_pat_` tokens. There is no
opt-in attribute and no configuration switch — a legacy token never triggers an exchange (only the
`kbc_at_` / `kbc_pat_` prefixes do), so existing legacy traffic is unaffected.

> **Resolver location.** The resolver HTTP call lives in the Manage API client
> (`keboola/kbc-manage-api-php-client` `^v10.2`) as `Client::resolveStorageToken()`, because the
> endpoint is a `/manage/...` route authenticated by the service's Kubernetes ServiceAccount JWT.
> The Manage API client is a direct dependency of the bundle, so the exchange is always available;
> `StorageApiTokenAuthenticator` calls it directly and maps its `ClientException` status codes to
> authentication errors.

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

Only `storageToken` is consumed by the bundle (the rest of the metadata is informational); the
resolved legacy token is re-verified against Storage API to build the full `StorageApiToken`.

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
  StorageApiTokenAuthenticator → Client::resolveStorageToken():     │
    Manage API client reads SA JWT from                             │
    /var/run/secrets/.../token (re-read per call)                   │
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

Why the second `verifyToken()` call: the resolver returns only the legacy token plus minimal
metadata, not the full token info (features, owner, backend, ...) that `StorageApiToken` carries
and that downstream code relies on. Re-verifying the resolved legacy token yields exactly the
object the legacy flow produces. There is no result caching in v1 (see [Caching](#caching)).

## Integration

`#[StorageApiTokenAuth]` is all that is needed — the exchange is transparent and always available.

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

## Project id

The new programmatic tokens are not project-scoped on their own, so the request must say which
project to resolve. It is taken from the `X-KBC-ProjectId` header (matching Connection's
`BearerTokenAuthenticator::PROJECT_ID_HEADER`). A programmatic token without a valid project id is
rejected with `400`.

## Configuration

None. The feature is always on and uses fixed conventions:

- ServiceAccount token path: `/var/run/secrets/connection.keboola.com/serviceaccount/token`. It is
  read from the projected file on every resolver call so kubelet-rotated tokens are picked up
  automatically; a missing / empty / unreadable file surfaces as `502` when a programmatic token is
  presented.
- Project id header: `X-KBC-ProjectId` (a platform-wide constant, not per-service tunable).
- Resolver DNS: internal in-cluster (`ServiceDnsType::INTERNAL`).

## Error mapping (resolver -> client)

| Resolver outcome | Response to the original client |
| --- | --- |
| `400` invalid / missing project id | `400` |
| `401` invalid subject token | `401` |
| `403` subject token cannot access project | `403` |
| `5xx` / timeout / network / SA token file error / unexpected | `502` |

**Known limitation.** Connection returns `401`/`403` both for subject-token problems (client
fault) and for our ServiceAccount identity problems (deployment misconfiguration). The bundle
cannot tell them apart from the bare status code, so it forwards `401`/`403` to the client. A
ServiceAccount misconfiguration therefore surfaces as a blanket `401`/`403` on every request
and is caught by a post-deploy smoke test. A richer Connection error contract would let us map
our-identity failures to `502` instead; tracked as a future improvement.

## Security

- Plaintext subject tokens and resolved legacy tokens are never logged and never placed in
  exception messages (`#[SensitiveParameter]` on token arguments, fixed error messages that never
  echo the Connection/Manage response body).
- The ServiceAccount JWT is read per request (rotation) and validated as non-empty.
- Resolver calls use internal DNS.
- No result caching in v1, so a revoked subject token stops working immediately.

## Caching

Not implemented in v1. Each authenticated programmatic request performs one resolver call plus one
`verifyToken` call. A short-lived (`(subjectTokenHash, projectId)`-keyed, 5-30 s TTL,
invalidate-on-401/403) cache could be added later, per the RFC guidance.

## Components

`Keboola\ApiBundle\Security\StorageApiToken`:

| Class | Responsibility |
| --- | --- |
| `ProgrammaticToken` | `kbc_at_` / `kbc_pat_` prefix detection. |
| `StorageApiTokenFactory` | Builds `StorageApiToken` from a request or from a resolved legacy token. |
| `StorageApiTokenAuthenticator` | Verifies legacy tokens; for programmatic tokens, calls `Client::resolveStorageToken()`, maps resolver errors, then verifies the resolved legacy token. |

The resolver client is a `Keboola\ManageApi\Client` registered under
`KeboolaApiExtension::STORAGE_TOKEN_RESOLVER_CLIENT_ID`, built via
`ManageApiClientFactory::getClientForServiceAccountTokenPath()` with the fixed SA token path and
internal DNS. The projected ServiceAccount JWT is read by the Manage API client's
`KubernetesServiceAccountTokenAuthenticationStrategy`, re-read on every request so kubelet-rotated
tokens are picked up.

## Infrastructure prerequisites

These live outside `api-bundle` but are required for the exchange to work:

1. **Connection k8s-auth mapping** — every consuming service's ServiceAccount subject must be
   mapped to the `internal:auth-bridge:resolve-storage-token` scope, per stack. Without it the
   resolver returns `403`.
2. **Projected ServiceAccount token** with audience `keboola-connection` mounted at the
   fixed path (already present for services using `#[ApplicationTokenAuth]` via the SA header).
3. **Callers (UI / clients)** send `Authorization: Bearer kbc_at_/kbc_pat_` and
   `X-KBC-ProjectId` instead of a legacy Storage token.

## Testing

`Keboola\ApiBundle\Test\AuthenticatorTestTrait::setupFakeConnectionToken()` stubs the resolver
client (and the Storage client verification) so controllers guarded by `#[StorageApiTokenAuth]`
can be exercised with a programmatic token in `KernelTestCase` tests without reaching Connection or
Storage API.
