# php-api-client-base — Client exception model

**Status:** Design approved (brainstorming), pending implementation plan.
**Date:** 2026-06-16
**Component:** `keboola/php-api-client-base` (`Keboola\ApiClientBase`) + its service-client consumers.

## Problem

Before the shared base lib, every Keboola service client shipped its own base exception
in its own namespace — `Keboola\VaultApiClient\Exception\ClientException`,
`Keboola\GitServiceApiClient\Exception\ClientException`, billing's `BillingException`, etc.
All were bare `RuntimeException` subclasses, but the **class/namespace alone identified which
client failed**: `catch (Keboola\VaultApiClient\Exception\ClientException $e)` meant "vault failed".

After migrating onto `php-api-client-base`, all clients throw the single base
`Keboola\ApiClientBase\Exception\ClientException`. A consumer that calls several Keboola
service clients can no longer tell *which* client failed from the caught type, and the base
exception carries no structured context (only `message` / `code` / `previous`).

## Goals

1. Restore **type-based identification** — `catch (VaultClientException)` for one client,
   `catch (ClientException)` for "any Keboola service client".
2. Keep the **single shared throw site** (`ApiClient`) and shared error handling.
3. Preserve a **clean exception trace** by default (origin at the real error site, no extra frames).
4. **Enrich** the base exception with structured HTTP context so consumers can react programmatically.
5. Fully **back-compatible / additive** — a client that does nothing keeps today's behaviour.

## Non-goals

- No marker-interface layer (see "Rejected alternatives").
- No request (method/URI) field on the exception for now (YAGNI; usually present in the previous exception's message).
- Billing's adoption — billing is a separate repo, handed off; it may adopt this independently later.

## Design

### 1. Identification — per-client subclass of a concrete base

The base lib keeps a concrete `Keboola\ApiClientBase\Exception\ClientException`. Each service
client ships a bare subclass, e.g.:

```php
namespace Keboola\VaultApiClient\Exception;

use Keboola\ApiClientBase\Exception\ClientException;

final class VaultClientException extends ClientException
{
}
```

Consumers:

```php
try {
    $vault->getVariable($id);
} catch (VaultClientException $e) {          // vault only
} catch (ClientException $e) {                // any Keboola service client
}
```

A concrete base class (not a marker interface) is used so every client exception inherits the
enriched context (see §3) for free, and so the base `ApiClient` can instantiate the client's
type directly via a `class-string` (see §2) without a factory.

### 2. Throw mechanism — `class-string`, instantiated inline

`ApiClient` gains one facade-mandated constructor argument (appended last, so existing positional
calls are unaffected):

```php
public function __construct(
    ?string $baseUrl,
    RequestAuthenticatorInterface $authenticator,
    ?ApiClientOptions $options = null,
    ?ErrorMessageResolverInterface $errorMessageResolver = null,
    array $retryableStatusCodes = [],
    string $exceptionClass = ClientException::class,   // class-string<ClientException>
) { ... }
```

Each throw site instantiates the configured class **inline** (mirroring the original code, which
threw `new ClientException(...)` directly at four sites and via `processRequestException()` for the
request-error path — only the literal class changes):

```php
throw new $this->exceptionClass($message, $code, $previous, $statusCode, $responseBody);
```

- **Default** (`ClientException::class`) → same thrown type and trace as today.
- A facade passes its own `VaultClientException::class` → `catch (VaultClientException)` works.
- Customization (e.g. response-aware construction, extra fields) is done in the **subclass
  constructor**, which receives `($message, $code, $previous, $statusCode, $responseBody)`. Because
  the `new` is inline at the throw site, a custom constructor does **not** add a trace frame.

**No factory, no shared builder.** The exception is deliberately NOT produced by a shared
`makeException()` helper, a closure, or a static named constructor. All of those do the `new`
inside themselves, which moves the exception's origin into that method and inserts it as the top
trace frame.

**Trace rationale (replicated):** PHP captures an exception's trace and `getFile()`/`getLine()` at
the `new`, not at the `throw`. Replication confirmed: an inline `throw new $cls(...)` — and an
inline `new` of a subclass *with a custom constructor* — reports the real throw site as the origin,
whereas a helper/factory method (`$x->make(...)`), a closure, or `ClientException::fromResponse(...)`
reports the helper's own file/line and inserts the helper as the top trace frame. That corrupts
"where did this happen" in logs and breaks error-tracker grouping (Sentry/Datadog group by the top
frame). Hence: instantiate inline only.

A `Closure` factory was considered as an opt-in escape hatch but dropped (YAGNI): the only thing it
adds over a subclass is *dynamic exception-type selection by status* (e.g. 404 → `NotFoundException`),
which no client needs today, and it cannot offer a clean trace. It can be re-introduced later if a
real need appears.

| Approach | `getFile():getLine()` | top trace frame |
| --- | --- | --- |
| inline `new $this->exceptionClass(...)` at the throw site | real throw site | real call chain |
| inline `new` of a subclass with a custom constructor | real throw site | real call chain |
| shared helper / closure / static named constructor | the helper's internals | the helper frame |

### 3. Enrichment — structured HTTP context on the base exception

```php
namespace Keboola\ApiClientBase\Exception;

use RuntimeException;
use Throwable;

class ClientException extends RuntimeException
{
    public function __construct(
        string $message = '',
        int $code = 0,
        ?Throwable $previous = null,
        private readonly ?int $statusCode = null,
        private readonly ?string $responseBody = null,
    ) {
        parent::__construct($message, $code, $previous);
    }

    public function getStatusCode(): ?int
    {
        return $this->statusCode;
    }

    public function getResponseBody(): ?string
    {
        return $this->responseBody;
    }
}
```

- Subclasses stay bare (`final class VaultClientException extends ClientException {}`) and inherit the constructor + accessors.
- `getStatusCode()` is `null` when there is no HTTP response (transport / connection / auth failures), unambiguously distinguishing "no response" from a genuine status `0`.
- `code` keeps its current value for back-compat (HTTP status for response errors, `0` otherwise) — e.g. billing reads `getCode()`.
- `responseBody` is stored as-is (no length cap / redaction for now); bodies here are typically small JSON error payloads.

Context populated per throw site in `ApiClient`:

| Throw site | `statusCode` | `responseBody` | `code` |
| --- | --- | --- | --- |
| HTTP error with response (`processRequestException`) | response status | response body | response status |
| JSON-decode / response-mapping failure (2xx) | response status | raw body | `0` |
| Transport / connection / authenticator failure (no response) | `null` | `null` | `0` |

### 4. Facade adoption pattern

Each service facade ships its `*ClientException` and passes the class to `ApiClient`:

```php
$this->apiClient = new ApiClient(
    $baseUrl,
    $authenticator,
    $options,
    errorMessageResolver: new VaultErrorMessageResolver(),
    retryableStatusCodes: [429],
    exceptionClass: VaultClientException::class,
);
```

## Affected components

- **Base lib** — `Exception/ClientException.php` (enrich), `ApiClient.php` (add `exceptionClass`
  arg; instantiate it inline at each throw site). New minor version (additive).
- **Each service client** — add `Exception/<Service>ClientException.php` and pass its class.
  The three migrated clients (`vault` #514, `sandboxes-service` #515, `git-service` #516) adopt it
  in their open PRs; `sync-actions` and `azure` adopt when migrated.

## Back-compat & versioning

All changes are additive: new optional constructor parameters (with defaults) on `ApiClient` and
`ClientException`, plus new accessors. A consumer or client that changes nothing keeps today's
behaviour and the plain `ClientException`. Base lib → minor bump (e.g. `1.1.0`). Each client that
adds its subclass → its own minor bump.

## Testing

- Base lib: default still throws `ClientException`; a passed `class-string` is instantiated and
  thrown (assert `instanceof` the subclass); `getStatusCode()` / `getResponseBody()` populated for
  HTTP errors, for JSON-decode and response-mapping failures (2xx), and `null` for transport errors;
  the default exception's origin stays inside `ApiClient` (`getFile()`).
- Each client: a failing request throws the client's `*ClientException` (which is also a `ClientException`).

## Rejected alternatives

- **Marker interface + per-client classes** — equivalent catch ergonomics, but no shared
  implementation (each client re-implements context accessors) and the base lib would need a
  factory to throw the client type (trace cost). The single-inheritance freedom it buys is moot —
  all clients are greenfield bare `RuntimeException`s.
- **Single base exception + service-name data field** — no type-based catch; consumers would have
  to catch one type and branch on a string.
- **Factory / closure / shared `makeException()` builder** — any method that does the `new`
  (helper, closure, or static named constructor) moves the exception's origin into itself and adds a
  trace frame (see §2, replicated). Dropped entirely; the base instantiates the class-string inline.
  A closure could be re-added later *only* if dynamic exception-type selection by status is needed.
