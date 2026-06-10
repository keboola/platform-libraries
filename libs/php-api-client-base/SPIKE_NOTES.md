# Spike: Symfony HttpClient instead of Guzzle

Throwaway exploration on branch `pepa/symfony-httpclient-spike`. Goal: reimplement
`keboola/php-api-client-base` on `symfony/http-client` (replacing `guzzlehttp/guzzle`),
port `keboola/git-service-api-client` onto it, and decide whether adopting Symfony
HttpClient is worth it. **Not for merge.**

Result: `composer ci` is green for both libs (phpstan level max, phpcs, phpunit). The
exact Guzzle-era API was intentionally not preserved where a cleaner Symfony idiom existed.

Installed: `symfony/http-client` v7.4 + `symfony/http-client-contracts` v3.7.

---

## What got simpler / what we deleted

### Retry — deleted `RetryDecider` (67 LOC) → `RetryStrategyFactory` (37 LOC)
The hand-rolled `RetryDecider` (count tracking, 4xx-vs-5xx branching, logging, the
"explicit codes win over the 4xx no-retry rule" logic) is replaced by Symfony's
`RetryableHttpClient` + `GenericRetryStrategy`. We keep a tiny factory only to encode
*policy*: retry transport errors (status-code key `0`), every 5xx, and any service-declared
extra codes (e.g. `429`). Backoff, jitter, max-delay, attempt counting and retry logging
are all provided by Symfony — we deleted all of it.

One real behavior nuance (see "what changed" below): Symfony's *default* strategy only
retries 5xx for idempotent methods. We deliberately pass a **flat** status-code list so 5xx
is retried for **all** methods, matching the old Guzzle behavior (git-service retries 5xx on
POST creates).

### JSON — deleted `Json.php` (32 LOC) and `JsonTest.php`
- Request bodies: pass `['json' => [...]]` and Symfony serializes it and sets
  `Content-Type: application/json` automatically. No manual `json_encode` + headers.
- Response decoding: `$response->toArray()` decodes JSON and throws a typed
  `DecodingExceptionInterface` on malformed bodies. No `Json::decodeArray` wrapper.
- `DefaultErrorMessageResolver` / `GitServiceErrorMessageResolver` now `json_decode` the
  body directly (they only ever needed body + status), dropping the `Json` dependency.

### Logging — deleted the Guzzle `Middleware::log` + `MessageFormatter` wiring
`RetryableHttpClient` takes the PSR-3 logger and logs retry attempts itself. (Full
request/response logging à la Guzzle's `MessageFormatter` is not built in; in a real
Symfony app you'd use the profiler/`TraceableHttpClient`, see DI section.)

### Mock testing — `MockHttpClient` + `MockResponse` is materially nicer
- No `HandlerStack::create($mock)` boilerplate. Inject a `MockHttpClient` straight into
  the test seam.
- Each `MockResponse` **records the request that consumed it** —
  `getRequestMethod()`, `getRequestUrl()` (resolved against `base_uri`), and
  `getRequestOptions()` (headers as `Name: value` lines, plus the serialized `body`). You
  assert on the exact response object, so it survives client decoration/cloning.
- `MockHttpClient::getRequestsCount()` gives a clean retry assertion (e.g. `[500, 200]`
  ⇒ count 2) — replacing the old "MockHandler queue is empty" indirection.

### Dependencies
Dropped `guzzlehttp/guzzle` **and** `psr/http-message` (no PSR-7 needed). Added
`symfony/http-client` + `symfony/http-client-contracts`. `psr/log` and
`webmozart/assert` unchanged.

### Net LOC
Base-lib `src/` went **486 → 472 LOC** with two files deleted and two smaller ones added.
The raw delta looks small, but that *understates* the win: ~100 lines of bespoke retry +
JSON + logging glue became thin configuration of battle-tested Symfony components. The
git-service facade `src/` shrank (`22 insertions / 37 deletions`) — mostly losing the
PSR-7 `Request` construction + `Json::encodeArray` per method.

---

## What got harder / what changed

### Auth contract: PSR-7 decorator → header map
`RequestAuthenticatorInterface` changed from
`__invoke(RequestInterface): RequestInterface` (mutate an immutable PSR-7 request) to
`getAuthenticationHeaders(): array<string,string>`. Symfony applies auth via the request
`headers` *option*, not by rewriting a request object, so a header map is the natural fit.
This is a **breaking contract change** for any existing custom authenticator (e.g. azure's
OAuth) — they'd need a mechanical rewrite. For the shipped authenticators it was trivial and
arguably clearer (no PSR-7 import, just return `[HEADER => value]`).

### New piece: the per-retry-auth decorator (`AuthenticatingHttpClient`, 41 LOC)
Guzzle re-ran auth on every attempt for free because `Middleware::mapRequest` sat *inside*
the retry middleware in the handler stack. Symfony has no equivalent "auth middleware", so
we add a small `HttpClientInterface` decorator that merges `getAuthenticationHeaders()` into
each request. Crucially it must be wrapped **inside** `RetryableHttpClient`
(`Retryable( Authenticating( inner ) )`) so that the retryable client re-invokes the auth
decorator on every attempt — this is what lets the projected-SA-token authenticator pick up
a kubelet-rotated token on retry. This wiring is a genuine "you have to know the layering"
gotcha; it's covered by a dedicated test (authenticator invoked twice across a `[500,200]`
sequence).

### Request API: method + path + options (not a PSR-7 `Request`)
`sendRequest` / `sendRequestAndMapResponse` now take `(string $method, string $path, array
$options = [])` instead of a `Psr\Http\Message\RequestInterface`. This is more idiomatic for
Symfony and removes a layer (no constructing `new Request(...)`), but it's an API break: the
`$options` array is now Symfony's request-options vocabulary (`json`, `query`, `headers`,
`body`, …). Facade methods became one-liners, e.g.
`sendRequestAndMapResponse('POST', 'repos', Repository::class, ['json' => ['name' => $name]])`.

### Response laziness & error semantics
Symfony responses are lazy: `getStatusCode()` never throws, but `getContent()` / `toArray()`
throw `HttpExceptionInterface` on 3xx–5xx **unless** you pass `false`. We lean into the
laziness rather than fight it: `sendRequest()` (void) awaits **only the status**
(`getStatusCode()`, which still drives `RetryableHttpClient`'s retries) and never buffers the
body of a successful request; `sendRequestAndMapResponse()` calls `toArray()` (await +
JSON-decode + throw). The response body is read **only on error**, to build the message via
the resolver — so a successful `DELETE` etc. never downloads a body. Error mapping:
`HttpExceptionInterface` → `ClientException` (status + `getContent(throw: false)` + resolver);
transport/decoding errors (no HTTP response) map with code `0`. The one caveat is the
laziness itself: nothing is sent/awaited until the response is consumed — correct here, but a
footgun if a future caller forgets to consume.

### Default headers (e.g. `User-Agent`) and the injected test seam
`User-Agent`/`base_uri`/timeouts are baked into `HttpClient::create([...])` when the client
builds its own transport. When a `MockHttpClient` is **injected** (the test seam), it is the
transport as-is and those defaults are *not* layered on (applying them via `withOptions()`
returns a clone, which would break the test's own handle to the mock's request count). So
the mock-based tests don't assert on the default `User-Agent`. With Guzzle this happened to
"just work" because the old seam injected only a handler, not the whole client. Minor, but a
real difference in what the seam covers.

### phpstan friction
Very little. `symfony/http-client` and `-contracts` ship solid generics/types, so
**phpstan level max passed with zero `@phpstan-ignore` in `src/`**. The only ignores are
two pre-existing test-only ones exercising `non-empty-string` runtime guards (unrelated to
the migration). No config was weakened.

---

## DI / Symfony-app integration angle

This is where Symfony HttpClient pulls clearly ahead for services that already run on
Symfony (most Keboola PHP services do):

- **HttpClient is a first-class service.** In a Symfony app you get an autowireable
  `HttpClientInterface`; our `ApiClientOptions::$httpClient` seam accepts it directly, so a
  service can hand the framework-configured client (with its TLS config, DNS cache, HTTP/2,
  connection pooling, proxy settings) to the API client instead of us calling
  `HttpClient::create()` ourselves.
- **Scoped clients.** `framework.http_client.scoped_clients` lets ops configure
  `base_uri`, default headers, timeouts and retry per upstream in `config/packages/*.yaml`,
  injected by name (`$gitServiceClient`). That externalizes config that's currently
  constructor args.
- **Observability for free.** Under the Symfony profiler the client is decorated with
  `TraceableHttpClient`; there's first-class Stopwatch + profiler-panel integration and
  PSR-3 logging, replacing Guzzle's `MessageFormatter` logging middleware.
- **Testing.** `MockHttpClient` is the framework-blessed test double; teams already know it,
  and it composes with the same decorator stack used in production.

For a **standalone library** consumed outside Symfony, the win is smaller — but the
dependency is light (http-client + contracts, no full framework) and the public surface is
unchanged for non-Symfony callers.

---

## Bottom line — is it worth switching?

**Cautiously yes, for new clients and Symfony-hosted services — but it is a breaking change,
so not a free in-place swap for the existing fleet.**

Why yes:
- We delete real, security-relevant glue (retry, backoff, JSON, logging) and lean on a
  maintained, well-typed component. phpstan level max stayed clean with no ignores.
- Tests got nicer (`MockHttpClient`/`MockResponse` request recording).
- For services already on Symfony, the DI / scoped-client / observability integration is a
  genuine step up over hand-instantiated Guzzle.

Why "cautiously":
- It is an **API + contract break**, not a drop-in: the auth interface
  (PSR-7 decorator → header map), the request API (PSR-7 `Request` → method/path/options),
  and the test seam (`requestHandler` → `HttpClientInterface`) all change. Every downstream
  client (`vault`, `sandboxes-service`, `sync-actions`, `azure` — note azure's custom OAuth
  authenticator) needs a mechanical but non-trivial port, with tests rewritten.
- You inherit Symfony's response-laziness and method-aware-retry semantics; we had to add
  explicit force-evaluation and a flat retry-code list to keep the old behavior. These are
  easy to get subtly wrong.
- The per-retry auth re-execution that Guzzle gave for free now requires a deliberate
  decorator + layering order.

**Recommendation:** adopt Symfony HttpClient for **net-new** Keboola service clients and as
the target when a client is next substantially reworked, and migrate the base lib + fleet in
a planned, batched effort (one client at a time, tests-green gate) rather than a big-bang
swap. The base lib itself is the right place to absorb the breaking changes once, so
downstream facades change only their request-construction calls and authenticators.
