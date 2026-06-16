# Client Exception Model Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let consumers identify which Keboola service client failed (via per-client exception subclasses) and give the base `ClientException` structured HTTP context, without losing the clean stack trace.

**Architecture:** `Keboola\ApiClientBase\Exception\ClientException` gains `statusCode`/`responseBody` (+ getters). `ApiClient` gains one facade-supplied argument `exceptionClass` (a `class-string<ClientException>`, default `ClientException::class`), instantiated inline at each throw site (clean trace). Per-client subclasses (`VaultClientException extends ClientException`) are added in each client's own PR, not here.

**Tech Stack:** PHP 8.2, Guzzle 7, PHPUnit 9, PHPStan level max, keboola/coding-standard. Spec: `docs/superpowers/specs/2026-06-16-client-exception-model-design.md`.

> **Post-implementation note (refined during code review):** Task 2 below was written around a
> `class-string<ClientException>|Closure $exceptionFactory` routed through a private `makeException()`
> helper. That was changed during review: a helper/closure that does the `new` itself spoils the
> stack trace (origin moves into the helper + an extra frame), so the closure was dropped and the
> argument is a plain `class-string<ClientException> $exceptionClass` instantiated **inline** at each
> throw site. The Task 2 steps mentioning `makeException`/the closure are superseded by spec §2; the
> shipped code (PR #522) reflects the inline design.

---

## Scope

Base lib only (`libs/php-api-client-base`), branch `pepa/PAT-1866_unifException`. Two tasks, both fully BC (additive optional params). Produces a working, testable base lib whose default behaviour is unchanged.

## Prerequisites (run once, from the worktree root)

Worktree root: `/home/pepa/.local/share/wtm/worktrees/platform-libraries@pepa_PAT-1866_unifException`

All commands run the lib inside the prebuilt `keboola/php-dev82` image with the whole monorepo mounted (so the lib's path repositories resolve). Install deps once:

```bash
docker run --rm --network host -v "$PWD":/code -w /code/libs/php-api-client-base keboola/php-dev82 \
  composer install --no-interaction
```

- Run a single test: `docker run --rm --network host -v "$PWD":/code -w /code/libs/php-api-client-base keboola/php-dev82 vendor/bin/phpunit --filter <TestName>`
- Full gate: `docker run --rm --network host -v "$PWD":/code -w /code/libs/php-api-client-base keboola/php-dev82 composer ci`
- `--network host` is required for Packagist DNS; never pipe the command through `| tail`/`| grep` without capturing `${PIPESTATUS[0]}` (a pipe masks the real exit code).

## File Structure

- `libs/php-api-client-base/src/Exception/ClientException.php` — **modify**: add `statusCode`/`responseBody` constructor params + getters. (Task 1)
- `libs/php-api-client-base/tests/Exception/ClientExceptionTest.php` — **create**: unit-test the value object. (Task 1)
- `libs/php-api-client-base/src/ApiClient.php` — **modify**: add `exceptionFactory` arg + property + `makeException()` helper; route all 6 throw sites through it; capture body before JSON decode. (Task 2)
- `libs/php-api-client-base/tests/Fixtures/DummyClientException.php` — **create**: a `ClientException` subclass for tests. (Task 2)
- `libs/php-api-client-base/tests/ApiClientTest.php` — **modify**: add tests for the factory hook + context population + trace origin. (Task 2)

---

### Task 1: Enrich `ClientException`

**Files:**
- Modify: `libs/php-api-client-base/src/Exception/ClientException.php`
- Test: `libs/php-api-client-base/tests/Exception/ClientExceptionTest.php`

- [ ] **Step 1: Write the failing test**

Create `libs/php-api-client-base/tests/Exception/ClientExceptionTest.php`:

```php
<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests\Exception;

use Keboola\ApiClientBase\Exception\ClientException;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class ClientExceptionTest extends TestCase
{
    public function testDefaults(): void
    {
        $e = new ClientException('boom');

        self::assertInstanceOf(RuntimeException::class, $e);
        self::assertSame('boom', $e->getMessage());
        self::assertSame(0, $e->getCode());
        self::assertNull($e->getPrevious());
        self::assertNull($e->getStatusCode());
        self::assertNull($e->getResponseBody());
    }

    public function testCarriesContext(): void
    {
        $previous = new RuntimeException('prev');
        $e = new ClientException('boom', 404, $previous, 404, '{"error":"not found"}');

        self::assertSame('boom', $e->getMessage());
        self::assertSame(404, $e->getCode());
        self::assertSame($previous, $e->getPrevious());
        self::assertSame(404, $e->getStatusCode());
        self::assertSame('{"error":"not found"}', $e->getResponseBody());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `docker run --rm --network host -v "$PWD":/code -w /code/libs/php-api-client-base keboola/php-dev82 vendor/bin/phpunit --filter ClientExceptionTest`
Expected: FAIL — `Call to undefined method ...::getStatusCode()`.

- [ ] **Step 3: Implement the enriched exception**

Replace the entire contents of `libs/php-api-client-base/src/Exception/ClientException.php`:

```php
<?php

declare(strict_types=1);

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

    /**
     * HTTP status code of the failing response, or null when there was no response
     * (transport/connection/authenticator failure).
     */
    public function getStatusCode(): ?int
    {
        return $this->statusCode;
    }

    /**
     * Raw response body when available, otherwise null.
     */
    public function getResponseBody(): ?string
    {
        return $this->responseBody;
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `docker run --rm --network host -v "$PWD":/code -w /code/libs/php-api-client-base keboola/php-dev82 vendor/bin/phpunit --filter ClientExceptionTest`
Expected: PASS (2 tests).

- [ ] **Step 5: Lint + static analysis**

Run: `docker run --rm --network host -v "$PWD":/code -w /code/libs/php-api-client-base keboola/php-dev82 bash -c 'composer phpcs && composer phpstan'`
Expected: phpcs no violations; phpstan `[OK] No errors`.

- [ ] **Step 6: Commit**

```bash
git add libs/php-api-client-base/src/Exception/ClientException.php libs/php-api-client-base/tests/Exception/ClientExceptionTest.php
git commit -m "PAT-1866 Enrich ClientException with status code and response body"
```

---

### Task 2: `ApiClient` exception factory hook + context population

**Files:**
- Modify: `libs/php-api-client-base/src/ApiClient.php`
- Create: `libs/php-api-client-base/tests/Fixtures/DummyClientException.php`
- Test: `libs/php-api-client-base/tests/ApiClientTest.php`

- [ ] **Step 1: Create the test fixture exception**

Create `libs/php-api-client-base/tests/Fixtures/DummyClientException.php`:

```php
<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests\Fixtures;

use Keboola\ApiClientBase\Exception\ClientException;

final class DummyClientException extends ClientException
{
}
```

- [ ] **Step 2: Write the failing tests**

Add these imports to the top of `libs/php-api-client-base/tests/ApiClientTest.php` (alongside the existing `use` block):

```php
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Psr7\Request;
use Keboola\ApiClientBase\Tests\Fixtures\DummyClientException;
use Throwable;
```

(`GuzzleHttp\Psr7\Request`, `RequestInterface`, `Response`, `MockHandler`, `HandlerStack`, `NoAuthAuthenticator`, `ApiClient`, `ApiClientOptions`, `ClientException` are already imported — do not duplicate.)

Append these methods inside `class ApiClientTest`:

```php
    public function testThrowsConfiguredExceptionClass(): void
    {
        $mock = new MockHandler([new Response(400, [], '{"error":"bad"}')]);
        $client = new ApiClient(
            'https://example.test',
            new NoAuthAuthenticator(),
            new ApiClientOptions(requestHandler: HandlerStack::create($mock)),
            exceptionFactory: DummyClientException::class,
        );

        try {
            $client->sendRequest(new Request('GET', 'foo'));
            self::fail('Expected exception');
        } catch (ClientException $e) {
            self::assertInstanceOf(DummyClientException::class, $e);
        }
    }

    public function testUsesExceptionFactoryClosure(): void
    {
        $mock = new MockHandler([new Response(500, [], 'oops')]);
        $client = new ApiClient(
            'https://example.test',
            new NoAuthAuthenticator(),
            new ApiClientOptions(requestHandler: HandlerStack::create($mock)),
            exceptionFactory: static fn(
                string $message,
                int $code,
                ?Throwable $previous,
                ?int $statusCode,
                ?string $responseBody,
            ): ClientException => new DummyClientException(
                'factory: ' . $message,
                $code,
                $previous,
                $statusCode,
                $responseBody,
            ),
        );

        try {
            $client->sendRequest(new Request('GET', 'foo'));
            self::fail('Expected exception');
        } catch (ClientException $e) {
            self::assertInstanceOf(DummyClientException::class, $e);
            self::assertStringStartsWith('factory: ', $e->getMessage());
        }
    }

    public function testPopulatesStatusCodeAndResponseBodyOnHttpError(): void
    {
        $mock = new MockHandler([new Response(409, [], '{"error":"conflict"}')]);
        $client = new ApiClient(
            'https://example.test',
            new NoAuthAuthenticator(),
            new ApiClientOptions(requestHandler: HandlerStack::create($mock)),
        );

        try {
            $client->sendRequest(new Request('GET', 'foo'));
            self::fail('Expected exception');
        } catch (ClientException $e) {
            self::assertSame(409, $e->getStatusCode());
            self::assertSame('{"error":"conflict"}', $e->getResponseBody());
        }
    }

    public function testStatusCodeNullOnTransportError(): void
    {
        $mock = new MockHandler([
            new ConnectException('Connection refused', new Request('GET', 'foo')),
        ]);
        $client = new ApiClient(
            'https://example.test',
            new NoAuthAuthenticator(),
            new ApiClientOptions(backoffMaxTries: 0, requestHandler: HandlerStack::create($mock)),
        );

        try {
            $client->sendRequest(new Request('GET', 'foo'));
            self::fail('Expected exception');
        } catch (ClientException $e) {
            self::assertNull($e->getStatusCode());
            self::assertNull($e->getResponseBody());
        }
    }

    public function testDefaultExceptionOriginatesInApiClient(): void
    {
        // Guards the clean-trace contract: the default path must construct the exception
        // inside ApiClient, not in external factory code.
        $mock = new MockHandler([new Response(400, [], 'bad')]);
        $client = new ApiClient(
            'https://example.test',
            new NoAuthAuthenticator(),
            new ApiClientOptions(requestHandler: HandlerStack::create($mock)),
        );

        try {
            $client->sendRequest(new Request('GET', 'foo'));
            self::fail('Expected exception');
        } catch (ClientException $e) {
            self::assertStringContainsString('ApiClient.php', $e->getFile());
        }
    }
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `docker run --rm --network host -v "$PWD":/code -w /code/libs/php-api-client-base keboola/php-dev82 vendor/bin/phpunit --filter ApiClientTest`
Expected: FAIL — `Unknown named parameter $exceptionFactory` (and the status-code/body assertions fail).

- [ ] **Step 4: Add the `Closure` import to `ApiClient`**

In `libs/php-api-client-base/src/ApiClient.php`, add `use Closure;` to the import block (keep imports alphabetical — it goes before `use GuzzleHttp\Client as GuzzleClient;`):

```php
use Closure;
use GuzzleHttp\Client as GuzzleClient;
```

- [ ] **Step 5: Add the `exceptionFactory` property**

In `libs/php-api-client-base/src/ApiClient.php`, replace the two existing property declarations:

```php
    private readonly GuzzleClient $httpClient;
    private readonly ErrorMessageResolverInterface $errorMessageResolver;
```

with:

```php
    private readonly GuzzleClient $httpClient;
    private readonly ErrorMessageResolverInterface $errorMessageResolver;

    /** @var class-string<ClientException>|Closure(string, int, ?Throwable, ?int, ?string): ClientException */
    private readonly Closure|string $exceptionFactory;
```

- [ ] **Step 6: Add the constructor argument and assign it**

In `libs/php-api-client-base/src/ApiClient.php`, replace the constructor signature + docblock:

```php
    /**
     * @param non-empty-string|null $baseUrl
     * @param list<int> $retryableStatusCodes Non-5xx status codes to also retry (e.g. [429]).
     */
    public function __construct(
        ?string $baseUrl,
        RequestAuthenticatorInterface $authenticator,
        ?ApiClientOptions $options = null,
        ?ErrorMessageResolverInterface $errorMessageResolver = null,
        array $retryableStatusCodes = [],
    ) {
        $options ??= new ApiClientOptions();
        $this->errorMessageResolver = $errorMessageResolver ?? new DefaultErrorMessageResolver();
        $logger = $options->logger ?? new NullLogger();
```

with:

```php
    /**
     * @param non-empty-string|null $baseUrl
     * @param list<int> $retryableStatusCodes Non-5xx status codes to also retry (e.g. [429]).
     * @param class-string<ClientException>|Closure(string, int, ?Throwable, ?int, ?string): ClientException $exceptionFactory
     *   Either a ClientException subclass name (instantiated inline → clean trace; default) or a
     *   closure that builds the exception (opt-in; adds a factory frame to the trace).
     */
    public function __construct(
        ?string $baseUrl,
        RequestAuthenticatorInterface $authenticator,
        ?ApiClientOptions $options = null,
        ?ErrorMessageResolverInterface $errorMessageResolver = null,
        array $retryableStatusCodes = [],
        string|Closure $exceptionFactory = ClientException::class,
    ) {
        $options ??= new ApiClientOptions();
        $this->errorMessageResolver = $errorMessageResolver ?? new DefaultErrorMessageResolver();
        $this->exceptionFactory = $exceptionFactory;
        $logger = $options->logger ?? new NullLogger();
```

- [ ] **Step 7: Capture the body before decode and route JSON/mapping throws through `makeException`**

In `libs/php-api-client-base/src/ApiClient.php`, replace the body of `sendRequestAndMapResponse` from the `$response = $this->doSendRequest(...)` line through the second `catch`:

```php
        $response = $this->doSendRequest($request, $options);

        try {
            $data = Json::decodeArray($response->getBody()->getContents());
        } catch (JsonException $e) {
            throw new ClientException('Response is not valid JSON: ' . $e->getMessage(), 0, $e);
        }

        try {
            if ($isList) {
                /** @var list<array<string, mixed>> $data */
                return array_values(array_map(
                    static fn(array $item): mixed => $responseClass::fromResponseData($item),
                    $data,
                ));
            }
            return $responseClass::fromResponseData($data);
        } catch (Throwable $e) {
            throw new ClientException('Failed to map response data: ' . $e->getMessage(), 0, $e);
        }
```

with:

```php
        $response = $this->doSendRequest($request, $options);
        $body = $response->getBody()->getContents();

        try {
            $data = Json::decodeArray($body);
        } catch (JsonException $e) {
            throw $this->makeException(
                'Response is not valid JSON: ' . $e->getMessage(),
                0,
                $e,
                $response->getStatusCode(),
                $body,
            );
        }

        try {
            if ($isList) {
                /** @var list<array<string, mixed>> $data */
                return array_values(array_map(
                    static fn(array $item): mixed => $responseClass::fromResponseData($item),
                    $data,
                ));
            }
            return $responseClass::fromResponseData($data);
        } catch (Throwable $e) {
            throw $this->makeException(
                'Failed to map response data: ' . $e->getMessage(),
                0,
                $e,
                $response->getStatusCode(),
                $body,
            );
        }
```

- [ ] **Step 8: Route the transport throws through `makeException`**

In `libs/php-api-client-base/src/ApiClient.php`, replace the `doSendRequest` catch block:

```php
        try {
            return $this->httpClient->send($request, $options);
        } catch (RequestException $e) {
            throw $this->processRequestException($e);
        } catch (GuzzleException $e) {
            throw new ClientException($e->getMessage(), 0, $e);
        } catch (Throwable $e) {
            // Non-Guzzle failure bubbling out of the handler stack — e.g. an authenticator
            // that could not produce credentials (after retries are exhausted).
            throw new ClientException(trim($e->getMessage()), 0, $e);
        }
```

with:

```php
        try {
            return $this->httpClient->send($request, $options);
        } catch (RequestException $e) {
            throw $this->processRequestException($e);
        } catch (GuzzleException $e) {
            throw $this->makeException($e->getMessage(), 0, $e, null, null);
        } catch (Throwable $e) {
            // Non-Guzzle failure bubbling out of the handler stack — e.g. an authenticator
            // that could not produce credentials (after retries are exhausted).
            throw $this->makeException(trim($e->getMessage()), 0, $e, null, null);
        }
```

- [ ] **Step 9: Route `processRequestException` through `makeException` and add the helper**

In `libs/php-api-client-base/src/ApiClient.php`, replace the entire `processRequestException` method:

```php
    private function processRequestException(RequestException $e): ClientException
    {
        $response = $e->getResponse();
        if ($response === null) {
            return new ClientException(trim($e->getMessage()), 0, $e);
        }

        $statusCode = $response->getStatusCode();
        $body = (string) $response->getBody();

        $message = ($this->errorMessageResolver)($body, $statusCode);
        return new ClientException($message ?? trim($e->getMessage()), $statusCode, $e);
    }
```

with:

```php
    private function processRequestException(RequestException $e): ClientException
    {
        $response = $e->getResponse();
        if ($response === null) {
            return $this->makeException(trim($e->getMessage()), 0, $e, null, null);
        }

        $statusCode = $response->getStatusCode();
        $body = (string) $response->getBody();

        $message = ($this->errorMessageResolver)($body, $statusCode);
        return $this->makeException($message ?? trim($e->getMessage()), $statusCode, $e, $statusCode, $body);
    }

    private function makeException(
        string $message,
        int $code,
        ?Throwable $previous,
        ?int $statusCode,
        ?string $responseBody,
    ): ClientException {
        $factory = $this->exceptionFactory;

        // is_string → class-string<ClientException>: instantiate inline so `new` happens here,
        // keeping the trace/origin in ApiClient. A closure is the caller's opt-in (extra frame).
        return is_string($factory)
            ? new $factory($message, $code, $previous, $statusCode, $responseBody)
            : $factory($message, $code, $previous, $statusCode, $responseBody);
    }
```

- [ ] **Step 10: Run tests to verify they pass**

Run: `docker run --rm --network host -v "$PWD":/code -w /code/libs/php-api-client-base keboola/php-dev82 vendor/bin/phpunit --filter ApiClientTest`
Expected: PASS — all existing tests plus the 5 new ones green.

- [ ] **Step 11: Full gate (validate + phpcs + phpstan + all tests)**

Run: `docker run --rm --network host -v "$PWD":/code -w /code/libs/php-api-client-base keboola/php-dev82 composer ci`
Expected: `./composer.json is valid`, phpcs no violations, phpstan `[OK] No errors`, PHPUnit `OK`.

- [ ] **Step 12: Commit**

```bash
git add libs/php-api-client-base/src/ApiClient.php \
        libs/php-api-client-base/tests/Fixtures/DummyClientException.php \
        libs/php-api-client-base/tests/ApiClientTest.php
git commit -m "PAT-1866 Add facade-supplied exception factory + HTTP context to ApiClient"
```

---

## Self-Review

**Spec coverage:**
- §1 identification (per-client subclass of concrete base) → base supports it via `exceptionFactory` class-string (Task 2); the subclasses themselves land in each client's PR (out of scope here, noted in spec).
- §2 throw mechanism (class-string default + closure opt-in, clean trace) → Task 2 Steps 5–9 + `testThrowsConfiguredExceptionClass`/`testUsesExceptionFactoryClosure`/`testDefaultExceptionOriginatesInApiClient`.
- §3 enrichment (`statusCode`/`responseBody`, `code` kept) → Task 1 + per-site population in Task 2 Steps 7–9 + `testPopulatesStatusCodeAndResponseBodyOnHttpError`/`testStatusCodeNullOnTransportError`.
- §3 per-site context table → Steps 7 (JSON/mapping: status + body), 8 (transport: null/null), 9 (HTTP error: status + body; no-response: null/null).
- Back-compat → all new params optional; existing `ApiClientTest` cases remain unchanged and must stay green (Step 10/11).

**Placeholder scan:** none — every code/command step is complete.

**Type consistency:** `exceptionFactory` is `class-string<ClientException>|Closure` in the property (Step 5), constructor (Step 6), and `makeException` consumes it (Step 9); the closure signature `(string, int, ?Throwable, ?int, ?string): ClientException` matches `makeException`'s call and the `ClientException` constructor from Task 1. `getStatusCode()`/`getResponseBody()` names match between Task 1 (definition) and Task 2 (assertions).
