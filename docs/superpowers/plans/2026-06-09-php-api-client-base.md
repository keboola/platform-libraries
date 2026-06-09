# php-api-client-base Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract the duplicated HTTP/auth/retry/JSON skeleton shared by the platform-libraries API clients into a new `keboola/php-api-client-base` library, then migrate all five clients onto it.

**Architecture:** A single base package provides `ApiClient` (Guzzle wrapper), `ApiClientConfiguration`, `RetryDecider`, `Json`, `ResponseModelInterface`, `Exception\ClientException`, an `Auth\RequestAuthenticatorInterface` (PSR-7 request-decorator contract), and three common authenticators. Each service client keeps only its domain facade(s) + `Model/` DTOs + service-specific auth, depending on the base via a composer path repository. Facades use composition (have-an `ApiClient`).

**Tech Stack:** PHP 8.2, Guzzle 7, PSR-3/PSR-7, Webmozart Assert, PHPUnit 9, PHPStan (level max), keboola/coding-standard. Monorepo: `platform-libraries`, sibling libs wired via `repositories: path` + `*@dev`, published via per-lib Azure DevOps split jobs.

**Spec:** `docs/superpowers/specs/2026-06-09-php-api-client-base-design.md`

---

## PR / branch structure

All work is in the `platform-libraries` monorepo. PRs are stacked:

| PR | Branch | Based on | Contents |
|----|--------|----------|----------|
| **PR 1** | `pepa/common-api-lib` (this worktree) | `main` | The base lib + spec + this plan + CI registration |
| **PR 2** | `pepa/common-api-lib-vault` | `pepa/common-api-lib` | Migrate `vault-api-client` |
| **PR 3** | `pepa/common-api-lib-sandboxes` | `pepa/common-api-lib` | Migrate `sandboxes-service-api-client` |
| **PR 4** | `pepa/common-api-lib-git-service` | `pepa/common-api-lib` | Migrate `git-service-api-client` |
| **PR 5** | `pepa/common-api-lib-sync-actions` | `pepa/common-api-lib` | Migrate `sync-actions-api-php-client` |
| **PR 6** | `pepa/common-api-lib-azure` | `pepa/common-api-lib` | Migrate `azure-api-client` |

Each client branch is created with `wtm fork <branch> --base pepa/common-api-lib`. While PR 1 is unmerged, the `*@dev` path dependency resolves against the local `libs/php-api-client-base/`, so client PRs build against it. After PR 1 merges to `main`, rebase each client branch onto `main`.

**Tasks 1–13 below fully specify PR 1.** PRs 2–6 are specified as migration playbooks (§ "PR 2–6"); each client's step-level TDD plan is authored at the start of its PR, after reading that client's facade/models/tests in full (the base must exist first, and sync-actions/azure need the structural investigation flagged in the spec).

---

## File structure (PR 1 — the base lib)

```
platform-libraries/libs/php-api-client-base/
  composer.json                              # keboola/php-api-client-base
  phpstan.neon
  phpunit.xml.dist
  azure-pipelines.tests.yml
  src/
    ApiClient.php                            # Guzzle wrapper, send + map + error normalization
    ApiClientConfiguration.php               # readonly config DTO
    RetryDecider.php                         # retry middleware decider (configurable codes)
    Json.php                                 # encodeArray()/decodeArray()
    ResponseModelInterface.php               # fromResponseData(array): static
    Exception/
      ClientException.php                    # base exception (extends RuntimeException)
    Auth/
      RequestAuthenticatorInterface.php      # __invoke(RequestInterface): RequestInterface
      StorageApiTokenAuthenticator.php       # X-StorageApi-Token
      ManageApiTokenAuthenticator.php        # X-KBC-ManageApiToken
      KeboolaServiceAccountAuthenticator.php # projected SA token file -> X-Kubernetes-Authorization
  tests/
    bootstrap.php
    JsonTest.php
    ResponseModelInterfaceTest.php
    Exception/ClientExceptionTest.php
    Auth/StorageApiTokenAuthenticatorTest.php
    Auth/ManageApiTokenAuthenticatorTest.php
    Auth/KeboolaServiceAccountAuthenticatorTest.php
    RetryDeciderTest.php
    ApiClientConfigurationTest.php
    ApiClientTest.php
    Fixtures/DummyModel.php                  # ResponseModelInterface fixture for ApiClient tests
```

---

## Task 1: Scaffold the package

**Files:**
- Create: `libs/php-api-client-base/composer.json`
- Create: `libs/php-api-client-base/phpstan.neon`
- Create: `libs/php-api-client-base/phpunit.xml.dist`
- Create: `libs/php-api-client-base/tests/bootstrap.php`
- Create: `libs/php-api-client-base/.gitignore`

- [ ] **Step 1: Create `composer.json`**

```json
{
    "name": "keboola/php-api-client-base",
    "type": "library",
    "license": "MIT",
    "description": "Shared base for Keboola service API clients (transport, auth, retry)",
    "authors": [
        {
            "name": "Keboola",
            "email": "devel@keboola.com"
        }
    ],
    "autoload": {
        "psr-4": {
            "Keboola\\ApiClientBase\\": "src/"
        }
    },
    "autoload-dev": {
        "psr-4": {
            "Keboola\\ApiClientBase\\Tests\\": "tests/"
        }
    },
    "require": {
        "php": "^8.2",
        "guzzlehttp/guzzle": "^7.8",
        "psr/http-message": "^1.0|^2.0",
        "psr/log": "^1.0|^2.0|^3.0",
        "webmozart/assert": "^1.11"
    },
    "require-dev": {
        "keboola/coding-standard": "^15.0",
        "phpstan/phpstan": "^1.10",
        "phpstan/phpstan-phpunit": "^1.3",
        "phpstan/phpstan-webmozart-assert": "^1.2",
        "phpunit/phpunit": "^9.6",
        "sempro/phpunit-pretty-print": "^1.4"
    },
    "config": {
        "lock": false,
        "sort-packages": true,
        "allow-plugins": {
            "dealerdirect/phpcodesniffer-composer-installer": true
        }
    },
    "scripts": {
        "ci": [
            "@composer validate --no-check-publish --no-check-all",
            "@phpcs",
            "@phpstan",
            "@phpunit"
        ],
        "phpcs": "phpcs -n --ignore=vendor,cache --extensions=php .",
        "phpcbf": "phpcbf --extensions=php src tests",
        "phpstan": "phpstan analyse --no-progress -c phpstan.neon",
        "phpunit": "phpunit"
    }
}
```

- [ ] **Step 2: Create `phpstan.neon`**

```neon
parameters:
    checkMissingIterableValueType: false
    level: max
    paths:
        - src
        - tests

includes:
    - vendor/phpstan/phpstan-phpunit/extension.neon
    - vendor/phpstan/phpstan-webmozart-assert/extension.neon
```

- [ ] **Step 3: Create `phpunit.xml.dist`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<phpunit xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:noNamespaceSchemaLocation="vendor/phpunit/phpunit/phpunit.xsd"
         bootstrap="tests/bootstrap.php"
         colors="true"
         cacheResultFile="/tmp/.phpunit.result.cache">
    <testsuites>
        <testsuite name="php-api-client-base">
            <directory>tests</directory>
        </testsuite>
    </testsuites>
    <coverage>
        <include>
            <directory suffix=".php">src</directory>
        </include>
    </coverage>
</phpunit>
```

- [ ] **Step 4: Create `tests/bootstrap.php`**

```php
<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';
```

- [ ] **Step 5: Create `.gitignore`**

```gitignore
/vendor/
/composer.lock
/.phpunit.result.cache
```

- [ ] **Step 6: Install dependencies and verify autoload**

Run: `cd libs/php-api-client-base && composer install`
Expected: installs cleanly; `composer validate --no-check-publish --no-check-all` passes.

- [ ] **Step 7: Commit**

```bash
git add libs/php-api-client-base/composer.json libs/php-api-client-base/phpstan.neon libs/php-api-client-base/phpunit.xml.dist libs/php-api-client-base/tests/bootstrap.php libs/php-api-client-base/.gitignore
git commit -m "feat(php-api-client-base): scaffold package"
```

---

## Task 2: `Json`

**Files:**
- Create: `libs/php-api-client-base/src/Json.php`
- Test: `libs/php-api-client-base/tests/JsonTest.php`

- [ ] **Step 1: Write the failing test**

```php
<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests;

use JsonException;
use Keboola\ApiClientBase\Json;
use PHPUnit\Framework\TestCase;

class JsonTest extends TestCase
{
    public function testEncodeArray(): void
    {
        self::assertSame('{"a":1}', Json::encodeArray(['a' => 1]));
    }

    public function testDecodeArray(): void
    {
        self::assertSame(['a' => 1], Json::decodeArray('{"a":1}'));
    }

    public function testDecodeInvalidJsonThrows(): void
    {
        $this->expectException(JsonException::class);
        Json::decodeArray('not-json');
    }

    public function testDecodeNonArrayThrows(): void
    {
        $this->expectException(JsonException::class);
        $this->expectExceptionMessage('Decoded data is int, array expected');
        Json::decodeArray('42');
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd libs/php-api-client-base && vendor/bin/phpunit tests/JsonTest.php`
Expected: FAIL — class `Keboola\ApiClientBase\Json` not found.

- [ ] **Step 3: Write the implementation**

```php
<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase;

use JsonException;

final class Json
{
    /**
     * @param array<mixed> $data
     */
    public static function encodeArray(array $data): string
    {
        return (string) json_encode($data, JSON_THROW_ON_ERROR);
    }

    /**
     * @return array<mixed>
     */
    public static function decodeArray(string $data): array
    {
        $result = json_decode($data, true, flags: JSON_THROW_ON_ERROR);
        if (!is_array($result)) {
            throw new JsonException(sprintf('Decoded data is %s, array expected', get_debug_type($result)));
        }

        return $result;
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd libs/php-api-client-base && vendor/bin/phpunit tests/JsonTest.php`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add libs/php-api-client-base/src/Json.php libs/php-api-client-base/tests/JsonTest.php
git commit -m "feat(php-api-client-base): add Json helper"
```

---

## Task 3: `ResponseModelInterface` + test fixture

**Files:**
- Create: `libs/php-api-client-base/src/ResponseModelInterface.php`
- Create: `libs/php-api-client-base/tests/Fixtures/DummyModel.php`
- Test: `libs/php-api-client-base/tests/ResponseModelInterfaceTest.php`

- [ ] **Step 1: Write the failing test**

```php
<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests;

use Keboola\ApiClientBase\ResponseModelInterface;
use Keboola\ApiClientBase\Tests\Fixtures\DummyModel;
use PHPUnit\Framework\TestCase;

class ResponseModelInterfaceTest extends TestCase
{
    public function testFixtureImplementsContract(): void
    {
        $model = DummyModel::fromResponseData(['name' => 'foo']);
        self::assertInstanceOf(ResponseModelInterface::class, $model);
        self::assertSame('foo', $model->name);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd libs/php-api-client-base && vendor/bin/phpunit tests/ResponseModelInterfaceTest.php`
Expected: FAIL — interface/fixture not found.

- [ ] **Step 3: Write the interface**

```php
<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase;

interface ResponseModelInterface
{
    /**
     * @param array<string, mixed> $data
     */
    public static function fromResponseData(array $data): static;
}
```

- [ ] **Step 4: Write the fixture**

```php
<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests\Fixtures;

use Keboola\ApiClientBase\ResponseModelInterface;

final class DummyModel implements ResponseModelInterface
{
    public function __construct(public readonly string $name)
    {
    }

    public static function fromResponseData(array $data): static
    {
        \assert(is_string($data['name']));
        return new self($data['name']);
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd libs/php-api-client-base && vendor/bin/phpunit tests/ResponseModelInterfaceTest.php`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add libs/php-api-client-base/src/ResponseModelInterface.php libs/php-api-client-base/tests/Fixtures/DummyModel.php libs/php-api-client-base/tests/ResponseModelInterfaceTest.php
git commit -m "feat(php-api-client-base): add ResponseModelInterface"
```

---

## Task 4: `Exception\ClientException`

**Files:**
- Create: `libs/php-api-client-base/src/Exception/ClientException.php`
- Test: `libs/php-api-client-base/tests/Exception/ClientExceptionTest.php`

- [ ] **Step 1: Write the failing test**

```php
<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests\Exception;

use Keboola\ApiClientBase\Exception\ClientException;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class ClientExceptionTest extends TestCase
{
    public function testIsRuntimeException(): void
    {
        $e = new ClientException('boom', 500);
        self::assertInstanceOf(RuntimeException::class, $e);
        self::assertSame('boom', $e->getMessage());
        self::assertSame(500, $e->getCode());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd libs/php-api-client-base && vendor/bin/phpunit tests/Exception/ClientExceptionTest.php`
Expected: FAIL — class not found.

- [ ] **Step 3: Write the implementation**

```php
<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Exception;

use RuntimeException;

class ClientException extends RuntimeException
{
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd libs/php-api-client-base && vendor/bin/phpunit tests/Exception/ClientExceptionTest.php`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/php-api-client-base/src/Exception/ClientException.php libs/php-api-client-base/tests/Exception/ClientExceptionTest.php
git commit -m "feat(php-api-client-base): add ClientException"
```

---

## Task 5: `Auth\RequestAuthenticatorInterface`

**Files:**
- Create: `libs/php-api-client-base/src/Auth/RequestAuthenticatorInterface.php`

This is a pure interface (no behavior); it is exercised by the authenticator tests in Tasks 6–8, so it has no standalone test.

- [ ] **Step 1: Write the interface**

```php
<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Auth;

use Psr\Http\Message\RequestInterface;

interface RequestAuthenticatorInterface
{
    public function __invoke(RequestInterface $request): RequestInterface;
}
```

- [ ] **Step 2: Verify it parses**

Run: `cd libs/php-api-client-base && php -l src/Auth/RequestAuthenticatorInterface.php`
Expected: `No syntax errors detected`.

- [ ] **Step 3: Commit**

```bash
git add libs/php-api-client-base/src/Auth/RequestAuthenticatorInterface.php
git commit -m "feat(php-api-client-base): add RequestAuthenticatorInterface"
```

---

## Task 6: `Auth\StorageApiTokenAuthenticator`

**Files:**
- Create: `libs/php-api-client-base/src/Auth/StorageApiTokenAuthenticator.php`
- Test: `libs/php-api-client-base/tests/Auth/StorageApiTokenAuthenticatorTest.php`

- [ ] **Step 1: Write the failing test**

```php
<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests\Auth;

use GuzzleHttp\Psr7\Request;
use InvalidArgumentException;
use Keboola\ApiClientBase\Auth\StorageApiTokenAuthenticator;
use PHPUnit\Framework\TestCase;

class StorageApiTokenAuthenticatorTest extends TestCase
{
    public function testAddsStorageApiTokenHeader(): void
    {
        $authenticator = new StorageApiTokenAuthenticator('secret-token');
        $request = $authenticator(new Request('GET', 'https://example.test'));

        self::assertSame('secret-token', $request->getHeaderLine('X-StorageApi-Token'));
    }

    public function testRejectsEmptyToken(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Storage API token must not be empty');
        /** @phpstan-ignore-next-line argument.type — exercising the runtime guard */
        new StorageApiTokenAuthenticator('');
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd libs/php-api-client-base && vendor/bin/phpunit tests/Auth/StorageApiTokenAuthenticatorTest.php`
Expected: FAIL — class not found.

- [ ] **Step 3: Write the implementation**

```php
<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Auth;

use Psr\Http\Message\RequestInterface;
use SensitiveParameter;
use Webmozart\Assert\Assert;

final readonly class StorageApiTokenAuthenticator implements RequestAuthenticatorInterface
{
    public const HEADER = 'X-StorageApi-Token';

    /**
     * @param non-empty-string $token
     */
    public function __construct(
        #[SensitiveParameter]
        private string $token,
    ) {
        Assert::stringNotEmpty($token, 'Storage API token must not be empty');
    }

    public function __invoke(RequestInterface $request): RequestInterface
    {
        return $request->withHeader(self::HEADER, $this->token);
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd libs/php-api-client-base && vendor/bin/phpunit tests/Auth/StorageApiTokenAuthenticatorTest.php`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/php-api-client-base/src/Auth/StorageApiTokenAuthenticator.php libs/php-api-client-base/tests/Auth/StorageApiTokenAuthenticatorTest.php
git commit -m "feat(php-api-client-base): add StorageApiTokenAuthenticator"
```

---

## Task 7: `Auth\ManageApiTokenAuthenticator`

**Files:**
- Create: `libs/php-api-client-base/src/Auth/ManageApiTokenAuthenticator.php`
- Test: `libs/php-api-client-base/tests/Auth/ManageApiTokenAuthenticatorTest.php`

- [ ] **Step 1: Write the failing test**

```php
<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests\Auth;

use GuzzleHttp\Psr7\Request;
use InvalidArgumentException;
use Keboola\ApiClientBase\Auth\ManageApiTokenAuthenticator;
use PHPUnit\Framework\TestCase;

class ManageApiTokenAuthenticatorTest extends TestCase
{
    public function testAddsManageApiTokenHeader(): void
    {
        $authenticator = new ManageApiTokenAuthenticator('secret-token');
        $request = $authenticator(new Request('GET', 'https://example.test'));

        self::assertSame('secret-token', $request->getHeaderLine('X-KBC-ManageApiToken'));
    }

    public function testRejectsEmptyToken(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Manage API token must not be empty');
        /** @phpstan-ignore-next-line argument.type — exercising the runtime guard */
        new ManageApiTokenAuthenticator('');
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd libs/php-api-client-base && vendor/bin/phpunit tests/Auth/ManageApiTokenAuthenticatorTest.php`
Expected: FAIL — class not found.

- [ ] **Step 3: Write the implementation**

```php
<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Auth;

use Psr\Http\Message\RequestInterface;
use SensitiveParameter;
use Webmozart\Assert\Assert;

final readonly class ManageApiTokenAuthenticator implements RequestAuthenticatorInterface
{
    public const HEADER = 'X-KBC-ManageApiToken';

    /**
     * @param non-empty-string $token
     */
    public function __construct(
        #[SensitiveParameter]
        private string $token,
    ) {
        Assert::stringNotEmpty($token, 'Manage API token must not be empty');
    }

    public function __invoke(RequestInterface $request): RequestInterface
    {
        return $request->withHeader(self::HEADER, $this->token);
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd libs/php-api-client-base && vendor/bin/phpunit tests/Auth/ManageApiTokenAuthenticatorTest.php`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/php-api-client-base/src/Auth/ManageApiTokenAuthenticator.php libs/php-api-client-base/tests/Auth/ManageApiTokenAuthenticatorTest.php
git commit -m "feat(php-api-client-base): add ManageApiTokenAuthenticator"
```

---

## Task 8: `Auth\KeboolaServiceAccountAuthenticator`

**Files:**
- Create: `libs/php-api-client-base/src/Auth/KeboolaServiceAccountAuthenticator.php`
- Test: `libs/php-api-client-base/tests/Auth/KeboolaServiceAccountAuthenticatorTest.php`

- [ ] **Step 1: Write the failing test**

```php
<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests\Auth;

use GuzzleHttp\Psr7\Request;
use Keboola\ApiClientBase\Auth\KeboolaServiceAccountAuthenticator;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class KeboolaServiceAccountAuthenticatorTest extends TestCase
{
    public function testReadsTokenFileAndSetsBearerHeader(): void
    {
        $path = (string) tempnam(sys_get_temp_dir(), 'sa-token-');
        file_put_contents($path, "the-token\n");
        try {
            $authenticator = new KeboolaServiceAccountAuthenticator($path);
            $request = $authenticator(new Request('GET', 'https://example.test'));
            self::assertSame('Bearer the-token', $request->getHeaderLine('X-Kubernetes-Authorization'));
        } finally {
            @unlink($path);
        }
    }

    public function testRereadsTokenOnEachCall(): void
    {
        $path = (string) tempnam(sys_get_temp_dir(), 'sa-token-');
        file_put_contents($path, "first\n");
        try {
            $authenticator = new KeboolaServiceAccountAuthenticator($path);
            $first = $authenticator(new Request('GET', 'https://example.test'));
            self::assertSame('Bearer first', $first->getHeaderLine('X-Kubernetes-Authorization'));

            file_put_contents($path, "second\n");
            $second = $authenticator(new Request('GET', 'https://example.test'));
            self::assertSame('Bearer second', $second->getHeaderLine('X-Kubernetes-Authorization'));
        } finally {
            @unlink($path);
        }
    }

    public function testThrowsWhenFileMissing(): void
    {
        $authenticator = new KeboolaServiceAccountAuthenticator('/nonexistent/token');
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('/nonexistent/token');
        $authenticator(new Request('GET', 'https://example.test'));
    }

    public function testThrowsWhenFileEmpty(): void
    {
        $path = (string) tempnam(sys_get_temp_dir(), 'sa-token-');
        file_put_contents($path, "   \n");
        try {
            $authenticator = new KeboolaServiceAccountAuthenticator($path);
            $this->expectException(RuntimeException::class);
            $this->expectExceptionMessage('is empty');
            $authenticator(new Request('GET', 'https://example.test'));
        } finally {
            @unlink($path);
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd libs/php-api-client-base && vendor/bin/phpunit tests/Auth/KeboolaServiceAccountAuthenticatorTest.php`
Expected: FAIL — class not found.

- [ ] **Step 3: Write the implementation**

```php
<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Auth;

use Psr\Http\Message\RequestInterface;
use RuntimeException;
use Webmozart\Assert\Assert;

/**
 * Authenticates via a projected Kubernetes ServiceAccount token mounted by the
 * kbc-stacks chart at {@see self::DEFAULT_TOKEN_PATH}. The file is re-read on
 * every request so kubelet-rotated tokens are picked up automatically.
 */
final readonly class KeboolaServiceAccountAuthenticator implements RequestAuthenticatorInterface
{
    public const HEADER = 'X-Kubernetes-Authorization';
    public const DEFAULT_TOKEN_PATH = '/var/run/secrets/connection.keboola.com/serviceaccount/token';

    /**
     * @param non-empty-string $tokenPath
     */
    public function __construct(private string $tokenPath = self::DEFAULT_TOKEN_PATH)
    {
        Assert::stringNotEmpty($tokenPath, 'Service account token path must not be empty');
    }

    public function __invoke(RequestInterface $request): RequestInterface
    {
        return $request->withHeader(self::HEADER, 'Bearer ' . $this->readToken());
    }

    /**
     * @return non-empty-string
     */
    private function readToken(): string
    {
        if (!is_readable($this->tokenPath)) {
            throw new RuntimeException(sprintf(
                'Service account token file "%s" is not readable',
                $this->tokenPath,
            ));
        }

        $token = file_get_contents($this->tokenPath);
        if ($token === false) {
            throw new RuntimeException(sprintf(
                'Failed to read service account token file "%s"',
                $this->tokenPath,
            ));
        }

        $token = trim($token);
        if ($token === '') {
            throw new RuntimeException(sprintf(
                'Service account token file is empty: "%s"',
                $this->tokenPath,
            ));
        }

        return $token;
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd libs/php-api-client-base && vendor/bin/phpunit tests/Auth/KeboolaServiceAccountAuthenticatorTest.php`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add libs/php-api-client-base/src/Auth/KeboolaServiceAccountAuthenticator.php libs/php-api-client-base/tests/Auth/KeboolaServiceAccountAuthenticatorTest.php
git commit -m "feat(php-api-client-base): add KeboolaServiceAccountAuthenticator"
```

---

## Task 9: `RetryDecider` (with configurable retryable codes)

**Files:**
- Create: `libs/php-api-client-base/src/RetryDecider.php`
- Test: `libs/php-api-client-base/tests/RetryDeciderTest.php`

- [ ] **Step 1: Write the failing test**

```php
<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests;

use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\ApiClientBase\RetryDecider;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class RetryDeciderTest extends TestCase
{
    public function testRetriesOn5xx(): void
    {
        $decider = new RetryDecider(3, new NullLogger());
        self::assertTrue($decider(0, new Request('GET', '/'), new Response(500)));
    }

    public function testDoesNotRetryOn4xxByDefault(): void
    {
        $decider = new RetryDecider(3, new NullLogger());
        self::assertFalse($decider(0, new Request('GET', '/'), new Response(404)));
        self::assertFalse($decider(0, new Request('GET', '/'), new Response(429)));
    }

    public function testRetriesOnConfiguredStatusCode(): void
    {
        $decider = new RetryDecider(3, new NullLogger(), [429]);
        self::assertTrue($decider(0, new Request('GET', '/'), new Response(429)));
        // other 4xx still not retried
        self::assertFalse($decider(0, new Request('GET', '/'), new Response(404)));
    }

    public function testRetriesOnTransportError(): void
    {
        $decider = new RetryDecider(3, new NullLogger());
        $error = new ConnectException('connection refused', new Request('GET', '/'));
        self::assertTrue($decider(0, new Request('GET', '/'), null, $error));
    }

    public function testStopsAfterMaxRetries(): void
    {
        $decider = new RetryDecider(3, new NullLogger());
        self::assertFalse($decider(3, new Request('GET', '/'), new Response(500)));
    }

    public function testDoesNotRetryOn2xx(): void
    {
        $decider = new RetryDecider(3, new NullLogger());
        self::assertFalse($decider(0, new Request('GET', '/'), new Response(200)));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd libs/php-api-client-base && vendor/bin/phpunit tests/RetryDeciderTest.php`
Expected: FAIL — class not found.

- [ ] **Step 3: Write the implementation**

```php
<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase;

use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LoggerInterface;
use Throwable;

class RetryDecider
{
    /**
     * @param list<int> $retryableStatusCodes Non-5xx status codes that should also be retried (e.g. [429]).
     */
    public function __construct(
        private readonly int $maxRetries,
        private readonly LoggerInterface $logger,
        private readonly array $retryableStatusCodes = [],
    ) {
    }

    public function __invoke(
        int $retries,
        RequestInterface $request,
        ?ResponseInterface $response = null,
        mixed $error = null,
    ): bool {
        if ($retries >= $this->maxRetries) {
            return false;
        }

        $code = null;
        if ($response !== null) {
            $code = $response->getStatusCode();
        } elseif ($error instanceof Throwable) {
            $code = $error->getCode();
        }

        // Explicitly retryable codes (e.g. 429) win over the generic 4xx no-retry rule.
        if ($code !== null && in_array($code, $this->retryableStatusCodes, true)) {
            return $this->logAndRetry($code, $error, $retries);
        }

        if ($code !== null && $code >= 400 && $code < 500) {
            return false;
        }

        if ($error !== null || ($code !== null && $code >= 500)) {
            return $this->logAndRetry($code, $error, $retries);
        }

        return false;
    }

    private function logAndRetry(?int $code, mixed $error, int $retries): bool
    {
        $this->logger->warning(sprintf(
            'Request failed (%s), retrying (%s of %s)',
            match (true) {
                $error instanceof Throwable => $error->getMessage(),
                $code !== null => 'HTTP ' . $code,
                default => 'unknown',
            },
            $retries,
            $this->maxRetries,
        ));

        return true;
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd libs/php-api-client-base && vendor/bin/phpunit tests/RetryDeciderTest.php`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add libs/php-api-client-base/src/RetryDecider.php libs/php-api-client-base/tests/RetryDeciderTest.php
git commit -m "feat(php-api-client-base): add RetryDecider with configurable retryable codes"
```

---

## Task 10: `ApiClientConfiguration`

**Files:**
- Create: `libs/php-api-client-base/src/ApiClientConfiguration.php`
- Test: `libs/php-api-client-base/tests/ApiClientConfigurationTest.php`

- [ ] **Step 1: Write the failing test**

```php
<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests;

use Keboola\ApiClientBase\ApiClientConfiguration;
use Keboola\ApiClientBase\Auth\ManageApiTokenAuthenticator;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class ApiClientConfigurationTest extends TestCase
{
    public function testDefaults(): void
    {
        $config = new ApiClientConfiguration();
        self::assertNull($config->authenticator);
        self::assertSame('Keboola PHP API Client', $config->userAgent);
        self::assertSame(5, $config->backoffMaxTries);
        self::assertSame([], $config->retryableStatusCodes);
        self::assertSame(10, $config->connectTimeout);
        self::assertSame(120, $config->requestTimeout);
        self::assertNull($config->requestHandler);
        self::assertInstanceOf(NullLogger::class, $config->logger);
        self::assertNull($config->errorMessageResolver);
    }

    public function testOverrides(): void
    {
        $auth = new ManageApiTokenAuthenticator('t');
        $config = new ApiClientConfiguration(
            authenticator: $auth,
            userAgent: 'My Client',
            backoffMaxTries: 2,
            retryableStatusCodes: [429],
        );
        self::assertSame($auth, $config->authenticator);
        self::assertSame('My Client', $config->userAgent);
        self::assertSame(2, $config->backoffMaxTries);
        self::assertSame([429], $config->retryableStatusCodes);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd libs/php-api-client-base && vendor/bin/phpunit tests/ApiClientConfigurationTest.php`
Expected: FAIL — class not found.

- [ ] **Step 3: Write the implementation**

```php
<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase;

use Closure;
use GuzzleHttp\HandlerStack;
use Keboola\ApiClientBase\Auth\RequestAuthenticatorInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class ApiClientConfiguration
{
    /**
     * @param int<0, max> $backoffMaxTries
     * @param list<int> $retryableStatusCodes Non-5xx status codes to also retry (e.g. [429]).
     * @param (Closure(string, int): ?string)|null $errorMessageResolver
     *   Maps a (responseBody, statusCode) to an error message, or null to fall back to the default.
     */
    public function __construct(
        public readonly ?RequestAuthenticatorInterface $authenticator = null,
        public readonly string $userAgent = 'Keboola PHP API Client',
        public readonly int $backoffMaxTries = 5,
        public readonly array $retryableStatusCodes = [],
        public readonly int $connectTimeout = 10,
        public readonly int $requestTimeout = 120,
        public readonly null|Closure|HandlerStack $requestHandler = null,
        public readonly LoggerInterface $logger = new NullLogger(),
        public readonly ?Closure $errorMessageResolver = null,
    ) {
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd libs/php-api-client-base && vendor/bin/phpunit tests/ApiClientConfigurationTest.php`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/php-api-client-base/src/ApiClientConfiguration.php libs/php-api-client-base/tests/ApiClientConfigurationTest.php
git commit -m "feat(php-api-client-base): add ApiClientConfiguration"
```

---

## Task 11: `ApiClient`

**Files:**
- Create: `libs/php-api-client-base/src/ApiClient.php`
- Test: `libs/php-api-client-base/tests/ApiClientTest.php`

- [ ] **Step 1: Write the failing test**

```php
<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\ApiClientBase\ApiClient;
use Keboola\ApiClientBase\ApiClientConfiguration;
use Keboola\ApiClientBase\Auth\ManageApiTokenAuthenticator;
use Keboola\ApiClientBase\Exception\ClientException;
use Keboola\ApiClientBase\Tests\Fixtures\DummyModel;
use PHPUnit\Framework\TestCase;

class ApiClientTest extends TestCase
{
    public function testSendsWithoutAuthHeaderWhenNoAuthenticator(): void
    {
        $mock = new MockHandler([new Response(200, [], '{}')]);
        $client = new ApiClient('https://example.test', new ApiClientConfiguration(
            requestHandler: HandlerStack::create($mock),
        ));
        $client->sendRequest(new Request('GET', 'foo'));

        $last = $mock->getLastRequest();
        self::assertNotNull($last);
        self::assertSame([], $last->getHeader('X-KBC-ManageApiToken'));
        self::assertStringContainsString('Keboola PHP API Client', $last->getHeaderLine('User-Agent'));
    }

    public function testAddsAuthHeaderPerRequest(): void
    {
        $mock = new MockHandler([new Response(200, [], '{}')]);
        $client = new ApiClient('https://example.test', new ApiClientConfiguration(
            authenticator: new ManageApiTokenAuthenticator('secret-token'),
            requestHandler: HandlerStack::create($mock),
        ));
        $client->sendRequest(new Request('GET', 'foo'));

        $last = $mock->getLastRequest();
        self::assertNotNull($last);
        self::assertSame('secret-token', $last->getHeaderLine('X-KBC-ManageApiToken'));
    }

    public function testMapsResponseToModel(): void
    {
        $mock = new MockHandler([new Response(200, [], '{"name":"foo"}')]);
        $client = new ApiClient('https://example.test', new ApiClientConfiguration(
            requestHandler: HandlerStack::create($mock),
        ));
        $model = $client->sendRequestAndMapResponse(new Request('GET', 'foo'), DummyModel::class);

        self::assertInstanceOf(DummyModel::class, $model);
        self::assertSame('foo', $model->name);
    }

    public function testMapsResponseToList(): void
    {
        $mock = new MockHandler([new Response(200, [], '[{"name":"a"},{"name":"b"}]')]);
        $client = new ApiClient('https://example.test', new ApiClientConfiguration(
            requestHandler: HandlerStack::create($mock),
        ));
        $models = $client->sendRequestAndMapResponse(new Request('GET', 'foo'), DummyModel::class, [], true);

        self::assertCount(2, $models);
        self::assertSame('a', $models[0]->name);
        self::assertSame('b', $models[1]->name);
    }

    public function testRetriesOn5xxThenSucceeds(): void
    {
        $mock = new MockHandler([new Response(500), new Response(200, [], '{}')]);
        $client = new ApiClient('https://example.test', new ApiClientConfiguration(
            requestHandler: HandlerStack::create($mock),
        ));
        $client->sendRequest(new Request('GET', 'foo'));
        self::assertSame(0, $mock->count());
    }

    public function testThrowsClientExceptionWithDefaultMessageExtraction(): void
    {
        $mock = new MockHandler([new Response(400, [], '{"error":"bad input"}')]);
        $client = new ApiClient('https://example.test', new ApiClientConfiguration(
            requestHandler: HandlerStack::create($mock),
        ));
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('bad input');
        $this->expectExceptionCode(400);
        $client->sendRequest(new Request('GET', 'foo'));
    }

    public function testUsesCustomErrorMessageResolver(): void
    {
        $mock = new MockHandler([new Response(409, [], '{"code":"CONFLICT","error":"already exists"}')]);
        $client = new ApiClient('https://example.test', new ApiClientConfiguration(
            requestHandler: HandlerStack::create($mock),
            errorMessageResolver: static function (string $body): string {
                /** @var array{code?: string, error?: string} $data */
                $data = json_decode($body, true);
                return ($data['code'] ?? '') . ': ' . ($data['error'] ?? '');
            },
        ));
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('CONFLICT: already exists');
        $client->sendRequest(new Request('GET', 'foo'));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd libs/php-api-client-base && vendor/bin/phpunit tests/ApiClientTest.php`
Expected: FAIL — class `Keboola\ApiClientBase\ApiClient` not found.

- [ ] **Step 3: Write the implementation**

```php
<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase;

use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\GuzzleException;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\MessageFormatter;
use GuzzleHttp\Middleware;
use JsonException;
use Keboola\ApiClientBase\Exception\ClientException;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Throwable;

class ApiClient
{
    private readonly GuzzleClient $httpClient;
    /** @var (Closure(string, int): ?string)|null */
    private $errorMessageResolver;

    /**
     * @param non-empty-string|null $baseUrl
     */
    public function __construct(
        ?string $baseUrl = null,
        ?ApiClientConfiguration $configuration = null,
    ) {
        $configuration ??= new ApiClientConfiguration();
        $this->errorMessageResolver = $configuration->errorMessageResolver;

        $stack = $configuration->requestHandler instanceof HandlerStack
            ? $configuration->requestHandler
            : HandlerStack::create($configuration->requestHandler);

        if ($configuration->authenticator !== null) {
            $stack->push(Middleware::mapRequest($configuration->authenticator));
        }

        if ($configuration->backoffMaxTries > 0) {
            $stack->push(Middleware::retry(new RetryDecider(
                $configuration->backoffMaxTries,
                $configuration->logger,
                $configuration->retryableStatusCodes,
            )));
        }

        $stack->push(Middleware::log(
            $configuration->logger,
            new MessageFormatter('{method} {uri} : {code} {res_header_Content-Length}'),
        ));

        $this->httpClient = new GuzzleClient([
            'base_uri' => $baseUrl === null ? null : rtrim($baseUrl, '/') . '/',
            'handler' => $stack,
            'headers' => [
                'User-Agent' => $configuration->userAgent,
            ],
            'connect_timeout' => $configuration->connectTimeout,
            'timeout' => $configuration->requestTimeout,
        ]);
    }

    public function sendRequest(RequestInterface $request): void
    {
        $this->doSendRequest($request);
    }

    /**
     * @template T of ResponseModelInterface
     * @param class-string<T> $responseClass
     * @param array<string, mixed> $options
     * @return ($isList is true ? list<T> : T)
     */
    public function sendRequestAndMapResponse(
        RequestInterface $request,
        string $responseClass,
        array $options = [],
        bool $isList = false,
    ) {
        $response = $this->doSendRequest($request, $options);

        try {
            $data = Json::decodeArray($response->getBody()->getContents());
        } catch (JsonException $e) {
            throw new ClientException('Response is not valid JSON: ' . $e->getMessage(), 0, $e);
        }

        try {
            if ($isList) {
                return array_values(array_map($responseClass::fromResponseData(...), $data));
            }
            return $responseClass::fromResponseData($data);
        } catch (Throwable $e) {
            throw new ClientException('Failed to map response data: ' . $e->getMessage(), 0, $e);
        }
    }

    /**
     * @param array<string, mixed> $options
     */
    private function doSendRequest(RequestInterface $request, array $options = []): ResponseInterface
    {
        try {
            return $this->httpClient->send($request, $options);
        } catch (RequestException $e) {
            throw $this->processRequestException($e);
        } catch (GuzzleException $e) {
            throw new ClientException($e->getMessage(), 0, $e);
        }
    }

    private function processRequestException(RequestException $e): ClientException
    {
        $response = $e->getResponse();
        if ($response === null) {
            return new ClientException(trim($e->getMessage()), 0, $e);
        }

        $statusCode = $response->getStatusCode();
        $body = (string) $response->getBody();

        if ($this->errorMessageResolver !== null) {
            $message = ($this->errorMessageResolver)($body, $statusCode);
            if ($message !== null && $message !== '') {
                return new ClientException($message, $statusCode, $e);
            }
            return new ClientException(trim($e->getMessage()), $statusCode, $e);
        }

        return new ClientException($this->defaultErrorMessage($body) ?? trim($e->getMessage()), $statusCode, $e);
    }

    private function defaultErrorMessage(string $body): ?string
    {
        try {
            $data = Json::decodeArray($body);
        } catch (JsonException) {
            return null;
        }

        foreach (['error', 'message'] as $key) {
            if (isset($data[$key]) && is_string($data[$key]) && $data[$key] !== '') {
                return $data[$key];
            }
        }

        return null;
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd libs/php-api-client-base && vendor/bin/phpunit tests/ApiClientTest.php`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
git add libs/php-api-client-base/src/ApiClient.php libs/php-api-client-base/tests/ApiClientTest.php
git commit -m "feat(php-api-client-base): add ApiClient"
```

---

## Task 12: README — document the base as the base for Keboola service clients

**Files:**
- Create: `libs/php-api-client-base/README.md`

- [ ] **Step 1: Write `README.md`**

The README must frame this as the base for **Keboola service** API clients — not a generic HTTP client. It encodes Keboola conventions (the Keboola auth headers, retry/JSON/error behavior shared by the platform's service clients). Content:

````markdown
# Keboola PHP API Client Base

Shared base for building PHP clients for **Keboola services** (Storage, Manage,
Vault, Git Service, Sandboxes, Sync Actions, Job Queue, …). It is **not** a
general-purpose HTTP client — it encodes Keboola platform conventions: the
Keboola authentication headers, retry behavior, JSON handling, and error
normalization that every Keboola service client needs.

Used by `keboola/vault-api-client`, `keboola/sandboxes-service-api-client`,
`keboola/git-service-api-client`, `keboola/sync-actions-client`,
`keboola/azure-api-client`, and new Keboola service clients.

## Installation

```bash
composer require keboola/php-api-client-base
```

## What it provides

- `ApiClient` — Guzzle wrapper with per-request auth, retry, logging, and
  response-to-model mapping.
- `ApiClientConfiguration` — auth, retries, timeouts, logger, error resolver.
- `Auth\RequestAuthenticatorInterface` + ready authenticators for the Keboola
  auth schemes: `StorageApiTokenAuthenticator` (`X-StorageApi-Token`),
  `ManageApiTokenAuthenticator` (`X-KBC-ManageApiToken`),
  `KeboolaServiceAccountAuthenticator` (projected SA token →
  `X-Kubernetes-Authorization`).
- `RetryDecider`, `Json`, `ResponseModelInterface`, `Exception\ClientException`.

## Building a Keboola service client

Compose an `ApiClient` inside your service facade and map responses to models:

```php
use GuzzleHttp\Psr7\Request;
use Keboola\ApiClientBase\ApiClient;
use Keboola\ApiClientBase\ApiClientConfiguration;
use Keboola\ApiClientBase\Auth\StorageApiTokenAuthenticator;
use Keboola\ApiClientBase\Json;
use Keboola\ApiClientBase\ResponseModelInterface;

final class WidgetModel implements ResponseModelInterface
{
    public function __construct(public readonly string $id) {}

    public static function fromResponseData(array $data): static
    {
        \assert(is_string($data['id']));
        return new self($data['id']);
    }
}

final class MyServiceClient
{
    private ApiClient $apiClient;

    public function __construct(string $baseUrl, ?ApiClientConfiguration $configuration = null)
    {
        $this->apiClient = new ApiClient($baseUrl, $configuration);
    }

    public function createWidget(string $name): WidgetModel
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request('POST', 'widgets', ['Content-Type' => 'application/json'], Json::encodeArray(['name' => $name])),
            WidgetModel::class,
        );
    }
}

$client = new MyServiceClient(
    'https://my-service.keboola.com',
    new ApiClientConfiguration(
        authenticator: new StorageApiTokenAuthenticator($storageApiToken),
    ),
);
```

## Authentication

Pick the authenticator matching the service's scheme, or implement
`RequestAuthenticatorInterface` for a service-specific scheme (e.g. azure's
OAuth). `Content-Type` is set per request on calls with a body; the only Guzzle
default header is `User-Agent` (set via `ApiClientConfiguration::$userAgent`).

## License

MIT
````

- [ ] **Step 2: Commit**

```bash
git add libs/php-api-client-base/README.md
git commit -m "docs(php-api-client-base): add README framed for Keboola service clients"
```

---

## Task 13: Full CI green (phpcs + phpstan + phpunit)

**Files:**
- Modify (as needed): any `src/` files flagged by phpcs/phpstan.

- [ ] **Step 1: Run the full CI script**

Run: `cd libs/php-api-client-base && composer ci`
Expected: `composer validate`, `phpcs`, `phpstan` (level max), and `phpunit` all PASS.

- [ ] **Step 2: Fix any phpcs/phpstan findings**

Run `composer phpcbf` for auto-fixable style; manually address remaining phpstan findings (e.g. add missing `@var`/generics). Re-run `composer ci` until green.

- [ ] **Step 3: Commit any fixes**

```bash
git add libs/php-api-client-base
git commit -m "chore(php-api-client-base): satisfy phpcs and phpstan"
```

---

## Task 14: Register the lib in the monorepo CI (test + split)

**Files:**
- Modify: `azure-pipelines.tests.yml` (lib-level) — create `libs/php-api-client-base/azure-pipelines.tests.yml`
- Modify: `azure-pipelines.yml` (root) — add the test-template include + split mapping
- Modify: `azure-pipelines.tags.yml` (root) — add the repository resource + `split-library` job

The existing `git-service-api-client` entries are the working template; duplicate each with the substitution `git-service-api-client → php-api-client-base` and `git-service-php-api-client → php-api-client-base` (the read-only target repo name; create that repo if it does not exist).

- [ ] **Step 1: Create the lib test pipeline**

`libs/php-api-client-base/azure-pipelines.tests.yml`:

```yaml
jobs:
  - template: ../../azure-pipelines/jobs/run-tests.yml
    parameters:
      displayName: Tests
      serviceName: dev-php-api-client-base
      testCommand: bash -c 'composer install && composer ci'
```

- [ ] **Step 2: Wire the root test pipeline**

In `azure-pipelines.yml`, locate the `- template: libs/git-service-api-client/azure-pipelines.tests.yml` include and the `gitServiceApiClient:libs/git-service-api-client` change-detection mapping; add the analogous lines:

```yaml
      - template: libs/php-api-client-base/azure-pipelines.tests.yml
```
```yaml
                phpApiClientBase:libs/php-api-client-base \
```

- [ ] **Step 3: Wire the split job**

In `azure-pipelines.tags.yml`, duplicate the git-service repository resource and `split-library` job, substituting names:

```yaml
    - repository: php-api-client-base
      type: github
      endpoint: keboola
      name: keboola/php-api-client-base
```
```yaml
  - template: azure-pipelines/jobs/split-library.yml
    parameters:
      condition: startsWith(variables['Build.SourceBranch'], 'refs/tags/php-api-client-base/')
      targetRepo: php-api-client-base
      libraryPath: libs/php-api-client-base
      tagPrefix: php-api-client-base/
```

(Match the surrounding indentation/structure of the existing git-service entries exactly.)

- [ ] **Step 4: Validate YAML locally**

Run: `cd libs/php-api-client-base && php -r "var_dump(yaml_parse_file('azure-pipelines.tests.yml') !== false);"` (or any available YAML linter)
Expected: no parse error. (Root pipeline syntax is validated by Azure DevOps on push.)

- [ ] **Step 5: Commit**

```bash
git add libs/php-api-client-base/azure-pipelines.tests.yml azure-pipelines.yml azure-pipelines.tags.yml
git commit -m "ci(php-api-client-base): register lib for tests and split publishing"
```

- [ ] **Step 6: Open PR 1**

Push `pepa/common-api-lib` and open the PR against `main`. Title/body per `/home/pepa/.claude-keboola/pr-style.md`. This PR is the base for all client-migration PRs.

---

# PR 2–6 — Client migration playbooks

Each migration PR follows the same shape. **The granular step-level TDD plan for each client is written at the start of that PR**, after reading the client's facade/models/tests in full — the base lib (PR 1) must exist first, and sync-actions/azure require structural investigation (see spec §10). The common procedure and per-client scope are fixed below.

## Common migration procedure (every client PR)

1. **Branch (stacked):** `wtm fork pepa/common-api-lib-<client> --base pepa/common-api-lib`.
2. **Add the base dependency** to the client's `composer.json`:
   - Add (or extend) the `repositories` block:
     ```json
     "repositories": {
         "libs": {
             "type": "path",
             "url": "../../libs/*"
         }
     },
     ```
   - Add to `require`: `"keboola/php-api-client-base": "*@dev"`.
   - Run `composer update keboola/php-api-client-base` and confirm it resolves to the local path.
3. **Delete** the duplicated plumbing files (per-client list below).
4. **Repoint** all `use` statements from the client's own classes to the base equivalents (mapping below).
5. **Apply per-client special handling** (below).
6. **Update the client's own tests** for the new namespaces/auth construction. The existing suite is the regression gate — behavior must not change (except the deliberate auth-construction API change).
7. **Update the client's `README.md`** with a working usage example reflecting the new base-lib construction — the facade plus a `Keboola\ApiClientBase\ApiClientConfiguration` carrying the right authenticator, and one representative call. Template (substitute the real facade, authenticator, and a real method):

   ````markdown
   ## Usage

   ```php
   use Keboola\ApiClientBase\ApiClientConfiguration;
   use Keboola\ApiClientBase\Auth\StorageApiTokenAuthenticator; // or the auth this service uses
   use Keboola\<ClientNamespace>\<FacadeClass>;

   $client = new <FacadeClass>(
       'https://<service>.keboola.com',
       new ApiClientConfiguration(
           authenticator: new StorageApiTokenAuthenticator($storageApiToken),
       ),
   );

   $result = $client-><representativeMethod>(...);
   ```
   ````

   If the README documented the old `auth:`/authenticator class names, replace them. Verify the example compiles against the facade's real signatures.
8. **Run `composer ci`** in the client lib; iterate to green.
9. **Bump the major version** as applicable in the client's release notes/changelog, and document the consumer-facing `use` changes.
10. **Open the PR** based on `pepa/common-api-lib` (per `/home/pepa/.claude-keboola/pr-style.md`).

## Base-class mapping (applies to all clients)

| Client's own class (deleted) | Base replacement |
|---|---|
| `…\ApiClient` | `Keboola\ApiClientBase\ApiClient` |
| `…\ApiClientConfiguration` | `Keboola\ApiClientBase\ApiClientConfiguration` |
| `…\RetryDecider` | `Keboola\ApiClientBase\RetryDecider` |
| `…\Json` | `Keboola\ApiClientBase\Json` |
| `…\ResponseModelInterface` | `Keboola\ApiClientBase\ResponseModelInterface` |
| `…\Exception\ClientException` | `Keboola\ApiClientBase\Exception\ClientException` |
| `…\Authentication\RequestAuthenticatorInterface` (vault, azure) | `Keboola\ApiClientBase\Auth\RequestAuthenticatorInterface` |
| storage-token authenticator (vault/sandboxes/sync-actions) | `Keboola\ApiClientBase\Auth\StorageApiTokenAuthenticator` |

## PR 2 — `vault-api-client`

- **Delete:** `src/ApiClient.php`, `src/ApiClientConfiguration.php`, `src/RetryDecider.php`, `src/Json.php`, `src/ResponseModelInterface.php`, `src/Exception/ClientException.php`, `src/Authentication/RequestAuthenticatorInterface.php`, `src/Authentication/StorageApiTokenAuthenticator.php`.
- **Keep:** `src/Variables/VariablesApiClient.php`, `src/Variables/Model/*`.
- **Special handling:** none beyond the common mapping. `VariablesApiClient` is already a composition facade over `ApiClient`; only its imports change. Confirm vault's error-response shape and pass an `errorMessageResolver` only if vault's messages differ from the default `error`/`message` extraction.
- **Acceptance:** existing vault test suite green under `composer ci`. *(Simplest migration — do this first to validate the base in anger.)*

## PR 3 — `sandboxes-service-api-client`

- **Delete:** `src/ApiClient.php`, `src/ApiClientConfiguration.php`, `src/RetryDecider.php`, `src/Json.php`, `src/ResponseModelInterface.php`, `src/Exception/ClientException.php`, `src/Authentication/StorageTokenAuthenticator.php`.
- **Keep:** `src/Apps/*`, `src/Sandboxes/*` (incl. `Sandboxes/Legacy/*`).
- **Special handling:** sandboxes' `StorageTokenAuthenticator` has no interface and a `STORAGE_TOKEN_HEADER = 'X-StorageApi-Token'` const — identical header to the base `StorageApiTokenAuthenticator`. Replace all usages with the base authenticator. Update any code/tests referencing the `STORAGE_TOKEN_HEADER` const to the base `StorageApiTokenAuthenticator::HEADER`.
- **Acceptance:** existing sandboxes-service test suite green.

## PR 4 — `git-service-api-client`

- **Delete:** `src/ApiClient.php`, `src/ApiClientConfiguration.php`, `src/RetryDecider.php`, `src/Json.php`, `src/ResponseModelInterface.php`, `src/Exception/ClientException.php`, `src/Auth/AuthInterface.php`, `src/Auth/ManageApiTokenAuth.php`, `src/Auth/KeboolaServiceAccountAuth.php`.
- **Keep:** `src/GitServiceApiClient.php`, `src/Model/*`, `src/CredentialType.php`, `src/KeyPermission.php`, `src/NewCredential.php`, `src/CreatedCredential.php` (and any remaining domain files).
- **Special handling (auth contract flip — the consumer break):**
  - git-service today uses the header-map `AuthInterface::getAuthenticationHeaders()`. The base uses the request-decorator. Consumers move from `ManageApiTokenAuth` → `ManageApiTokenAuthenticator` and `KeboolaServiceAccountAuth` → `KeboolaServiceAccountAuthenticator`, and from `ApiClientConfiguration(auth: …)` → `(authenticator: …)`.
  - **Preserve the default-SA behavior:** today `ApiClientConfiguration` defaults `auth` to `KeboolaServiceAccountAuth`. The base config defaults `authenticator` to `null`. So `GitServiceApiClient::__construct` must default the authenticator: when the caller passes no configuration, construct `new ApiClientConfiguration(authenticator: new KeboolaServiceAccountAuthenticator())` before handing it to the base `ApiClient`.
  - **Preserve the error message format:** git-service's `ApiClient::processRequestException` formats `{code, error}` JSON as `"<code>: <error>"`. Pass an `errorMessageResolver` to the base config that reproduces this exactly so error messages don't regress.
  - **Content-Type:** confirm every body-bearing facade method passes `Content-Type: application/json` per request (`createRepository` does via `JSON_HEADERS`); add it to any write method that relied on the now-removed Guzzle default.
- **Acceptance:** existing git-service test suite green (after updating tests for `authenticator:`/authenticator class names); the SA-default and error-format tests must still pass.

## PR 5 — `sync-actions-api-php-client`

- **Delete:** `src/RetryDecider.php`, `src/StorageApiTokenAuthenticator.php`.
- **Reshape:** `src/Client.php` is currently its own Guzzle client (not a facade-over-`ApiClient`) and sets a default `Content-Type`. Convert it to a composition facade that holds a base `ApiClient`, builds PSR-7 requests, and uses base `Json` (sync-actions has no `Json` today). Set `Content-Type: application/json` per request on POSTs.
- **Keep:** `src/ActionData.php`, `src/Model/*` (note: sync-actions' `Model/ResponseModelInterface.php` is replaced by the base interface — update `Model/ActionResponse` and `Model/ListActionsResponse` to implement `Keboola\ApiClientBase\ResponseModelInterface`).
- **Special handling:** this is the largest structural change; read `src/Client.php` and its tests in full before writing the step plan. Confirm sync-actions' error-response shape for the `errorMessageResolver`.
- **Acceptance:** existing sync-actions test suite green.

## PR 6 — `azure-api-client`

- **Delete:** `src/ApiClient.php`, `src/ApiClientConfiguration.php`, `src/RetryDecider.php`, `src/Json.php`, `src/ResponseModelInterface.php`, `src/Exception/ClientException.php`, `src/Authentication/Authenticator/RequestAuthenticatorInterface.php`.
- **Keep:** the **entire** `src/Authentication/*` subtree except the deleted leaf interface — `Authenticator/ClientCredentialsAuth.php`, `ManagedCredentialsAuth.php`, `StaticBearerTokenAuth.php`, `CustomHeaderAuth.php`, `RequestAuthenticatorFactoryInterface.php`, `Internal/*`, `Model/*`; plus `src/Marketplace/*`, `src/Exception/InvalidResponseException.php` (azure-specific).
- **Special handling:**
  - The leaf authenticators (`ClientCredentialsAuth`, `ManagedCredentialsAuth`, `StaticBearerTokenAuth`, `CustomHeaderAuth`, and `Internal/*Authenticator`) now implement the base `Auth\RequestAuthenticatorInterface` (identical signature). Delete azure's own `Authenticator/RequestAuthenticatorInterface.php` and repoint.
  - **Reshape `authenticate($resource)`:** azure currently resolves the authenticator lazily and pushes it onto the handler stack. Instead, the azure facade (`MarketplaceApiClient` / `MeteringServiceApiClient`) resolves the per-resource authenticator via the factory and passes it to the base `ApiClientConfiguration(authenticator: …)` at construction. Token caching stays inside `BearerTokenResolver`.
  - **429 retries:** pass `retryableStatusCodes: [429]` to the base config (replaces azure's `AZURE_THROTTLING_CODE` special-case in its old RetryDecider).
  - Confirm azure's error shape and wire `errorMessageResolver` (azure has `InvalidResponseException` for its own cases — keep that path).
- **Special note:** read azure's `ApiClient`, `SystemAuthenticatorResolver`, `BearerTokenAuthenticatorFactory`, and facade tests in full before writing the step plan — this is the most involved migration; do it last.
- **Acceptance:** existing azure test suite green.

---

## Self-review (against the spec)

**Spec coverage:**
- §5 package layout → Tasks 1–11 create every listed file. ✓
- §6.1 config (no `defaultHeaders`) → Task 10 (no `defaultHeaders` field). ✓
- §6.2 ApiClient (auth→retry→log order, nullable baseUrl, UA-only default, per-request auth) → Task 11. ✓
- §6.3 RetryDecider/Json/ResponseModelInterface → Tasks 9, 2, 3. ✓
- §6.4 error handling (default extractor + `errorMessageResolver`) → Task 11. ✓
- §7 auth (decorator interface + 3 authenticators, azure subtree kept) → Tasks 5–8 + PR 6. ✓
- §8 migration map (all 5 clients) → PR 2–6 playbooks. ✓
- §8 monorepo wiring (path repo + split) → Task 14 + common migration procedure. ✓
- §9 testing → TDD throughout; client suites are the regression gate. ✓
- §10 risks (sync-actions structure, error shapes, azure lifecycle, nullable baseUrl, git-service Content-Type) → called out in PR 4/5/6 special handling + Task 11. ✓
- §11 rollout order (vault→sandboxes→git-service→sync-actions→azure) → PR 2→6 ordering. ✓
- Base lib documented as the base for **Keboola service** clients (not generic) → Task 12 README + the `composer.json` description in Task 1. ✓
- Each client README updated with a usage example → common migration procedure step 7 (applies to PR 2–6). ✓

**Placeholder scan:** PR 1 (Tasks 1–14) contains complete code/commands for every step. PR 2–6 are intentionally playbook-level (granular steps authored per-PR after reading each client) — this is the spec-sanctioned decomposition, not a placeholder, and each has exact file-deletion lists + class mappings + acceptance.

**Type consistency:** `ApiClientConfiguration` fields (Task 10) match `ApiClient` usage (Task 11): `authenticator`, `backoffMaxTries`, `retryableStatusCodes`, `connectTimeout`, `requestTimeout`, `requestHandler`, `logger`, `errorMessageResolver`. `RetryDecider($maxRetries, $logger, $retryableStatusCodes)` (Task 9) matches the call in Task 11. Authenticators implement `RequestAuthenticatorInterface::__invoke` (Task 5) and are used via `Middleware::mapRequest` in Task 11. `ResponseModelInterface::fromResponseData(): static` (Task 3) matches `sendRequestAndMapResponse` usage (Task 11). ✓
