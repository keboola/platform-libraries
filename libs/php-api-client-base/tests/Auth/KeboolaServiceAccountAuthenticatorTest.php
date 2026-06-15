<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests\Auth;

use GuzzleHttp\Psr7\Request;
use InvalidArgumentException;
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
            /** @phpstan-ignore-next-line argument.type — tempnam returns string, not non-empty-string */
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
            /** @phpstan-ignore-next-line argument.type — tempnam returns string, not non-empty-string */
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
            /** @phpstan-ignore-next-line argument.type — tempnam returns string, not non-empty-string */
            $authenticator = new KeboolaServiceAccountAuthenticator($path);
            $this->expectException(RuntimeException::class);
            $this->expectExceptionMessage('is empty');
            $authenticator(new Request('GET', 'https://example.test'));
        } finally {
            @unlink($path);
        }
    }

    public function testRejectsZeroMaxReadAttempts(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('maxReadAttempts');
        /** @phpstan-ignore-next-line argument.type — exercising the runtime guard */
        new KeboolaServiceAccountAuthenticator('/fake/token', 0);
    }

    public function testRejectsNegativeRetryDelay(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('retryBaseDelayMicroseconds');
        /** @phpstan-ignore-next-line argument.type — exercising the runtime guard */
        new KeboolaServiceAccountAuthenticator('/fake/token', 6, -1);
    }

    public function testExposesDefaultRetryConstants(): void
    {
        self::assertSame(6, KeboolaServiceAccountAuthenticator::DEFAULT_MAX_READ_ATTEMPTS);
        self::assertSame(40_000, KeboolaServiceAccountAuthenticator::DEFAULT_RETRY_BASE_DELAY_US);
    }
}
