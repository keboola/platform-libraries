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

    protected function tearDown(): void
    {
        FunctionMocks::reset();
        parent::tearDown();
    }

    public function testRecoversWhenFirstReadFailsThenSucceeds(): void
    {
        // false => transient unreadable during a symlink swap; is_readable() is
        // shadowed to true so the re-read path is taken rather than throwing.
        FunctionMocks::enable([false, 'the-token'], isReadable: true);

        $authenticator = new KeboolaServiceAccountAuthenticator('/fake/sa/token');
        $request = $authenticator(new Request('GET', 'https://example.test'));

        self::assertSame('Bearer the-token', $request->getHeaderLine('X-Kubernetes-Authorization'));
        self::assertSame(2, FunctionMocks::readCount());
        self::assertSame([], FunctionMocks::recordedSleeps());
    }

    public function testRecoversWhenFirstReadEmptyThenSucceeds(): void
    {
        FunctionMocks::enable(['', 'the-token']);

        $authenticator = new KeboolaServiceAccountAuthenticator('/fake/sa/token');
        $request = $authenticator(new Request('GET', 'https://example.test'));

        self::assertSame('Bearer the-token', $request->getHeaderLine('X-Kubernetes-Authorization'));
        self::assertSame(2, FunctionMocks::readCount());
        // exactly one backoff sleep (40 ms base) before the successful retry
        self::assertSame([40_000], FunctionMocks::recordedSleeps());
    }

    public function testDefaultScheduleRetriesFiveTimesThenThrows(): void
    {
        FunctionMocks::enable(['', '', '', '', '', '']); // 6 reads, all empty

        $authenticator = new KeboolaServiceAccountAuthenticator('/fake/sa/token');

        try {
            $authenticator(new Request('GET', 'https://example.test'));
            self::fail('Expected RuntimeException');
        } catch (RuntimeException $e) {
            self::assertStringContainsString('is empty', $e->getMessage());
        }

        self::assertSame(6, FunctionMocks::readCount());
        self::assertSame(
            [40_000, 80_000, 160_000, 320_000, 640_000],
            FunctionMocks::recordedSleeps(),
        );
    }

    public function testSingleAttemptThrowsWithoutSleeping(): void
    {
        FunctionMocks::enable(['']);

        $authenticator = new KeboolaServiceAccountAuthenticator('/fake/sa/token', maxReadAttempts: 1);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('is empty');
        try {
            $authenticator(new Request('GET', 'https://example.test'));
        } finally {
            self::assertSame(1, FunctionMocks::readCount());
            self::assertSame([], FunctionMocks::recordedSleeps());
        }
    }
}
