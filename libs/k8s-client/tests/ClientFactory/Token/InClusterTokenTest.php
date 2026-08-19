<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ClientFactory\Token;

use Keboola\K8sClient\ClientFactory\Token\InClusterToken;
use Keboola\K8sClient\Exception\ConfigurationException;
use PHPUnit\Framework\TestCase;

class InClusterTokenTest extends TestCase
{
    public function testGetValueReadsFromFile(): void
    {
        $tmpFile = tempnam(sys_get_temp_dir(), 'k8s-creds-test');
        file_put_contents($tmpFile, 'foo-token-1');

        $token = new InClusterToken($tmpFile);

        self::assertSame('foo-token-1', $token->getValue());
    }

    public function testGetValueTrimsFileContents(): void
    {
        $tmpFile = tempnam(sys_get_temp_dir(), 'k8s-creds-test');
        file_put_contents($tmpFile, "\nfoo-token-1\n");

        $token = new InClusterToken($tmpFile);

        self::assertSame('foo-token-1', $token->getValue());
    }

    public function testGetValueReadsLazily(): void
    {
        $tmpFile = tempnam(sys_get_temp_dir(), 'k8s-creds-test');
        file_put_contents($tmpFile, 'foo-token-1');

        $token = new InClusterToken($tmpFile);

        file_put_contents($tmpFile, 'foo-token-2');
        self::assertSame('foo-token-2', $token->getValue());
    }

    public function testGetValueCachesValue(): void
    {
        $tmpFile = tempnam(sys_get_temp_dir(), 'k8s-creds-test');
        file_put_contents($tmpFile, 'foo-token-1');

        $token = new InClusterToken(
            $tmpFile,
            expirationTime: 2,
        );

        self::assertSame('foo-token-1', $token->getValue());

        file_put_contents($tmpFile, 'foo-token-2');
        self::assertSame('foo-token-1', $token->getValue());

        sleep(2);
        self::assertSame('foo-token-2', $token->getValue());
    }

    public function testGetValueThrowsExceptionOnReadError(): void
    {
        $token = new InClusterToken('non-existing-file');

        $this->expectException(ConfigurationException::class);
        $this->expectExceptionMessage('Failed to read contents of in-cluster configuration file "non-existing-file"');
        $token->getValue();
    }

    public function testGetValueReturnsCachedValueWhenRefreshReadFails(): void
    {
        $tmpFile = (string) tempnam(sys_get_temp_dir(), 'k8s-creds-test');
        file_put_contents($tmpFile, 'foo-token-1');

        $token = new InClusterToken(
            $tmpFile,
            expirationTime: 0,
        );

        self::assertSame('foo-token-1', $token->getValue());

        unlink($tmpFile);

        self::assertSame('foo-token-1', $token->getValue());
    }

    public function testGetValueReadsExternallyRotatedTokenWithoutSpendingRetryBudget(): void
    {
        // projected service account volume layout: token -> ..data/token -> ..<timestamp>/token
        $dir = sys_get_temp_dir().'/k8s-creds-test-'.uniqid();
        mkdir($dir.'/..2026_a', 0777, true);
        file_put_contents($dir.'/..2026_a/token', 'foo-token-1');
        symlink('..2026_a', $dir.'/..data');
        symlink('..data/token', $dir.'/token');

        $token = new InClusterToken(
            $dir.'/token',
            expirationTime: 0,
            retryBaseDelayMicroseconds: 1_000_000,
        );

        self::assertSame('foo-token-1', $token->getValue());

        // rotate the way kubelet does - in another process, so that PHP can't invalidate its own
        // realpath cache the way it would for a deletion performed by this process
        $rotate = <<<SH
            cd {$dir}
            mkdir ..2026_b && printf foo-token-2 > ..2026_b/token
            ln -sfn ..2026_b ..data_tmp && mv -Tf ..data_tmp ..data
            rm -rf ..2026_a
            SH;
        exec($rotate, $output, $resultCode);
        self::assertSame(0, $resultCode);

        $startTime = microtime(true);
        self::assertSame('foo-token-2', $token->getValue());
        self::assertLessThan(0.5, microtime(true) - $startTime);
    }

    public function testConstructRejectsInvalidMaxReadAttempts(): void
    {
        $this->expectException(ConfigurationException::class);
        $this->expectExceptionMessage('Maximum number of token read attempts must be at least 1');

        new InClusterToken('token-file', maxReadAttempts: 0);
    }

    public function testConstructRejectsNegativeRetryBaseDelay(): void
    {
        $this->expectException(ConfigurationException::class);
        $this->expectExceptionMessage('Token read retry base delay must not be negative');

        new InClusterToken('token-file', retryBaseDelayMicroseconds: -1);
    }

    public function testGetValueReturnsCachedValueWhenFileBecomesEmpty(): void
    {
        $tmpFile = (string) tempnam(sys_get_temp_dir(), 'k8s-creds-test');
        file_put_contents($tmpFile, 'foo-token-1');

        $token = new InClusterToken(
            $tmpFile,
            expirationTime: 0,
        );

        self::assertSame('foo-token-1', $token->getValue());

        file_put_contents($tmpFile, '');

        self::assertSame('foo-token-1', $token->getValue());
    }

    public function testGetValueRetriesUntilTokenAppears(): void
    {
        $tmpFile = (string) tempnam(sys_get_temp_dir(), 'k8s-creds-test');
        file_put_contents($tmpFile, '');

        $writer = proc_open(
            sprintf('sleep 0.2; printf foo-token-1 > %s', escapeshellarg($tmpFile)),
            [],
            $pipes,
        );
        self::assertNotFalse($writer);

        $token = new InClusterToken(
            $tmpFile,
            maxReadAttempts: 8,
            retryBaseDelayMicroseconds: 100_000,
        );

        try {
            self::assertSame('foo-token-1', $token->getValue());
        } finally {
            proc_close($writer);
        }
    }

    public function testGetValueThrowsAfterExhaustingRetries(): void
    {
        $tmpFile = (string) tempnam(sys_get_temp_dir(), 'k8s-creds-test');
        file_put_contents($tmpFile, '');

        $token = new InClusterToken(
            $tmpFile,
            maxReadAttempts: 3,
            retryBaseDelayMicroseconds: 1_000,
        );

        $this->expectException(ConfigurationException::class);
        $this->expectExceptionMessage(sprintf(
            'In-cluster configuration file "%s" is empty or unreadable after 3 attempts',
            $tmpFile,
        ));
        $token->getValue();
    }

    public function testGetValueDoesNotRetryWhenFileIsMissing(): void
    {
        $tmpFile = (string) tempnam(sys_get_temp_dir(), 'k8s-creds-test');
        file_put_contents($tmpFile, 'foo-token-1');

        $token = new InClusterToken(
            $tmpFile,
            expirationTime: 0,
            maxReadAttempts: 6,
            retryBaseDelayMicroseconds: 1_000_000,
        );

        self::assertSame('foo-token-1', $token->getValue());

        unlink($tmpFile);

        $startTime = microtime(true);
        self::assertSame('foo-token-1', $token->getValue());
        self::assertLessThan(0.5, microtime(true) - $startTime);
    }

    public function testGetValueRetriesReadImmediatelyAfterFailedRefresh(): void
    {
        $tmpFile = (string) tempnam(sys_get_temp_dir(), 'k8s-creds-test');
        file_put_contents($tmpFile, 'foo-token-1');

        $token = new InClusterToken(
            $tmpFile,
            expirationTime: 1,
        );

        self::assertSame('foo-token-1', $token->getValue());

        sleep(1);
        unlink($tmpFile);
        self::assertSame('foo-token-1', $token->getValue());

        file_put_contents($tmpFile, 'foo-token-2');
        self::assertSame('foo-token-2', $token->getValue());
    }
}
