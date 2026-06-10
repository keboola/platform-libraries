<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests\Auth;

use Keboola\ApiClientBase\Auth\KeboolaServiceAccountAuthenticator;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class KeboolaServiceAccountAuthenticatorTest extends TestCase
{
    public function testReadsTokenFileAndReturnsBearerHeader(): void
    {
        $path = (string) tempnam(sys_get_temp_dir(), 'sa-token-');
        file_put_contents($path, "the-token\n");
        try {
            /** @phpstan-ignore-next-line argument.type — tempnam returns string, not non-empty-string */
            $authenticator = new KeboolaServiceAccountAuthenticator($path);
            self::assertSame(
                ['X-Kubernetes-Authorization' => 'Bearer the-token'],
                $authenticator->getAuthenticationHeaders(),
            );
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
            self::assertSame(
                ['X-Kubernetes-Authorization' => 'Bearer first'],
                $authenticator->getAuthenticationHeaders(),
            );

            file_put_contents($path, "second\n");
            self::assertSame(
                ['X-Kubernetes-Authorization' => 'Bearer second'],
                $authenticator->getAuthenticationHeaders(),
            );
        } finally {
            @unlink($path);
        }
    }

    public function testThrowsWhenFileMissing(): void
    {
        $authenticator = new KeboolaServiceAccountAuthenticator('/nonexistent/token');
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('/nonexistent/token');
        $authenticator->getAuthenticationHeaders();
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
            $authenticator->getAuthenticationHeaders();
        } finally {
            @unlink($path);
        }
    }
}
