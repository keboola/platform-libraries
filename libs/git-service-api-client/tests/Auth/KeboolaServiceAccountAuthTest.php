<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Tests\Auth;

use InvalidArgumentException;
use Keboola\GitServiceApiClient\Auth\KeboolaServiceAccountAuth;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class KeboolaServiceAccountAuthTest extends TestCase
{
    public function testRejectsEmptyTokenPath(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('token path must not be empty');
        /** @phpstan-ignore-next-line argument.type — exercising the runtime guard */
        new KeboolaServiceAccountAuth('');
    }

    public function testDefaultTokenPathPointsAtKeboolaProjectedMount(): void
    {
        self::assertSame(
            '/var/run/secrets/connection.keboola.com/serviceaccount/token',
            KeboolaServiceAccountAuth::DEFAULT_TOKEN_PATH,
        );
    }

    public function testReturnsBearerHeaderFromFile(): void
    {
        $tokenPath = $this->makeTempFile('jwt-from-file');
        try {
            $auth = new KeboolaServiceAccountAuth($tokenPath);

            self::assertSame(
                ['X-Kubernetes-Authorization' => 'Bearer jwt-from-file'],
                $auth->getAuthenticationHeaders(),
            );
        } finally {
            @unlink($tokenPath);
        }
    }

    public function testTrimsTrailingWhitespaceFromFileContents(): void
    {
        // Projected SA tokens are usually written without a trailing newline,
        // but be defensive — `kubectl create token` and similar tools add one.
        $tokenPath = $this->makeTempFile("jwt-from-file\n");
        try {
            $auth = new KeboolaServiceAccountAuth($tokenPath);

            self::assertSame(
                ['X-Kubernetes-Authorization' => 'Bearer jwt-from-file'],
                $auth->getAuthenticationHeaders(),
            );
        } finally {
            @unlink($tokenPath);
        }
    }

    public function testRereadsFileOnEveryCall(): void
    {
        $tokenPath = $this->makeTempFile('first-token');
        try {
            $auth = new KeboolaServiceAccountAuth($tokenPath);

            self::assertSame(
                ['X-Kubernetes-Authorization' => 'Bearer first-token'],
                $auth->getAuthenticationHeaders(),
            );

            // Simulate kubelet rotating the projected token file.
            file_put_contents($tokenPath, 'second-token');

            self::assertSame(
                ['X-Kubernetes-Authorization' => 'Bearer second-token'],
                $auth->getAuthenticationHeaders(),
            );
        } finally {
            @unlink($tokenPath);
        }
    }

    public function testThrowsWhenFileIsNotReadable(): void
    {
        $missingPath = sys_get_temp_dir() . '/does-not-exist-' . bin2hex(random_bytes(8));
        $auth = new KeboolaServiceAccountAuth($missingPath);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('is not readable');
        $this->expectExceptionMessage($missingPath);
        $auth->getAuthenticationHeaders();
    }

    public function testThrowsWhenFileIsEmpty(): void
    {
        $tokenPath = $this->makeTempFile('');
        try {
            $auth = new KeboolaServiceAccountAuth($tokenPath);

            $this->expectException(RuntimeException::class);
            $this->expectExceptionMessage('is empty');
            $this->expectExceptionMessage($tokenPath);
            $auth->getAuthenticationHeaders();
        } finally {
            @unlink($tokenPath);
        }
    }

    public function testThrowsWhenFileContainsOnlyWhitespace(): void
    {
        $tokenPath = $this->makeTempFile("\n\t ");
        try {
            $auth = new KeboolaServiceAccountAuth($tokenPath);

            $this->expectException(RuntimeException::class);
            $this->expectExceptionMessage('is empty');
            $auth->getAuthenticationHeaders();
        } finally {
            @unlink($tokenPath);
        }
    }

    /**
     * @return non-empty-string
     */
    private function makeTempFile(string $contents): string
    {
        $path = (string) tempnam(sys_get_temp_dir(), 'kbla-sa-auth-');
        self::assertNotSame('', $path);
        file_put_contents($path, $contents);
        return $path;
    }
}
