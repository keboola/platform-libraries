<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\AuthBridge;

use Keboola\ApiBundle\AuthBridge\Exception\ResolverUnavailableException;
use Keboola\ApiBundle\AuthBridge\KubernetesServiceAccountTokenProvider;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(KubernetesServiceAccountTokenProvider::class)]
class KubernetesServiceAccountTokenProviderTest extends TestCase
{
    private string $tokenPath;

    protected function setUp(): void
    {
        parent::setUp();
        $this->tokenPath = sys_get_temp_dir() . '/' . uniqid('sa-token-', true);
    }

    protected function tearDown(): void
    {
        if (file_exists($this->tokenPath)) {
            unlink($this->tokenPath);
        }
        parent::tearDown();
    }

    public function testGetTokenReturnsTrimmedContent(): void
    {
        file_put_contents($this->tokenPath, "  jwt-token-value \n");

        $provider = new KubernetesServiceAccountTokenProvider($this->tokenPath);

        self::assertSame('jwt-token-value', $provider->getToken());
    }

    public function testGetTokenIsReadOnEveryCallSupportingRotation(): void
    {
        file_put_contents($this->tokenPath, 'token-a');
        $provider = new KubernetesServiceAccountTokenProvider($this->tokenPath);

        self::assertSame('token-a', $provider->getToken());

        file_put_contents($this->tokenPath, 'token-b');

        self::assertSame('token-b', $provider->getToken());
    }

    public function testGetTokenThrowsWhenFileIsMissing(): void
    {
        $missingPath = $this->tokenPath . '-missing';
        $provider = new KubernetesServiceAccountTokenProvider($missingPath);

        self::expectException(ResolverUnavailableException::class);
        self::expectExceptionMessage($missingPath);

        $provider->getToken();
    }

    public function testGetTokenThrowsWhenFileIsEmpty(): void
    {
        file_put_contents($this->tokenPath, '');
        $provider = new KubernetesServiceAccountTokenProvider($this->tokenPath);

        self::expectException(ResolverUnavailableException::class);
        self::expectExceptionMessage('empty');

        $provider->getToken();
    }

    public function testGetTokenThrowsWhenFileContainsOnlyWhitespace(): void
    {
        file_put_contents($this->tokenPath, "  \n\t ");
        $provider = new KubernetesServiceAccountTokenProvider($this->tokenPath);

        self::expectException(ResolverUnavailableException::class);

        $provider->getToken();
    }
}
