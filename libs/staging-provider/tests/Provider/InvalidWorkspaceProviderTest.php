<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\InvalidWorkspaceProvider;
use PHPUnit\Framework\TestCase;

class InvalidWorkspaceProviderTest extends TestCase
{
    public function testWorkspaceGetters(): void
    {
        $workspaceProvider = new InvalidWorkspaceProvider('test-workspace');

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('No workspace provider is available for staging type "test-workspace"');
        $workspaceProvider->getWorkspaceId();
    }

    public function testCredentialsThrowsException(): void
    {
        $workspaceProvider = new InvalidWorkspaceProvider('test-workspace');

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('No workspace provider is available for staging type "test-workspace"');
        $workspaceProvider->getCredentials();
    }

    public function testPathThrowsException(): void
    {
        $workspaceProvider = new InvalidWorkspaceProvider('test-workspace');

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('No workspace provider is available for staging type "test-workspace"');
        $workspaceProvider->getPath();
    }

    public function testBackendSizeThrowsException(): void
    {
        $workspaceProvider = new InvalidWorkspaceProvider('test-workspace');

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('No workspace provider is available for staging type "test-workspace"');
        $workspaceProvider->getBackendSize();
    }

    public function testBackendTypeThrowsException(): void
    {
        $workspaceProvider = new InvalidWorkspaceProvider('test-workspace');

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('No workspace provider is available for staging type "test-workspace"');
        $workspaceProvider->getBackendType();
    }

    public function testCleanupDoesNothing(): void
    {
        $workspaceProvider = new InvalidWorkspaceProvider('test-workspace');
        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('No workspace provider is available for staging type "test-workspace"');
        $workspaceProvider->cleanup();
    }

    public function testGetWorkspaceId(): void
    {
        $workspaceProvider = new InvalidWorkspaceProvider('test-workspace');
        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('No workspace provider is available for staging type "test-workspace"');
        $workspaceProvider->getWorkspaceId();
    }

    public function testGetCredentials(): void
    {
        $workspaceProvider = new InvalidWorkspaceProvider('test-workspace');
        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('No workspace provider is available for staging type "test-workspace"');
        $workspaceProvider->getCredentials();
    }
}
