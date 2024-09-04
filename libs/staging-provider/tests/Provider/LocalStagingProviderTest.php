<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\LocalStagingProvider;

use PHPUnit\Framework\TestCase;

class LocalStagingProviderTest extends TestCase
{
    public function testWorkspaceIdThrowsException(): void
    {
        $workspaceProvider = new LocalStagingProvider('/test');

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Local staging provider does not support workspace ID.');

        $workspaceProvider->getWorkspaceId();
    }

    public function testCredentialsThrowsException(): void
    {
        $workspaceProvider = new LocalStagingProvider('/test');

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Local staging provider does not support workspace credentials.');

        $workspaceProvider->getCredentials();
    }

    public function testPathIsReturnedForLocalStaging(): void
    {
        $localPath = '/data/in/test';
        $workspaceProvider = new LocalStagingProvider($localPath);

        self::assertSame($localPath, $workspaceProvider->getPath());
    }
    public function testCleanupDeletedWorkspaceStaging(): void
    {
        $workspaceProvider = new LocalStagingProvider('/test');
        $workspaceProvider->cleanup();
        $this->expectNotToPerformAssertions();
    }
}
