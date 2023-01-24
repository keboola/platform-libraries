<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\LocalStagingProvider;
use Keboola\StagingProvider\Staging\LocalStaging;

use PHPUnit\Framework\TestCase;

class LocalStagingProviderTest extends TestCase
{
    public function testWorkspaceIdThrowsException(): void
    {
        $workspaceProvider = new LocalStagingProvider(function () {
            return new LocalStaging('/test');
        });

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Local staging provider does not support workspace ID.');

        $workspaceProvider->getWorkspaceId();
    }

    public function testCredentialsThrowsException(): void
    {
        $workspaceProvider = new LocalStagingProvider(function () {
            return new LocalStaging('/test');
        });

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Local staging provider does not support workspace credentials.');

        $workspaceProvider->getCredentials();
    }

    public function testPathIsReturnedForLocalStaging(): void
    {
        $localPath = '/data/in/test';

        $workspaceProvider = new LocalStagingProvider(function () use ($localPath) {
            return new LocalStaging($localPath);
        });

        self::assertSame($localPath, $workspaceProvider->getPath());
    }
    public function testCleanupDeletedWorkspaceStaging(): void
    {
        $workspaceProvider = new LocalStagingProvider(function () {
            return new LocalStaging('/data');
        });
        $workspaceProvider->cleanup();
        $this->expectNotToPerformAssertions();
    }

    public function testWorkspaceStagingIsCreatedLazily(): void
    {
        $callCounter = 0;

        $workspaceProvider = new LocalStagingProvider(function () use (&$callCounter) {
            $callCounter += 1;
            return new LocalStaging('/data');
        });

        /* intentionally assertEquals instead of assertSame because otherwise phpstan is confused and things
            that $callCounter is constant === 0 */
        self::assertEquals(0, $callCounter, 'Check getter was not called after construction');

        $workspaceProvider->getPath();
        self::assertSame(1, $callCounter, 'Check getter was called once after getPath');

        $workspaceProvider->getPath();
        self::assertSame(1, $callCounter, 'Check getter is called at most once');
    }
}
