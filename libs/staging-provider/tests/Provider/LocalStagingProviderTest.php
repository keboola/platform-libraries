<?php

namespace Keboola\WorkspaceProvider\Tests\Provider;

use Keboola\WorkspaceProvider\Exception\StagingProviderException;
use Keboola\WorkspaceProvider\Provider\LocalStagingProvider;
use Keboola\WorkspaceProvider\Staging\LocalStaging;
use Keboola\WorkspaceProvider\Staging\Workspace\SnowflakeWorkspaceStaging;

use PHPUnit\Framework\TestCase;

class LocalStagingProviderTest extends TestCase
{
    public function testWorkspaceIdThrowsException()
    {
        $workspaceProvider = new LocalStagingProvider(function () {
            return new LocalStaging('/test');
        });

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Local staging provider does not support workspace ID.');

        $workspaceProvider->getWorkspaceId();
    }

    public function testCredentialsThrowsException()
    {
        $workspaceProvider = new LocalStagingProvider(function () {
            return new LocalStaging('/test');
        });

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Local staging provider does not support workspace credentials.');

        $workspaceProvider->getCredentials();
    }

    public function testPathIsReturnedForLocalStaging()
    {
        $localPath = '/data/in/test';

        $workspaceProvider = new LocalStagingProvider(function () use ($localPath) {
            return new LocalStaging($localPath);
        });

        self::assertSame($localPath, $workspaceProvider->getPath());
    }

    /**
     * @doesNotPerformAssertion
     */
    public function testCleanupDeletedWorkspaceStaging()
    {
        $workspaceProvider = new LocalStagingProvider(function () {
            return new LocalStaging('/data');
        });
        $workspaceProvider->cleanup();
    }

    public function testWorkspaceStagingIsCreatedLazily()
    {
        $callCounter = 0;

        $workspaceProvider = new LocalStagingProvider(function () use (&$callCounter) {
            $callCounter += 1;
            return new LocalStaging('/data');
        });

        self::assertSame(0, $callCounter, 'Check getter was not called after construction');

        $workspaceProvider->getPath();
        self::assertSame(1, $callCounter, 'Check getter was called once after getPath');

        $workspaceProvider->getPath();
        self::assertSame(1, $callCounter, 'Check getter is called at most once');
    }

    public function testStagingGetterResultTypeIsChecked()
    {
        $workspaceProvider = new LocalStagingProvider(function () {
            return new SnowflakeWorkspaceStaging([
                'connection' => [
                    'backend' => SnowflakeWorkspaceStaging::getType(),
                ],
            ]);
        });

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage(sprintf(
            'Staging getter must return instance of %s, %s returned.',
            LocalStaging::class,
            SnowflakeWorkspaceStaging::class
        ));

        $workspaceProvider->getPath();
    }
}
