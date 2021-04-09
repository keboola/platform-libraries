<?php

namespace Keboola\StagingProvider\Tests\Provider;

use Keboola\StorageApi\Workspaces;
use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\WorkspaceStagingProvider;
use Keboola\StagingProvider\Staging\LocalStaging;
use Keboola\StagingProvider\Staging\StagingInterface;
use Keboola\StagingProvider\Staging\Workspace\SnowflakeWorkspaceStaging;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStaging;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStagingInterface;
use PHPUnit\Framework\TestCase;

class WorkspaceStagingProviderTest extends TestCase
{
    public function testWorkspaceIdIsReturned()
    {
        $workspaceId = 'test';

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::never())->method(self::anything());

        $workspaceProvider = new WorkspaceStagingProvider($workspacesApiClient, function () use ($workspaceId) {
            return new SnowflakeWorkspaceStaging([
                'id' => $workspaceId,
                'connection' => [
                    'backend' => SnowflakeWorkspaceStaging::getType(),
                ],
            ]);
        });

        self::assertSame($workspaceId, $workspaceProvider->getWorkspaceId());
    }

    public function testCredentialsAreReturnedForWorkspaceStaging()
    {
        $credentials = [
            'host' => 'host',
            'warehouse' => 'warehouse',
            'database' => 'database',
            'schema' => 'schema',
            'user' => 'user',
            'password' => 'password',
        ];

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::never())->method(self::anything());

        $workspaceProvider = new WorkspaceStagingProvider($workspacesApiClient, function () use ($credentials) {
            return new SnowflakeWorkspaceStaging([
                'connection' => $credentials + [
                        'backend' => SnowflakeWorkspaceStaging::getType(),
                    ],
            ]);
        });

        self::assertSame($credentials, $workspaceProvider->getCredentials());
    }

    public function testPathThrowsExceptionOnRemoteProvider()
    {
        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::never())->method(self::anything());

        $workspaceProvider = new WorkspaceStagingProvider($workspacesApiClient, function () {
            return new SnowflakeWorkspaceStaging([
                'connection' => [
                    'backend' => SnowflakeWorkspaceStaging::getType(),
                ]
            ]);
        });

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Workspace staging provider does not support path.');

        $workspaceProvider->getPath();
    }

    public function testCleanupDeletedWorkspaceStaging()
    {
        $workspaceId = '1';

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::once())->method('deleteWorkspace')->with($workspaceId, ['async' => true]);

        $workspaceProvider = new WorkspaceStagingProvider($workspacesApiClient, function () use ($workspaceId) {
            return new SnowflakeWorkspaceStaging([
                'id' => $workspaceId,
                'connection' => [
                    'backend' => SnowflakeWorkspaceStaging::getType(),
                ],
            ]);
        });
        $workspaceProvider->cleanup();
    }

    public function testWorkspaceStagingIsCreatedLazily()
    {
        $callCounter = 0;

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::never())->method(self::anything());

        $workspaceProvider = new WorkspaceStagingProvider($workspacesApiClient, function () use (&$callCounter) {
            $callCounter += 1;
            return new SnowflakeWorkspaceStaging([
                'id' => 'test',
                'connection' => [
                    'backend' => SnowflakeWorkspaceStaging::getType(),
                    'host' => 'host',
                    'warehouse' => 'warehouse',
                    'database' => 'database',
                    'schema' => 'schema',
                    'user' => 'user',
                    'password' => 'password',
                ],
            ]);
        });

        self::assertSame(0, $callCounter, 'Check getter was not called after construction');

        $workspaceProvider->getWorkspaceId();
        self::assertSame(1, $callCounter, 'Check getter was called once after getWorkspaceId');

        $workspaceProvider->getWorkspaceId();
        self::assertSame(1, $callCounter, 'Check getter is called at most once within getWorkspaceId');

        $workspaceProvider->getCredentials();
        self::assertSame(1, $callCounter, 'Check getter is called at most once within any method');
    }

    public function testStagingGetterResultTypeIsChecked()
    {
        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::never())->method(self::anything());

        $workspaceProvider = new WorkspaceStagingProvider($workspacesApiClient, function () {
            return new LocalStaging('/data');
        });

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage(sprintf(
            'Staging getter must return instance of %s, %s returned.',
            WorkspaceStagingInterface::class,
            LocalStaging::class
        ));

        $workspaceProvider->getWorkspaceId();
    }
}
