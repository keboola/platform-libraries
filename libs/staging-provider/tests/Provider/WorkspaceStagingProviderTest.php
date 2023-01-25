<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\WorkspaceStagingProvider;
use Keboola\StagingProvider\Staging\Workspace\SnowflakeWorkspaceStaging;
use Keboola\StorageApi\Workspaces;
use PHPUnit\Framework\TestCase;

class WorkspaceStagingProviderTest extends TestCase
{
    public function testWorkspaceIdIsReturned(): void
    {
        $workspaceId = 'test';
        $backendSize = 'large';

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::never())->method(self::anything());

        $workspaceProvider = new WorkspaceStagingProvider(
            $workspacesApiClient,
            function () use ($workspaceId, $backendSize) {
                return new SnowflakeWorkspaceStaging([
                    'id' => $workspaceId,
                    'backendSize' => $backendSize,
                    'connection' => [
                        'backend' => SnowflakeWorkspaceStaging::getType(),
                    ],
                ]);
            }
        );

        self::assertSame($workspaceId, $workspaceProvider->getWorkspaceId());
        self::assertSame($backendSize, $workspaceProvider->getBackendSize());
    }

    public function testCredentialsAreReturnedForWorkspaceStaging(): void
    {
        $credentials = [
            'host' => 'host',
            'warehouse' => 'warehouse',
            'database' => 'database',
            'schema' => 'schema',
            'user' => 'user',
            'password' => 'password',
            'account' => 'host',
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

    public function testPathThrowsExceptionOnRemoteProvider(): void
    {
        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::never())->method(self::anything());

        $workspaceProvider = new WorkspaceStagingProvider($workspacesApiClient, function () {
            return new SnowflakeWorkspaceStaging([
                'connection' => [
                    'backend' => SnowflakeWorkspaceStaging::getType(),
                ],
            ]);
        });

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Workspace staging provider does not support path.');

        $workspaceProvider->getPath();
    }

    public function testCleanupDeletedWorkspaceStaging(): void
    {
        $workspaceId = '1';

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::once())->method('deleteWorkspace')->with($workspaceId, [], true);

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

    public function testWorkspaceStagingIsCreatedLazily(): void
    {
        $callCounter = 0;

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::never())->method(self::anything());

        $workspaceProvider = new WorkspaceStagingProvider(
            $workspacesApiClient,
            function () use (&$callCounter): SnowflakeWorkspaceStaging {
                $callCounter += 1;
                return new SnowflakeWorkspaceStaging([
                    'id' => 'test',
                    'backendSize' => 'medium',
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
            }
        );

        /* intentionally assertEquals instead of assertSame because otherwise phpstan is confused and things
            that $callCounter is constant === 0 */
        self::assertEquals(0, $callCounter, 'Check getter was not called after construction');

        $workspaceProvider->getWorkspaceId();
        self::assertSame(1, $callCounter, 'Check getter was called once after getWorkspaceId');

        $workspaceProvider->getWorkspaceId();
        self::assertSame(1, $callCounter, 'Check getter is called at most once within getWorkspaceId');

        $workspaceProvider->getCredentials();
        self::assertSame(1, $callCounter, 'Check getter is called at most once within any method');

        $workspaceProvider->getBackendSize();
        self::assertSame(1, $callCounter, 'Check getter is called at most once within any method');
    }
}
