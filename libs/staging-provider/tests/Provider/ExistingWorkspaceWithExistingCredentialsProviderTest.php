<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\Credentials\DatabaseWorkspaceCredentials;
use Keboola\StagingProvider\Provider\ExistingWorkspaceWithExistingCredentialsProvider;
use Keboola\StorageApi\Workspaces;
use PHPUnit\Framework\TestCase;

class ExistingWorkspaceWithExistingCredentialsProviderTest extends TestCase
{
    public function testWorkspaceGetters(): void
    {
        $workspaceId = '123456';
        $backendSize = 'large';

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects(self::once())
            ->method('getWorkspace')
            ->with($workspaceId)
            ->willReturn([
                'id' => $workspaceId,
                'backendSize' => $backendSize,
                'connection' => [
                    'backend' => 'snowflake',
                    'host' => 'some-host',
                    'warehouse' => 'some-warehouse',
                    'database' => 'some-database',
                    'schema' => 'some-schema',
                    'user' => 'some-user',
                ],
            ]);

        $workspaceProvider = new ExistingWorkspaceWithExistingCredentialsProvider(
            $workspacesApiClient,
            $workspaceId,
            DatabaseWorkspaceCredentials::fromPasswordResetArray(['password' => 'test']),
        );

        self::assertSame($workspaceId, $workspaceProvider->getWorkspaceId());
        self::assertSame($backendSize, $workspaceProvider->getBackendSize());
        self::assertSame('snowflake', $workspaceProvider->getBackendType());
        self::assertSame(
            [
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'schema' => 'some-schema',
                'user' => 'some-user',
                'password' => 'test',
                'account' => 'some-host',
            ],
            $workspaceProvider->getCredentials(),
        );
    }

    public function testPathThrowsException(): void
    {
        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects(self::never())
            ->method('getWorkspace');

        $workspaceProvider = new ExistingWorkspaceWithExistingCredentialsProvider(
            $workspacesApiClient,
            '123456',
            DatabaseWorkspaceCredentials::fromPasswordResetArray(['password' => 'test']),
        );

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage(
            ExistingWorkspaceWithExistingCredentialsProvider::class . ' does not support path',
        );
        $workspaceProvider->getPath();
    }

    public function testCleanupDeletedWorkspaceStaging(): void
    {
        $workspaceId = '1';

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects(self::once())
            ->method('deleteWorkspace')
            ->with($workspaceId, [], true);
        $workspacesApiClient
            ->expects(self::never())
            ->method('getWorkspace');

        $workspaceProvider = new ExistingWorkspaceWithExistingCredentialsProvider(
            $workspacesApiClient,
            $workspaceId,
            DatabaseWorkspaceCredentials::fromPasswordResetArray(['password' => 'test']),
        );
        self::assertSame($workspaceId, $workspaceProvider->getWorkspaceId());
        $workspaceProvider->cleanup();
    }

    public function testWorkspaceStagingIsCreatedLazilyAndCached(): void
    {
        $workspaceId = '123456';
        $backendSize = 'large';

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects(self::once())
            ->method('getWorkspace')
            ->with($workspaceId)
            ->willReturn([
                'id' => $workspaceId,
                'backendSize' => $backendSize,
                'connection' => [
                    'backend' => 'snowflake',
                    'host' => 'some-host',
                    'warehouse' => 'some-warehouse',
                    'database' => 'some-database',
                    'schema' => 'some-schema',
                    'user' => 'some-user',
                ],
            ]);

        $workspaceProvider = new ExistingWorkspaceWithExistingCredentialsProvider(
            $workspacesApiClient,
            $workspaceId,
            DatabaseWorkspaceCredentials::fromPasswordResetArray(['password' => 'test']),
        );

        // first call should create the workspace
        self::assertSame($backendSize, $workspaceProvider->getBackendSize());

        // second call should use cached workspace
        self::assertSame($backendSize, $workspaceProvider->getBackendSize());
    }
}
