<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\ExistingWorkspaceWithoutCredentialsProvider;
use Keboola\StorageApi\Workspaces;
use PHPUnit\Framework\TestCase;

class ExistingWorkspaceWithoutCredentialsProviderTest extends TestCase
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

        $workspaceProvider = new ExistingWorkspaceWithoutCredentialsProvider(
            $workspacesApiClient,
            $workspaceId,
        );

        self::assertSame($workspaceId, $workspaceProvider->getWorkspaceId());
        self::assertSame($backendSize, $workspaceProvider->getBackendSize());
        self::assertSame('snowflake', $workspaceProvider->getBackendType());
    }

    public function testCredentialsThrowsException(): void
    {
        $workspaceId = '123456';

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects(self::never())
            ->method('getWorkspace');

        $workspaceProvider = new ExistingWorkspaceWithoutCredentialsProvider(
            $workspacesApiClient,
            $workspaceId,
        );

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage(
            ExistingWorkspaceWithoutCredentialsProvider::class . ' does not support credentials',
        );
        $workspaceProvider->getCredentials();
    }

    public function testPathThrowsExceptionOnRemoteProvider(): void
    {
        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects(self::never())
            ->method('getWorkspace');

        $workspaceProvider = new ExistingWorkspaceWithoutCredentialsProvider(
            $workspacesApiClient,
            '123456',
        );

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage(
            ExistingWorkspaceWithoutCredentialsProvider::class . ' does not support path',
        );
        $workspaceProvider->getPath();
    }

    public function testCleanupDeletedWorkspaceStaging(): void
    {
        $workspaceId = '123456';

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects(self::once())
            ->method('deleteWorkspace')
            ->with($workspaceId, [], true);
        $workspacesApiClient
            ->expects(self::never())
            ->method('getWorkspace');

        $workspaceProvider = new ExistingWorkspaceWithoutCredentialsProvider(
            $workspacesApiClient,
            $workspaceId,
        );

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

        $workspaceProvider = new ExistingWorkspaceWithoutCredentialsProvider(
            $workspacesApiClient,
            $workspaceId,
        );

        // first call should create the workspace
        self::assertSame($backendSize, $workspaceProvider->getBackendSize());

        // second call should use cached workspace
        self::assertSame($backendSize, $workspaceProvider->getBackendSize());
    }
}
