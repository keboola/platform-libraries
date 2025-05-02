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

    public function testCredentialsThrowsExceptionByDefault(): void
    {
        $workspaceId = '123456';
        $workspaceData = [
            'id' => $workspaceId,
            'backendSize' => 'small',
            'connection' => [
                'backend' => 'snowflake',
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'schema' => 'some-schema',
                'user' => 'some-user',
            ],
        ];

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects(self::once())
            ->method('getWorkspace')
            ->with($workspaceId)
            ->willReturn($workspaceData);

        $workspaceProvider = new ExistingWorkspaceWithoutCredentialsProvider(
            $workspacesApiClient,
            $workspaceId,
        );

        // Initialize workspace by calling getBackendSize
        $workspaceProvider->getBackendSize();

        // Now try to get credentials - should throw exception
        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Credentials are not available');
        $workspaceProvider->getCredentials();
    }

    public function testCredentialsAvailableAfterReset(): void
    {
        $workspaceId = '123456';
        $workspaceData = [
            'id' => $workspaceId,
            'backendSize' => 'small',
            'connection' => [
                'backend' => 'snowflake',
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'schema' => 'some-schema',
                'user' => 'some-user',
            ],
        ];

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects(self::once())
            ->method('getWorkspace')
            ->with($workspaceId)
            ->willReturn($workspaceData);

        $workspacesApiClient
            ->expects(self::once())
            ->method('resetWorkspacePassword')
            ->with((int) $workspaceId)
            ->willReturn([
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'schema' => 'some-schema',
                'user' => 'some-user',
                'password' => 'new-password',
            ]);

        $workspaceProvider = new ExistingWorkspaceWithoutCredentialsProvider(
            $workspacesApiClient,
            $workspaceId,
        );

        // Initialize workspace by calling getBackendSize
        $workspaceProvider->getBackendSize();

        try {
            $workspaceProvider->getCredentials();
            self::fail('Exception should be thrown when accessing credentials before reset');
        } catch (StagingProviderException $e) {
            self::assertSame('Credentials are not available', $e->getMessage());
        }

        // Reset credentials
        $workspaceProvider->resetCredentials([]);

        // Now credentials should be available
        $credentials = $workspaceProvider->getCredentials();
        self::assertSame('new-password', $credentials['password']);
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
