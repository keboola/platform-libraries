<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider;

use Exception;
use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Workspace\Credentials\WorkspaceCredentialsProviderInterface;
use Keboola\StagingProvider\Workspace\ExistingWorkspaceProvider;
use Keboola\StorageApi\WorkspaceLoginType;
use Keboola\StorageApi\Workspaces;
use PHPUnit\Framework\TestCase;
use Throwable;

class ExistingWorkspaceProviderTest extends TestCase
{
    public function testWorkspaceGetters(): void
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
                'password' => 'test',
                'loginType' => WorkspaceLoginType::DEFAULT->value,
            ],
        ];

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::once())
            ->method('getWorkspace')
            ->with($workspaceId)
            ->willReturn($workspaceData);

        $credentialsProvider = $this->createMock(WorkspaceCredentialsProviderInterface::class);
        $credentialsProvider->expects(self::once())
            ->method('provideCredentials')
            ->willReturn([
                'password' => 'test',
            ]);

        $provider = new ExistingWorkspaceProvider($workspacesApiClient, $workspaceId, $credentialsProvider);

        self::assertSame($workspaceId, $provider->getWorkspaceId());
        self::assertSame('small', $provider->getBackendSize());
        self::assertSame('snowflake', $provider->getBackendType());
        self::assertSame([
            'host' => 'some-host',
            'warehouse' => 'some-warehouse',
            'database' => 'some-database',
            'schema' => 'some-schema',
            'user' => 'some-user',
            'password' => 'test',
            'privateKey' => null,
            'account' => 'some-host',
        ], $provider->getCredentials());
    }

    public function testGetWorkspaceIdDoesNotInitializeWorkspace(): void
    {
        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::never())->method(self::anything());

        $credentialsProvider = $this->createMock(WorkspaceCredentialsProviderInterface::class);
        $provider = new ExistingWorkspaceProvider($workspacesApiClient, '123456', $credentialsProvider);

        self::assertSame('123456', $provider->getWorkspaceId());
    }

    public function testPathThrowsException(): void
    {
        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::never())->method(self::anything());

        $credentialsProvider = $this->createMock(WorkspaceCredentialsProviderInterface::class);
        $provider = new ExistingWorkspaceProvider($workspacesApiClient, '123456', $credentialsProvider);

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage(ExistingWorkspaceProvider::class . ' does not support path');
        $provider->getPath();
    }

    public function testCleanupDeletesWorkspace(): void
    {
        $workspaceId = '123456';

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects(self::once())
            ->method('deleteWorkspace')
            ->with($workspaceId, [], true);

        $credentialsProvider = $this->createMock(WorkspaceCredentialsProviderInterface::class);
        $provider = new ExistingWorkspaceProvider($workspacesApiClient, $workspaceId, $credentialsProvider);
        $provider->cleanup();
    }

    public function testWorkspaceIsCachedInternally(): void
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
                'password' => 'test',
                'loginType' => WorkspaceLoginType::DEFAULT->value,
            ],
        ];

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects(self::once())
            ->method('getWorkspace')
            ->with($workspaceId)
            ->willReturn($workspaceData);

        $credentialsProvider = $this->createMock(WorkspaceCredentialsProviderInterface::class);
        $provider = new ExistingWorkspaceProvider($workspacesApiClient, $workspaceId, $credentialsProvider);

        // First call should fetch the workspace
        self::assertSame('small', $provider->getBackendSize());

        // Second call should use cached workspace
        self::assertSame('small', $provider->getBackendSize());
    }

    public function testInvalidWorkspaceId(): void
    {
        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects($this->once())
            ->method('getWorkspace')
            ->willThrowException(new Exception('Workspace not found'));

        $credentialsProvider = $this->createMock(WorkspaceCredentialsProviderInterface::class);
        $provider = new ExistingWorkspaceProvider($workspacesApiClient, 'invalid-id', $credentialsProvider);

        $this->expectException(Throwable::class);
        $this->expectExceptionMessage('Workspace not found');
        $provider->getBackendSize();
    }

    public function testInvalidWorkspaceData(): void
    {
        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->method('getWorkspace')
            ->with('123456')
            ->willReturn([
                'id' => '123456',
                // Missing required fields
            ]);

        $credentialsProvider = $this->createMock(WorkspaceCredentialsProviderInterface::class);
        $provider = new ExistingWorkspaceProvider($workspacesApiClient, '123456', $credentialsProvider);

        $this->expectException(StagingProviderException::class);
        $provider->getBackendSize();
    }

    public function testCredentialsProviderFailure(): void
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
                'password' => 'test',
                'loginType' => WorkspaceLoginType::DEFAULT->value,
            ],
        ];

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->method('getWorkspace')
            ->with($workspaceId)
            ->willReturn($workspaceData);

        $credentialsProvider = $this->createMock(WorkspaceCredentialsProviderInterface::class);
        $credentialsProvider
            ->method('provideCredentials')
            ->willThrowException(new Exception('Failed to provide credentials'));

        $provider = new ExistingWorkspaceProvider($workspacesApiClient, $workspaceId, $credentialsProvider);

        $this->expectException(Throwable::class);
        $this->expectExceptionMessage('Failed to provide credentials');
        $provider->getCredentials();
    }

    public function testCleanupWithNonExistentWorkspace(): void
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
                'password' => 'test',
                'loginType' => WorkspaceLoginType::DEFAULT->value,
            ],
        ];

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->method('getWorkspace')
            ->with($workspaceId)
            ->willReturn($workspaceData);

        $workspacesApiClient
            ->method('deleteWorkspace')
            ->with($workspaceId, [], true)
            ->willThrowException(new Exception('Workspace not found'));

        $credentialsProvider = $this->createMock(WorkspaceCredentialsProviderInterface::class);
        $provider = new ExistingWorkspaceProvider($workspacesApiClient, $workspaceId, $credentialsProvider);

        $this->expectException(Throwable::class);
        $this->expectExceptionMessage('Workspace not found');
        $provider->cleanup();
    }

    public function testResetCredentialsWithInvalidParams(): void
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
                'password' => 'test',
                'loginType' => WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR->value,
            ],
        ];

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->method('getWorkspace')
            ->with($workspaceId)
            ->willReturn($workspaceData);

        $credentialsProvider = $this->createMock(WorkspaceCredentialsProviderInterface::class);
        $credentialsProvider
            ->expects(self::once())
            ->method('provideCredentials')
            ->willThrowException(new StagingProviderException('Invalid parameters for key-pair authentication'));

        $provider = new ExistingWorkspaceProvider($workspacesApiClient, $workspaceId, $credentialsProvider);

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Invalid parameters for key-pair authentication');
        $provider->getCredentials();
    }
}
