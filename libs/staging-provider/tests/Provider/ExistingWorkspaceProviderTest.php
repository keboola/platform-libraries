<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider;

use Exception;
use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\Credentials\ExistingWorkspaceCredentialsProviderInterface;
use Keboola\StagingProvider\Provider\ExistingWorkspaceProvider;
use Keboola\StorageApi\WorkspaceLoginType;
use Keboola\StorageApi\Workspaces;
use PHPUnit\Framework\TestCase;
use Throwable;

class ExistingWorkspaceProviderTest extends TestCase
{
    public function testGetWorkspaceIdDoesNotInitializeWorkspace(): void
    {
        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::never())->method(self::anything());

        $credentialsProvider = $this->createMock(ExistingWorkspaceCredentialsProviderInterface::class);
        $provider = new ExistingWorkspaceProvider($workspacesApiClient, '123456', $credentialsProvider);

        self::assertSame('123456', $provider->getWorkspaceId());
    }

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

        $credentialsProvider = $this->createMock(ExistingWorkspaceCredentialsProviderInterface::class);
        $provider = new ExistingWorkspaceProvider($workspacesApiClient, $workspaceId, $credentialsProvider);

        self::assertSame('small', $provider->getBackendSize());
        self::assertSame('snowflake', $provider->getBackendType());
    }

    public function testPathThrowsException(): void
    {
        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::never())->method(self::anything());

        $credentialsProvider = $this->createMock(ExistingWorkspaceCredentialsProviderInterface::class);
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

        $credentialsProvider = $this->createMock(ExistingWorkspaceCredentialsProviderInterface::class);
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

        $credentialsProvider = $this->createMock(ExistingWorkspaceCredentialsProviderInterface::class);
        $provider = new ExistingWorkspaceProvider($workspacesApiClient, $workspaceId, $credentialsProvider);

        // First call should fetch the workspace
        self::assertSame('small', $provider->getBackendSize());

        // Second call should use cached workspace
        self::assertSame('small', $provider->getBackendSize());
    }

    public function testResetCredentialsWithPassword(): void
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

        $workspacesApiClient
            ->expects(self::once())
            ->method('resetWorkspacePassword')
            ->with($workspaceId)
            ->willReturn([
                'password' => 'new-password',
            ]);

        $credentialsProvider = $this->createMock(ExistingWorkspaceCredentialsProviderInterface::class);
        $provider = new ExistingWorkspaceProvider($workspacesApiClient, $workspaceId, $credentialsProvider);
        $provider->resetCredentials([]);
    }

    public function testResetCredentialsWithKeyPair(): void
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
            ->expects(self::once())
            ->method('getWorkspace')
            ->with($workspaceId)
            ->willReturn($workspaceData);

        $credentialsProvider = $this->createMock(ExistingWorkspaceCredentialsProviderInterface::class);
        $provider = new ExistingWorkspaceProvider($workspacesApiClient, $workspaceId, $credentialsProvider);

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Invalid parameters for key-pair authentication');
        $provider->resetCredentials([]);
    }

    public function testResetCredentialsWithKeyPairAndPublicKey(): void
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
            ->expects(self::once())
            ->method('getWorkspace')
            ->with($workspaceId)
            ->willReturn($workspaceData);

        $credentialsProvider = $this->createMock(ExistingWorkspaceCredentialsProviderInterface::class);
        $provider = new ExistingWorkspaceProvider($workspacesApiClient, $workspaceId, $credentialsProvider);
        $provider->resetCredentials(['publicKey' => 'public-key']);
    }

    public function testInvalidWorkspaceId(): void
    {
        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects($this->once())
            ->method('getWorkspace')
            ->willThrowException(new Exception('Workspace not found'));

        $credentialsProvider = $this->createMock(ExistingWorkspaceCredentialsProviderInterface::class);
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

        $credentialsProvider = $this->createMock(ExistingWorkspaceCredentialsProviderInterface::class);
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

        $credentialsProvider = $this->createMock(ExistingWorkspaceCredentialsProviderInterface::class);
        $credentialsProvider
            ->method('provideCredentials')
            ->willThrowException(new Exception('Failed to provide credentials'));

        $provider = new ExistingWorkspaceProvider($workspacesApiClient, $workspaceId, $credentialsProvider);

        $this->expectException(Throwable::class);
        $this->expectExceptionMessage('Failed to provide credentials');
        $provider->getCredentials();
    }

    public function testConcurrentAccess(): void
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
            ->expects($this->once())
            ->method('getWorkspace')
            ->willReturn($workspaceData);

        $credentialsProvider = $this->createMock(ExistingWorkspaceCredentialsProviderInterface::class);
        $credentialsProvider
            ->expects($this->exactly(1))
            ->method('provideCredentials')
            ->willReturnCallback(function ($provider, $workspace) {
                $workspace->setCredentialsFromData([
                    'backend' => 'snowflake',
                    'host' => 'some-host',
                    'warehouse' => 'some-warehouse',
                    'database' => 'some-database',
                    'schema' => 'some-schema',
                    'user' => 'some-user',
                    'password' => 'test',
                ]);
            });

        $provider = new ExistingWorkspaceProvider($workspacesApiClient, $workspaceId, $credentialsProvider);

        // Simulate concurrent access
        $backendSize = $provider->getBackendSize();
        $backendType = $provider->getBackendType();
        $provider->getCredentials();

        self::assertSame('small', $backendSize);
        self::assertSame('snowflake', $backendType);
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

        $credentialsProvider = $this->createMock(ExistingWorkspaceCredentialsProviderInterface::class);
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

        $credentialsProvider = $this->createMock(ExistingWorkspaceCredentialsProviderInterface::class);
        $provider = new ExistingWorkspaceProvider($workspacesApiClient, $workspaceId, $credentialsProvider);

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Invalid parameters for key-pair authentication');
        $provider->resetCredentials(['invalid' => 'param']);
    }
}
