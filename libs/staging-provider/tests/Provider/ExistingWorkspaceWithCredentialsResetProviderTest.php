<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\ExistingWorkspaceWithCredentialsResetProvider;
use Keboola\StagingProvider\Provider\SnowflakeKeypairGenerator;
use Keboola\StorageApi\WorkspaceLoginType;
use Keboola\StorageApi\Workspaces;
use PHPUnit\Framework\TestCase;

class ExistingWorkspaceWithCredentialsResetProviderTest extends TestCase
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
                    'loginType' => WorkspaceLoginType::SNOWFLAKE_LEGACY_SERVICE_PASSWORD->value,
                ],
            ]);

        $workspacesApiClient
            ->expects(self::once())
            ->method('resetWorkspacePassword')
            ->with($workspaceId)
            ->willReturn([
                'password' => 'new-password',
            ]);

        $snowflakeKeypairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);
        $snowflakeKeypairGenerator->expects(self::never())->method(self::anything());

        $workspaceProvider = new ExistingWorkspaceWithCredentialsResetProvider(
            $workspacesApiClient,
            $snowflakeKeypairGenerator,
            $workspaceId,
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
                'password' => 'new-password',
                'privateKey' => null,
                'account' => 'some-host',
            ],
            $workspaceProvider->getCredentials(),
        );
    }

    public function testKeyPairAuthNotSupported(): void
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
            ->with((int) $workspaceId)
            ->willReturn($workspaceData);

        $snowflakeKeypairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);
        $snowflakeKeypairGenerator->expects(self::never())->method(self::anything());

        $workspaceProvider = new ExistingWorkspaceWithCredentialsResetProvider(
            $workspacesApiClient,
            $snowflakeKeypairGenerator,
            $workspaceId,
        );

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Invalid parameters for key-pair authentication');
        $workspaceProvider->resetCredentials([]);
    }

    public function testPathThrowsException(): void
    {
        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects(self::never())
            ->method('getWorkspace');

        $snowflakeKeypairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);
        $snowflakeKeypairGenerator->expects(self::never())->method(self::anything());

        $workspaceProvider = new ExistingWorkspaceWithCredentialsResetProvider(
            $workspacesApiClient,
            $snowflakeKeypairGenerator,
            '123456',
        );

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage(
            ExistingWorkspaceWithCredentialsResetProvider::class . ' does not support path',
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

        $snowflakeKeypairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);
        $snowflakeKeypairGenerator->expects(self::never())->method(self::anything());

        $workspaceProvider = new ExistingWorkspaceWithCredentialsResetProvider(
            $workspacesApiClient,
            $snowflakeKeypairGenerator,
            $workspaceId,
        );
        self::assertSame($workspaceId, $workspaceProvider->getWorkspaceId());
        $workspaceProvider->cleanup();
    }

    public function testWorkspaceStagingIsCreatedLazilyAndCached(): void
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
            ->with((int) $workspaceId)
            ->willReturn($workspaceData);

        $workspacesApiClient
            ->expects(self::exactly(2))
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

        $snowflakeKeypairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);
        $snowflakeKeypairGenerator->expects(self::never())->method(self::anything());

        $workspaceProvider = new ExistingWorkspaceWithCredentialsResetProvider(
            $workspacesApiClient,
            $snowflakeKeypairGenerator,
            $workspaceId,
        );

        // Get initial credentials
        $credentials = $workspaceProvider->getCredentials();
        self::assertArrayHasKey('password', $credentials);

        // Reset credentials
        $workspaceProvider->resetCredentials([]);

        // Get updated credentials
        $credentials = $workspaceProvider->getCredentials();
        self::assertSame('new-password', $credentials['password']);
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
            ->with((int) $workspaceId)
            ->willReturn($workspaceData);

        $workspacesApiClient
            ->expects(self::exactly(2))
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

        $snowflakeKeypairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);
        $snowflakeKeypairGenerator->expects(self::never())->method(self::anything());

        $workspaceProvider = new ExistingWorkspaceWithCredentialsResetProvider(
            $workspacesApiClient,
            $snowflakeKeypairGenerator,
            $workspaceId,
        );

        // Get initial credentials
        $credentials = $workspaceProvider->getCredentials();
        self::assertArrayHasKey('password', $credentials);

        // Reset credentials
        $workspaceProvider->resetCredentials([]);

        // Get updated credentials
        $credentials = $workspaceProvider->getCredentials();
        self::assertSame('new-password', $credentials['password']);
    }
}
