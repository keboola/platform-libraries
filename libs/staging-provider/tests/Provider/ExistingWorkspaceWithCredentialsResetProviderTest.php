<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\ExistingWorkspaceWithCredentialsResetProvider;
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

        $workspaceProvider = new ExistingWorkspaceWithCredentialsResetProvider(
            $workspacesApiClient,
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
                'account' => 'some-host',
            ],
            $workspaceProvider->getCredentials(),
        );
    }

    public function testKeyPairAuthNotSupported(): void
    {
        $workspaceId = '123456';

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects(self::once())
            ->method('getWorkspace')
            ->with($workspaceId)
            ->willReturn([
                'id' => $workspaceId,
                'backendSize' => 'large',
                'connection' => [
                    'backend' => 'snowflake',
                    'host' => 'some-host',
                    'warehouse' => 'some-warehouse',
                    'database' => 'some-database',
                    'schema' => 'some-schema',
                    'user' => 'some-user',
                    'loginType' => WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR->value,
                ],
            ]);

        $workspacesApiClient
            ->expects(self::never())
            ->method('resetWorkspacePassword');

        $workspaceProvider = new ExistingWorkspaceWithCredentialsResetProvider(
            $workspacesApiClient,
            $workspaceId,
        );

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Credentials reset for key-pair auth is not supported yet');
        $workspaceProvider->getCredentials();
    }

    public function testPathThrowsException(): void
    {
        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects(self::never())
            ->method('getWorkspace');

        $workspaceProvider = new ExistingWorkspaceWithCredentialsResetProvider(
            $workspacesApiClient,
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

        $workspaceProvider = new ExistingWorkspaceWithCredentialsResetProvider(
            $workspacesApiClient,
            $workspaceId,
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

        $workspaceProvider = new ExistingWorkspaceWithCredentialsResetProvider(
            $workspacesApiClient,
            $workspaceId,
        );

        // first call should create the workspace
        self::assertSame($backendSize, $workspaceProvider->getBackendSize());

        // second call should use cached workspace
        self::assertSame($backendSize, $workspaceProvider->getBackendSize());
    }
}
