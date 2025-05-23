<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Workspace;

use Keboola\KeyGenerator\PemKeyCertificatePair;
use Keboola\StagingProvider\Exception\StagingNotSupportedByProjectException;
use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Staging\StagingType;
use Keboola\StagingProvider\Workspace\Configuration\NetworkPolicy;
use Keboola\StagingProvider\Workspace\Configuration\NewWorkspaceConfig;
use Keboola\StagingProvider\Workspace\SnowflakeKeypairGenerator;
use Keboola\StagingProvider\Workspace\Workspace;
use Keboola\StagingProvider\Workspace\WorkspaceInterface;
use Keboola\StagingProvider\Workspace\WorkspaceProvider;
use Keboola\StagingProvider\Workspace\WorkspaceWithCredentials;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\WorkspaceLoginType;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApi\Workspaces\ResetCredentialsRequest;
use Keboola\StorageApiBranch\StorageApiToken;
use PHPUnit\Framework\TestCase;

class WorkspaceProviderTest extends TestCase
{
    public function testCreateNewWorkspace(): void
    {
        $workspaceId = '123456';
        $backendSize = 'large';

        $componentsApiClient = $this->createMock(Components::class);
        $componentsApiClient
            ->expects(self::once())
            ->method('createConfigurationWorkspace')
            ->with(
                'test-component',
                'test-config',
                [
                    'backend' => 'snowflake',
                    'backendSize' => $backendSize,
                    'networkPolicy' => 'user',
                    'loginType' => WorkspaceLoginType::DEFAULT,
                ],
                true,
            )
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
                    'password' => 'secret',
                ],
            ]);

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::never())->method(self::anything());

        $snowflakeKeypairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);
        $snowflakeKeypairGenerator->expects(self::never())->method(self::anything());

        $storageApiToken = $this->createMock(StorageApiToken::class);
        $storageApiToken
            ->expects(self::once())
            ->method('getTokenInfo')
            ->willReturn([
                'owner' => [
                    'hasSnowflake' => true,
                ],
            ]);

        $config = new NewWorkspaceConfig(
            StagingType::WorkspaceSnowflake,
            'test-component',
            'test-config',
            $backendSize,
            null,
            NetworkPolicy::USER,
            null,
        );

        $workspaceProvider = new WorkspaceProvider(
            $workspacesApiClient,
            $componentsApiClient,
            $snowflakeKeypairGenerator,
        );

        $workspace = $workspaceProvider->createNewWorkspace($storageApiToken, $config);

        self::assertSame($workspaceId, $workspace->getWorkspaceId());
        self::assertSame($backendSize, $workspace->getBackendSize());
        self::assertSame('snowflake', $workspace->getBackendType());
        self::assertSame(
            [
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'schema' => 'some-schema',
                'user' => 'some-user',
                'password' => 'secret',
                'privateKey' => null,
                'account' => 'some-host',
            ],
            $workspace->getCredentials(),
        );
    }

    public function testCreateNewWorkspaceWithoutConfigId(): void
    {
        $workspaceId = '123456';
        $backendSize = 'large';

        $componentsApiClient = $this->createMock(Components::class);
        $componentsApiClient
            ->expects(self::never())
            ->method('createConfigurationWorkspace');

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects(self::once())
            ->method('createWorkspace')
            ->with(
                [
                    'backend' => 'snowflake',
                    'backendSize' => $backendSize,
                    'readOnlyStorageAccess' => true,
                    'networkPolicy' => 'system',
                    'loginType' => WorkspaceLoginType::DEFAULT,
                ],
                true,
            )
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
                    'password' => 'secret',
                ],
            ]);

        $snowflakeKeypairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);
        $snowflakeKeypairGenerator->expects(self::never())->method(self::anything());

        $storageApiToken = $this->createMock(StorageApiToken::class);
        $storageApiToken
            ->expects(self::once())
            ->method('getTokenInfo')
            ->willReturn([
                'owner' => [
                    'hasSnowflake' => true,
                ],
            ]);

        $config = new NewWorkspaceConfig(
            StagingType::WorkspaceSnowflake,
            'test-component',
            null,
            $backendSize,
            true,
            NetworkPolicy::SYSTEM,
            null,
        );

        $workspaceProvider = new WorkspaceProvider(
            $workspacesApiClient,
            $componentsApiClient,
            $snowflakeKeypairGenerator,
        );

        $workspace = $workspaceProvider->createNewWorkspace($storageApiToken, $config);

        self::assertSame($workspaceId, $workspace->getWorkspaceId());
        self::assertSame($backendSize, $workspace->getBackendSize());
        self::assertSame('snowflake', $workspace->getBackendType());
        self::assertSame(
            [
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'schema' => 'some-schema',
                'user' => 'some-user',
                'password' => 'secret',
                'privateKey' => null,
                'account' => 'some-host',
            ],
            $workspace->getCredentials(),
        );
    }

    public function testCreateNewWorkspaceWithKeyPairLogin(): void
    {
        $workspaceId = '123456';
        $backendSize = 'large';
        $publicKey = 'public-key';
        $privateKey = 'private-key';

        $componentsApiClient = $this->createMock(Components::class);
        $componentsApiClient
            ->expects(self::once())
            ->method('createConfigurationWorkspace')
            ->with(
                'test-component',
                'test-config',
                [
                    'backend' => 'snowflake',
                    'backendSize' => $backendSize,
                    'networkPolicy' => 'system',
                    'loginType' => WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR,
                    'publicKey' => $publicKey,
                ],
                true,
            )
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

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::never())->method(self::anything());

        $snowflakeKeypairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);
        $snowflakeKeypairGenerator
            ->expects(self::once())
            ->method('generateKeyPair')
            ->willReturn(new PemKeyCertificatePair(
                privateKey: $privateKey,
                publicKey: $publicKey,
            ));

        $storageApiToken = $this->createMock(StorageApiToken::class);
        $storageApiToken
            ->expects(self::once())
            ->method('getTokenInfo')
            ->willReturn([
                'owner' => [
                    'hasSnowflake' => true,
                ],
            ]);

        $config = new NewWorkspaceConfig(
            StagingType::WorkspaceSnowflake,
            'test-component',
            'test-config',
            $backendSize,
            null,
            NetworkPolicy::SYSTEM,
            WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR,
        );

        $workspaceProvider = new WorkspaceProvider(
            $workspacesApiClient,
            $componentsApiClient,
            $snowflakeKeypairGenerator,
        );

        $workspace = $workspaceProvider->createNewWorkspace($storageApiToken, $config);

        self::assertSame($workspaceId, $workspace->getWorkspaceId());
        self::assertSame($backendSize, $workspace->getBackendSize());
        self::assertSame('snowflake', $workspace->getBackendType());
        self::assertSame(
            [
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'schema' => 'some-schema',
                'user' => 'some-user',
                'password' => null,
                'privateKey' => $privateKey,
                'account' => 'some-host',
            ],
            $workspace->getCredentials(),
        );
    }

    public function testCreateNewWorkspaceForNonWorkspaceStagingThrowsError(): void
    {
        $componentsApiClient = $this->createMock(Components::class);
        $componentsApiClient->expects(self::never())->method(self::anything());

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::never())->method(self::anything());

        $snowflakeKeypairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);
        $snowflakeKeypairGenerator->expects(self::never())->method(self::anything());

        $workspaceProvider = new WorkspaceProvider(
            $workspacesApiClient,
            $componentsApiClient,
            $snowflakeKeypairGenerator,
        );

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Can\'t create workspace for staging type "local"');

        $workspaceProvider->createNewWorkspace(
            $this->createMock(StorageApiToken::class),
            new NewWorkspaceConfig(
                stagingType: StagingType::Local,
                componentId: 'test-component',
                configId: null,
                size: null,
                useReadonlyRole: null,
                networkPolicy: NetworkPolicy::SYSTEM,
                loginType: null,
            ),
        );
    }

    public function testCreateNewWorkspaceWithUnsupportedBackend(): void
    {
        $componentsApiClient = $this->createMock(Components::class);
        $componentsApiClient->expects(self::never())->method(self::anything());

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::never())->method(self::anything());

        $snowflakeKeypairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);
        $snowflakeKeypairGenerator->expects(self::never())->method(self::anything());

        $storageApiToken = $this->createMock(StorageApiToken::class);
        $storageApiToken
            ->expects(self::once())
            ->method('getTokenInfo')
            ->willReturn([
                'owner' => [
                    // no hasSnowflake/hasBigquery
                ],
            ]);

        $config = new NewWorkspaceConfig(
            StagingType::WorkspaceSnowflake,
            'test-component',
            'test-config',
            'large',
            null,
            NetworkPolicy::SYSTEM,
            null,
        );

        $workspaceProvider = new WorkspaceProvider(
            $workspacesApiClient,
            $componentsApiClient,
            $snowflakeKeypairGenerator,
        );

        $this->expectException(StagingNotSupportedByProjectException::class);
        $this->expectExceptionMessage('The project does not support "workspace-snowflake" staging.');

        $workspaceProvider->createNewWorkspace($storageApiToken, $config);
    }

    public function testGetExistingWorkspace(): void
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
                'loginType' => WorkspaceLoginType::DEFAULT->value,
            ],
        ];

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::once())
            ->method('getWorkspace')
            ->with((int) $workspaceId)
            ->willReturn($workspaceData);

        $componentsApiClient = $this->createMock(Components::class);
        $componentsApiClient->expects(self::never())->method(self::anything());

        $snowflakeKeypairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);
        $snowflakeKeypairGenerator->expects(self::never())->method(self::anything());

        $workspaceProvider = new WorkspaceProvider(
            $workspacesApiClient,
            $componentsApiClient,
            $snowflakeKeypairGenerator,
        );

        $workspace = $workspaceProvider->getExistingWorkspace($workspaceId, null);

        self::assertInstanceOf(Workspace::class, $workspace);
        self::assertSame($workspaceId, $workspace->getWorkspaceId());
        self::assertSame('small', $workspace->getBackendSize());
        self::assertSame('snowflake', $workspace->getBackendType());
    }

    public function testGetExistingWorkspaceWithCredentials(): void
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
                'loginType' => WorkspaceLoginType::DEFAULT->value,
            ],
        ];

        $credentialsData = [
            'password' => 'provided-password',
        ];

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::once())
            ->method('getWorkspace')
            ->with((int) $workspaceId)
            ->willReturn($workspaceData);

        $componentsApiClient = $this->createMock(Components::class);
        $componentsApiClient->expects(self::never())->method(self::anything());

        $snowflakeKeypairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);
        $snowflakeKeypairGenerator->expects(self::never())->method(self::anything());

        $workspaceProvider = new WorkspaceProvider(
            $workspacesApiClient,
            $componentsApiClient,
            $snowflakeKeypairGenerator,
        );

        $workspace = $workspaceProvider->getExistingWorkspace($workspaceId, $credentialsData);

        self::assertInstanceOf(WorkspaceWithCredentials::class, $workspace);
        self::assertSame($workspaceId, $workspace->getWorkspaceId());
        self::assertSame('small', $workspace->getBackendSize());
        self::assertSame('snowflake', $workspace->getBackendType());
        self::assertSame(
            [
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'schema' => 'some-schema',
                'user' => 'some-user',
                'password' => 'provided-password',
                'privateKey' => null,
                'account' => 'some-host',
            ],
            $workspace->getCredentials(),
        );
    }

    public function testResetWorkspaceCredentials(): void
    {
        $workspaceId = '123456';
        $resetCredentials = [
            'password' => 'new-password',
            'user' => 'new-user',
        ];

        $workspace = $this->createMock(WorkspaceInterface::class);
        $workspace->expects(self::once())
            ->method('getWorkspaceId')
            ->willReturn($workspaceId);
        $workspace->expects(self::once())
            ->method('getLoginType')
            ->willReturn(WorkspaceLoginType::DEFAULT);

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::once())
            ->method('resetCredentials')
            ->with(
                $workspaceId,
                new ResetCredentialsRequest(),
            )
            ->willReturn($resetCredentials);

        $componentsApiClient = $this->createMock(Components::class);
        $componentsApiClient->expects(self::never())->method(self::anything());

        $snowflakeKeypairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);
        $snowflakeKeypairGenerator->expects(self::never())->method(self::anything());

        $workspaceProvider = new WorkspaceProvider(
            $workspacesApiClient,
            $componentsApiClient,
            $snowflakeKeypairGenerator,
        );

        $credentials = $workspaceProvider->resetWorkspaceCredentials($workspace);

        self::assertSame($resetCredentials, $credentials);
    }

    public function testResetWorkspaceCredentialsWithKeyPair(): void
    {
        $workspaceId = '123456';
        $publicKey = 'new-public-key';
        $privateKey = 'new-private-key';

        $workspace = $this->createMock(WorkspaceInterface::class);
        $workspace->expects(self::once())
            ->method('getWorkspaceId')
            ->willReturn($workspaceId);
        $workspace->expects(self::once())
            ->method('getLoginType')
            ->willReturn(WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR);

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::once())
            ->method('resetCredentials')
            ->with(
                $workspaceId,
                new ResetCredentialsRequest(
                    publicKey: $publicKey,
                ),
            );

        $componentsApiClient = $this->createMock(Components::class);
        $componentsApiClient->expects(self::never())->method(self::anything());

        $snowflakeKeypairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);
        $snowflakeKeypairGenerator->expects(self::once())
            ->method('generateKeyPair')
            ->willReturn(new PemKeyCertificatePair(
                privateKey: $privateKey,
                publicKey: $publicKey,
            ));

        $workspaceProvider = new WorkspaceProvider(
            $workspacesApiClient,
            $componentsApiClient,
            $snowflakeKeypairGenerator,
        );

        $credentials = $workspaceProvider->resetWorkspaceCredentials($workspace);

        self::assertSame(['privateKey' => $privateKey], $credentials);
    }

    public function testCleanupWorkspace(): void
    {
        $workspaceId = '123456';

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects(self::once())
            ->method('deleteWorkspace')
            ->with((int) $workspaceId, [], true);

        $componentsApiClient = $this->createMock(Components::class);
        $componentsApiClient->expects(self::never())->method(self::anything());

        $snowflakeKeypairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);
        $snowflakeKeypairGenerator->expects(self::never())->method(self::anything());

        $workspaceProvider = new WorkspaceProvider(
            $workspacesApiClient,
            $componentsApiClient,
            $snowflakeKeypairGenerator,
        );

        $workspaceProvider->cleanupWorkspace($workspaceId);
    }

    public function testCleanupNonExistentWorkspace(): void
    {
        $workspaceId = '123456';

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects(self::once())
            ->method('deleteWorkspace')
            ->with((int) $workspaceId, [], true)
            ->willThrowException(new ClientException('Workspace not found', 404));

        $componentsApiClient = $this->createMock(Components::class);
        $componentsApiClient->expects(self::never())->method(self::anything());

        $snowflakeKeypairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);
        $snowflakeKeypairGenerator->expects(self::never())->method(self::anything());

        $workspaceProvider = new WorkspaceProvider(
            $workspacesApiClient,
            $componentsApiClient,
            $snowflakeKeypairGenerator,
        );

        // Should not throw an exception for 404 errors
        $workspaceProvider->cleanupWorkspace($workspaceId);
    }

    public function testCleanupWorkspaceWithError(): void
    {
        $workspaceId = '123456';

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects(self::once())
            ->method('deleteWorkspace')
            ->with((int) $workspaceId, [], true)
            ->willThrowException(new ClientException('Some other error', 500));

        $componentsApiClient = $this->createMock(Components::class);
        $componentsApiClient->expects(self::never())->method(self::anything());

        $snowflakeKeypairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);
        $snowflakeKeypairGenerator->expects(self::never())->method(self::anything());

        $workspaceProvider = new WorkspaceProvider(
            $workspacesApiClient,
            $componentsApiClient,
            $snowflakeKeypairGenerator,
        );

        // Should throw an exception for non-404 errors
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Some other error');
        $workspaceProvider->cleanupWorkspace($workspaceId);
    }
}
