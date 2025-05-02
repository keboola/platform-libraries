<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider;

use Keboola\KeyGenerator\PemKeyCertificatePair;
use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\Configuration\NetworkPolicy;
use Keboola\StagingProvider\Provider\Configuration\WorkspaceBackendConfig;
use Keboola\StagingProvider\Provider\NewWorkspaceProvider;
use Keboola\StagingProvider\Provider\SnowflakeKeypairGenerator;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\WorkspaceLoginType;
use Keboola\StorageApi\Workspaces;
use Keboola\StagingProvider\Provider\Workspace;
use PHPUnit\Framework\TestCase;

class NewWorkspaceProviderTest extends TestCase
{
    public function testWorkspaceGetters(): void
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

        $workspaceProvider = new NewWorkspaceProvider(
            $workspacesApiClient,
            $componentsApiClient,
            $snowflakeKeypairGenerator,
            new WorkspaceBackendConfig(
                'workspace-snowflake',
                $backendSize,
                null,
                NetworkPolicy::USER,
                WorkspaceLoginType::DEFAULT,
            ),
            'test-component',
            'test-config',
        );

        self::assertSame($workspaceId, $workspaceProvider->getWorkspaceId());
        self::assertSame($backendSize, $workspaceProvider->getBackendSize());
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
            $workspaceProvider->getCredentials(),
        );
    }

    public function testWorkspaceWithoutConfiguration(): void
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

        $workspaceProvider = new NewWorkspaceProvider(
            $workspacesApiClient,
            $componentsApiClient,
            $snowflakeKeypairGenerator,
            new WorkspaceBackendConfig(
                'workspace-snowflake',
                $backendSize,
                true,
                NetworkPolicy::SYSTEM,
                WorkspaceLoginType::DEFAULT,
            ),
            'test-component',
            null,
        );

        self::assertSame($workspaceId, $workspaceProvider->getWorkspaceId());
        self::assertSame($backendSize, $workspaceProvider->getBackendSize());
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
            $workspaceProvider->getCredentials(),
        );
    }

    public function testWorkspaceAbs(): void
    {
        $workspaceId = '123456';
        $workspaceData = [
            'id' => $workspaceId,
            'backendSize' => 'small',
            'connection' => [
                'backend' => 'abs',
                'container' => 'some-container',
                'connectionString' => 'some-string',
            ],
        ];

        $componentsApiClient = $this->createMock(Components::class);
        $componentsApiClient
            ->expects(self::once())
            ->method('createConfigurationWorkspace')
            ->with(
                'test-component',
                'test-config',
                [
                    'backend' => 'abs',
                    'networkPolicy' => 'system',
                    'loginType' => WorkspaceLoginType::DEFAULT,
                    'backendSize' => 'small',
                ],
                true,
            )
            ->willReturn($workspaceData);

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::never())->method(self::anything());

        $snowflakeKeypairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);
        $snowflakeKeypairGenerator->expects(self::never())->method(self::anything());

        $workspaceProvider = new NewWorkspaceProvider(
            $workspacesApiClient,
            $componentsApiClient,
            $snowflakeKeypairGenerator,
            new WorkspaceBackendConfig(
                'workspace-abs',
                'small',
                null,
                NetworkPolicy::SYSTEM,
                WorkspaceLoginType::DEFAULT,
            ),
            'test-component',
            'test-config',
        );

        self::assertSame($workspaceId, $workspaceProvider->getWorkspaceId());
        self::assertSame(
            [
                'container' => 'some-container',
                'connectionString' => 'some-string',
            ],
            $workspaceProvider->getCredentials(),
        );
    }

    public function testPathThrowsException(): void
    {
        $workspaces = $this->createMock(Workspaces::class);
        $components = $this->createMock(Components::class);
        $keypairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);

        $provider = new NewWorkspaceProvider(
            $workspaces,
            $components,
            $keypairGenerator,
            new WorkspaceBackendConfig(
                'workspace-snowflake',
                'small',
                null,
                NetworkPolicy::SYSTEM,
                WorkspaceLoginType::DEFAULT,
            ),
            'test-component',
            'test-config',
        );

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('NewWorkspaceProvider does not support path');

        $provider->getPath();
    }

    public function testCleanupDeletedWorkspaceStaging(): void
    {
        $workspaceId = '123456';

        $componentsApiClient = $this->createMock(Components::class);
        $componentsApiClient
            ->expects(self::once())
            ->method('createConfigurationWorkspace')
            ->willReturn([
                'id' => $workspaceId,
                'backendSize' => 'so-so',
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
        $workspacesApiClient
            ->expects(self::once())
            ->method('deleteWorkspace')
            ->with($workspaceId, [], true);

        $snowflakeKeypairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);
        $snowflakeKeypairGenerator->expects(self::never())->method(self::anything());

        $workspaceProvider = new NewWorkspaceProvider(
            $workspacesApiClient,
            $componentsApiClient,
            $snowflakeKeypairGenerator,
            new WorkspaceBackendConfig(
                'workspace-snowflake',
                'so-so',
                null,
                NetworkPolicy::SYSTEM,
                WorkspaceLoginType::DEFAULT,
            ),
            'test-component',
            'test-config',
        );
        self::assertSame($workspaceId, $workspaceProvider->getWorkspaceId());  // Ensure workspace is initialized
        $workspaceProvider->cleanup();
    }

    public function testCleanupDeletedWorkspaceStagingNotInitialized(): void
    {
        $componentsApiClient = $this->createMock(Components::class);
        $componentsApiClient
            ->expects(self::never())
            ->method('createConfigurationWorkspace');

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects(self::never())
            ->method('deleteWorkspace');

        $snowflakeKeypairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);
        $snowflakeKeypairGenerator->expects(self::never())->method(self::anything());

        $workspaceProvider = new NewWorkspaceProvider(
            $workspacesApiClient,
            $componentsApiClient,
            $snowflakeKeypairGenerator,
            new WorkspaceBackendConfig(
                'workspace-snowflake',
                'so-so',
                null,
                NetworkPolicy::SYSTEM,
                WorkspaceLoginType::DEFAULT,
            ),
            'test-component',
            'test-config',
        );
        $workspaceProvider->cleanup();
    }

    public function testWorkspaceStagingIsCreatedLazilyAndCached(): void
    {
        $workspaceId = '123456';
        $backendSize = 'xsmall';

        $componentsApiClient = $this->createMock(Components::class);
        $componentsApiClient
            ->expects(self::once())
            ->method('createConfigurationWorkspace')
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

        $workspaceProvider = new NewWorkspaceProvider(
            $workspacesApiClient,
            $componentsApiClient,
            $snowflakeKeypairGenerator,
            new WorkspaceBackendConfig(
                'workspace-snowflake',
                'so-so',
                null,
                NetworkPolicy::SYSTEM,
                WorkspaceLoginType::SNOWFLAKE_PERSON_SSO,
            ),
            'test-component',
            'test-config',
        );

        // first call should create the workspace
        self::assertSame($backendSize, $workspaceProvider->getBackendSize());

        // second call should use cached workspace
        self::assertSame($backendSize, $workspaceProvider->getBackendSize());
    }

    public static function provideSnowflakeKeyPairTypes(): iterable
    {
        yield 'snowflake person key-pair' => [
            'type' => WorkspaceLoginType::SNOWFLAKE_PERSON_KEYPAIR,
        ];

        yield 'snowflake service key-pair' => [
            'type' => WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR,
        ];
    }

    /** @dataProvider provideSnowflakeKeyPairTypes */
    public function testSnowflakeKeyPairWorkspace(WorkspaceLoginType $workspaceLoginType): void
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
                    'loginType' => $workspaceLoginType,
                    'publicKey' => 'public-key',
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
        $snowflakeKeypairGenerator->expects(self::once())
            ->method('generateKeyPair')
            ->willReturn(new PemKeyCertificatePair(
                privateKey: 'private-key',
                publicKey: 'public-key',
            ))
        ;

        $workspaceProvider = new NewWorkspaceProvider(
            $workspacesApiClient,
            $componentsApiClient,
            $snowflakeKeypairGenerator,
            new WorkspaceBackendConfig(
                'workspace-snowflake',
                $backendSize,
                null,
                NetworkPolicy::USER,
                $workspaceLoginType,
            ),
            'test-component',
            'test-config',
        );

        self::assertSame(
            [
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'schema' => 'some-schema',
                'user' => 'some-user',
                'password' => null,
                'privateKey' => 'private-key',
                'account' => 'some-host',
            ],
            $workspaceProvider->getCredentials(),
        );
    }

    public function testResetCredentials(): void
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
                'password' => 'secret',
            ],
        ];

        $componentsApiClient = $this->createMock(Components::class);
        $componentsApiClient->expects(self::never())->method(self::anything());

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects(self::once())
            ->method('createWorkspace')
            ->with(
                [
                    'backend' => 'snowflake',
                    'backendSize' => 'small',
                    'networkPolicy' => 'system',
                    'loginType' => WorkspaceLoginType::DEFAULT,
                ],
                true,
            )
            ->willReturn($workspaceData);

        $snowflakeKeypairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);
        $snowflakeKeypairGenerator->expects(self::never())->method(self::anything());

        $workspaceProvider = new NewWorkspaceProvider(
            $workspacesApiClient,
            $componentsApiClient,
            $snowflakeKeypairGenerator,
            new WorkspaceBackendConfig(
                'workspace-snowflake',
                'small',
                null,
                NetworkPolicy::SYSTEM,
                WorkspaceLoginType::DEFAULT,
            ),
            'test-component',
            null,
        );

        self::assertSame($workspaceId, $workspaceProvider->getWorkspaceId());
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
            $workspaceProvider->getCredentials(),
        );
    }

    public function testResetCredentialsWithKeyPair(): void
    {
        $workspaces = $this->createMock(Workspaces::class);
        $components = $this->createMock(Components::class);
        $keypairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);

        $workspaceData = [
            'id' => '123456',
            'backendSize' => 'small',
            'connection' => [
                'backend' => 'snowflake',
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'schema' => 'some-schema',
                'user' => 'some-user',
                'loginType' => WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR->value,
            ],
        ];

        $components->expects(self::once())
            ->method('createConfigurationWorkspace')
            ->with(
                'test-component',
                'test-config',
                [
                    'backend' => 'snowflake',
                    'backendSize' => 'small',
                    'networkPolicy' => 'system',
                    'loginType' => WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR,
                    'publicKey' => 'public-key',
                ],
                true,
            )
            ->willReturn($workspaceData);

        $keypairGenerator->expects(self::once())
            ->method('generateKeyPair')
            ->willReturn(new PemKeyCertificatePair(
                privateKey: 'private-key',
                publicKey: 'public-key',
            ));

        $provider = new NewWorkspaceProvider(
            $workspaces,
            $components,
            $keypairGenerator,
            new WorkspaceBackendConfig(
                'workspace-snowflake',
                'small',
                null,
                NetworkPolicy::SYSTEM,
                WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR,
            ),
            'test-component',
            'test-config',
        );

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Invalid parameters for key-pair authentication');

        $provider->resetCredentials([]);
    }
}
