<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider;

use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\KeyGenerator\PemKeyCertificatePair;
use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\Configuration\NetworkPolicy;
use Keboola\StagingProvider\Provider\Configuration\WorkspaceBackendConfig;
use Keboola\StagingProvider\Provider\NewWorkspaceProvider;
use Keboola\StagingProvider\Provider\SnowflakeKeypairGenerator;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\WorkspaceLoginType;
use Keboola\StorageApi\Workspaces;
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
        self::assertSame('snowflake', $workspaceProvider->getBackendType());
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
        self::assertSame('snowflake', $workspaceProvider->getBackendType());
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

    public function testBackendSizeAndTypeGettersDoesNotInitializeWorkspace(): void
    {
        $componentsApiClient = $this->createMock(Components::class);
        $componentsApiClient
            ->expects(self::never())
            ->method('createConfigurationWorkspace');

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::never())->method('createWorkspace');

        $snowflakeKeypairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);
        $snowflakeKeypairGenerator->expects(self::never())->method(self::anything());

        $workspaceProvider = new NewWorkspaceProvider(
            $workspacesApiClient,
            $componentsApiClient,
            $snowflakeKeypairGenerator,
            new WorkspaceBackendConfig(
                'workspace-snowflake',
                'small',
                true,
                NetworkPolicy::SYSTEM,
                WorkspaceLoginType::DEFAULT,
            ),
            'test-component',
            null,
        );

        self::assertSame('small', $workspaceProvider->getBackendSize());
        self::assertSame('snowflake', $workspaceProvider->getBackendType());
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
                $backendSize,
                null,
                NetworkPolicy::SYSTEM,
                WorkspaceLoginType::SNOWFLAKE_PERSON_SSO,
            ),
            'test-component',
            'test-config',
        );

        // first call should create the workspace
        $credentials = $workspaceProvider->getCredentials();

        // second call should use cached workspace
        self::assertSame($credentials, $workspaceProvider->getCredentials());
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

    public static function provideDefaultLoginTypeCases(): iterable
    {
// TODO enable once key-pair auth is default for Snowflake
//        yield 'snowflake workspace defaults to service keypair' => [
//            'workspaceType' => 'workspace-snowflake',
//            'expectedLoginType' => WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR,
//            'expectedWorkspaceRequest' => [
//                'backend' => 'snowflake',
//                'backendSize' => 'small',
//                'networkPolicy' => 'system',
//                'loginType' => WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR,
//                'publicKey' => 'public-key',
//            ],
//            'workspaceResponse' => [
//                'id' => '123456',
//                'backendSize' => 'small',
//                'connection' => [
//                    'backend' => 'snowflake',
//                    'host' => 'some-host',
//                    'warehouse' => 'some-warehouse',
//                    'database' => 'some-database',
//                    'schema' => 'some-schema',
//                    'user' => 'some-user',
//                ],
//            ],
//            'expectedCredentials' => [
//                'host' => 'some-host',
//                'warehouse' => 'some-warehouse',
//                'database' => 'some-database',
//                'schema' => 'some-schema',
//                'user' => 'some-user',
//                'password' => null,
//                'privateKey' => 'private-key',
//                'account' => 'some-host',
//            ],
//        ];

        yield 'abs workspace defaults to default login type' => [
            'workspaceType' => 'workspace-abs',
            'expectedLoginType' => WorkspaceLoginType::DEFAULT,
            'expectedWorkspaceRequest' => [
                'backend' => 'abs',
                'backendSize' => 'small',
                'networkPolicy' => 'system',
            ],
            'workspaceResponse' => [
                'id' => '123456',
                'backendSize' => 'small',
                'connection' => [
                    'backend' => 'abs',
                    'container' => 'some-container',
                    'connectionString' => 'some-string',
                ],
            ],
            'expectedCredentials' => [
                'container' => 'some-container',
                'connectionString' => 'some-string',
            ],
        ];

        yield 'redshift workspace defaults to default login type' => [
            'workspaceType' => 'workspace-redshift',
            'expectedLoginType' => WorkspaceLoginType::DEFAULT,
            'expectedWorkspaceRequest' => [
                'backend' => 'redshift',
                'backendSize' => 'small',
                'networkPolicy' => 'system',
            ],
            'workspaceResponse' => [
                'id' => '123456',
                'backendSize' => 'small',
                'connection' => [
                    'backend' => 'redshift',
                    'host' => 'some-host',
                    'warehouse' => 'some-warehouse',
                    'database' => 'some-database',
                    'schema' => 'some-schema',
                    'user' => 'some-user',
                    'password' => 'secret',
                ],
            ],
            'expectedCredentials' => [
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'schema' => 'some-schema',
                'user' => 'some-user',
                'password' => 'secret',
            ],
        ];
    }

    /**
     * @param value-of<AbstractStrategyFactory::WORKSPACE_TYPES> $workspaceType
     * @dataProvider provideDefaultLoginTypeCases
     */
    public function testDefaultLoginTypeConfiguration(
        string $workspaceType,
        WorkspaceLoginType $expectedLoginType,
        array $expectedWorkspaceRequest,
        array $workspaceResponse,
        array $expectedCredentials,
    ): void {
        $workspaceId = '123456';
        $backendSize = 'small';

        $componentsApiClient = $this->createMock(Components::class);
        $componentsApiClient
            ->expects(self::once())
            ->method('createConfigurationWorkspace')
            ->with(
                'test-component',
                'test-config',
                $expectedWorkspaceRequest,
                true,
            )
            ->willReturn($workspaceResponse);

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::never())->method(self::anything());

        $snowflakeKeypairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);
        $snowflakeKeypairGenerator
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
                $workspaceType,
                $backendSize,
                null,
                NetworkPolicy::SYSTEM,
                null, // Explicitly set to null to test default behavior
            ),
            'test-component',
            'test-config',
        );

        self::assertSame($workspaceId, $workspaceProvider->getWorkspaceId());
        self::assertSame($expectedCredentials, $workspaceProvider->getCredentials());
    }

    public function testPublicAndPrivateKeysAreGeneratedForKeyPairLoginType(): void
    {
        $workspaceId = '123456';
        $backendSize = 'small';
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
                NetworkPolicy::SYSTEM,
                WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR,
            ),
            'test-component',
            'test-config',
        );

        self::assertSame($workspaceId, $workspaceProvider->getWorkspaceId());
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
            $workspaceProvider->getCredentials(),
        );
    }
}
