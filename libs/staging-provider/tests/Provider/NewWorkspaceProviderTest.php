<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\Configuration\NetworkPolicy;
use Keboola\StagingProvider\Provider\Configuration\WorkspaceBackendConfig;
use Keboola\StagingProvider\Provider\NewWorkspaceProvider;
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

        $workspaceProvider = new NewWorkspaceProvider(
            $workspacesApiClient,
            $componentsApiClient,
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

        $workspaceProvider = new NewWorkspaceProvider(
            $workspacesApiClient,
            $componentsApiClient,
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
                'account' => 'some-host',
            ],
            $workspaceProvider->getCredentials(),
        );
    }

    public function testWorkspaceAbs(): void
    {
        $workspaceId = '123456';

        $componentsApiClient = $this->createMock(Components::class);
        $componentsApiClient
            ->expects(self::once())
            ->method('createConfigurationWorkspace')
            ->with(
                'test-component',
                'test-config',
                ['backend' => 'abs', 'networkPolicy' => 'system', 'loginType' => WorkspaceLoginType::DEFAULT],
                true,
            )
            ->willReturn([
                'id' => $workspaceId,
                'backendSize' => null,
                'connection' => [
                    'backend' => 'abs',
                    'container' => 'some-container',
                    'connectionString' => 'some-string',
                ],
            ]);

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::never())->method(self::anything());

        $workspaceProvider = new NewWorkspaceProvider(
            $workspacesApiClient,
            $componentsApiClient,
            new WorkspaceBackendConfig(
                'workspace-abs',
                null,
                null,
                NetworkPolicy::SYSTEM,
                WorkspaceLoginType::DEFAULT,
            ),
            'test-component',
            'test-config',
        );

        self::assertSame($workspaceId, $workspaceProvider->getWorkspaceId());
        self::assertSame(null, $workspaceProvider->getBackendSize());
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
        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::never())->method(self::anything());

        $componentsApiClient = $this->createMock(Components::class);
        $componentsApiClient->expects(self::never())->method(self::anything());

        $workspaceProvider = new NewWorkspaceProvider(
            $workspacesApiClient,
            $componentsApiClient,
            new WorkspaceBackendConfig(
                'workspace-snowflake',
                'large',
                null,
                NetworkPolicy::SYSTEM,
                WorkspaceLoginType::DEFAULT,
            ),
            'test-component',
            'test-config',
        );

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Workspace staging provider does not support path.');

        $workspaceProvider->getPath();
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

        $workspaceProvider = new NewWorkspaceProvider(
            $workspacesApiClient,
            $componentsApiClient,
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

        $workspaceProvider = new NewWorkspaceProvider(
            $workspacesApiClient,
            $componentsApiClient,
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

        $workspaceProvider = new NewWorkspaceProvider(
            $workspacesApiClient,
            $componentsApiClient,
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
}
