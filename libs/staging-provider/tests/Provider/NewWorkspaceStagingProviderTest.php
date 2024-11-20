<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\Configuration\WorkspaceBackendConfig;
use Keboola\StagingProvider\Provider\NewWorkspaceStagingProvider;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Workspaces;
use PHPUnit\Framework\TestCase;

class NewWorkspaceStagingProviderTest extends TestCase
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
                ['backend' => 'snowflake', 'backendSize' => $backendSize, 'networkPolicy' => 'user'],
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

        $workspaceProvider = new NewWorkspaceStagingProvider(
            $workspacesApiClient,
            $componentsApiClient,
            new WorkspaceBackendConfig(
                'workspace-snowflake',
                $backendSize,
                null,
                'user',
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

        $workspaceProvider = new NewWorkspaceStagingProvider(
            $workspacesApiClient,
            $componentsApiClient,
            new WorkspaceBackendConfig(
                'workspace-snowflake',
                $backendSize,
                true,
                'system',
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
                ['backend' => 'abs', 'networkPolicy' => 'system'],
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

        $workspaceProvider = new NewWorkspaceStagingProvider(
            $workspacesApiClient,
            $componentsApiClient,
            new WorkspaceBackendConfig(
                'workspace-abs',
                null,
                null,
                'system',
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

    public function testPathThrowsExceptionOnRemoteProvider(): void
    {
        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient->expects(self::never())->method(self::anything());

        $componentsApiClient = $this->createMock(Components::class);
        $componentsApiClient->expects(self::never())->method(self::anything());

        $workspaceProvider = new NewWorkspaceStagingProvider(
            $workspacesApiClient,
            $componentsApiClient,
            new WorkspaceBackendConfig(
                'workspace-snowflake',
                'large',
                null,
                'system',
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

        $workspaceProvider = new NewWorkspaceStagingProvider(
            $workspacesApiClient,
            $componentsApiClient,
            new WorkspaceBackendConfig(
                'workspace-snowflake',
                'so-so',
                null,
                'system',
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

        $workspaceProvider = new NewWorkspaceStagingProvider(
            $workspacesApiClient,
            $componentsApiClient,
            new WorkspaceBackendConfig(
                'workspace-snowflake',
                'so-so',
                null,
                'system',
            ),
            'test-component',
            'test-config',
        );
        $workspaceProvider->cleanup();
    }

    public function testWorkspaceStagingIsCreatedLazilyAndCached(): void
    {
        $workspaceId = '123456';

        $matcher = self::once();
        $componentsApiClient = $this->createMock(Components::class);
        $componentsApiClient
            ->expects($matcher)
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
        $workspacesApiClient->expects(self::never())->method(self::anything());

        $workspaceProvider = new NewWorkspaceStagingProvider(
            $workspacesApiClient,
            $componentsApiClient,
            new WorkspaceBackendConfig(
                'workspace-snowflake',
                'so-so',
                null,
                'system',
            ),
            'test-component',
            'test-config',
        );

        self::assertSame(0, $matcher->getInvocationCount());
        self::assertSame($workspaceId, $workspaceProvider->getWorkspaceId());
        self::assertSame(1, $matcher->getInvocationCount());

        self::assertSame($workspaceId, $workspaceProvider->getWorkspaceId());
        self::assertSame(1, $matcher->getInvocationCount());
    }
}
