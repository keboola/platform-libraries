<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\Credentials\DatabaseWorkspaceCredentials;
use Keboola\StagingProvider\Provider\ExistingWorkspaceStagingProvider;
use Keboola\StorageApi\Workspaces;
use PHPUnit\Framework\TestCase;

class ExistingWorkspaceStagingProviderTest extends TestCase
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

        $workspaceProvider = new ExistingWorkspaceStagingProvider(
            $workspacesApiClient,
            $workspaceId,
            DatabaseWorkspaceCredentials::fromPasswordResetArray(['password' => 'test']),
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
                'password' => 'test',
                'account' => 'some-host',
            ],
            $workspaceProvider->getCredentials(),
        );
    }

    public function testPathThrowsExceptionOnRemoteProvider(): void
    {
        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects(self::never())
            ->method('getWorkspace');

        $workspaceProvider = new ExistingWorkspaceStagingProvider(
            $workspacesApiClient,
            '123456',
            DatabaseWorkspaceCredentials::fromPasswordResetArray(['password' => 'test']),
        );

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Workspace staging provider does not support path.');
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
                ],
            ]);

        $workspaceProvider = new ExistingWorkspaceStagingProvider(
            $workspacesApiClient,
            $workspaceId,
            DatabaseWorkspaceCredentials::fromPasswordResetArray(['password' => 'test']),
        );
        self::assertSame($workspaceId, $workspaceProvider->getWorkspaceId());
        $workspaceProvider->cleanup();
    }

    public function testCleanupDeletedWorkspaceStagingNotInitialized(): void
    {
        $workspaceId = '1';

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects(self::once())
            ->method('deleteWorkspace')
            ->with($workspaceId, [], true);
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
                ],
            ]);

        $workspaceProvider = new ExistingWorkspaceStagingProvider(
            $workspacesApiClient,
            $workspaceId,
            DatabaseWorkspaceCredentials::fromPasswordResetArray(['password' => 'test']),
        );
        // the workspace is cleaned even if "not initialized" (no getWorkspaceId called)
        $workspaceProvider->cleanup();
    }

    public function testWorkspaceStagingIsCreatedLazilyAndCached(): void
    {
        $workspaceId = '123456';
        $backendSize = 'large';

        $matcher = self::once();
        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects($matcher)
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

        $workspaceProvider = new ExistingWorkspaceStagingProvider(
            $workspacesApiClient,
            $workspaceId,
            DatabaseWorkspaceCredentials::fromPasswordResetArray(['password' => 'test']),
        );

        self::assertSame(0, $matcher->getInvocationCount());
        self::assertSame($workspaceId, $workspaceProvider->getWorkspaceId());
        self::assertSame(1, $matcher->getInvocationCount());

        self::assertSame($workspaceId, $workspaceProvider->getWorkspaceId());
        self::assertSame(1, $matcher->getInvocationCount());
    }
}
