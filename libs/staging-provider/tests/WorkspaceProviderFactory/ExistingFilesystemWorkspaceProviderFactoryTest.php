<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\WorkspaceProviderFactory;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Staging\Workspace\AbsWorkspaceStaging;
use Keboola\StagingProvider\Staging\Workspace\SnowflakeWorkspaceStaging;
use Keboola\StagingProvider\WorkspaceProviderFactory\Credentials\ABSWorkspaceCredentials;
use Keboola\StagingProvider\WorkspaceProviderFactory\ExistingFilesystemWorkspaceProviderFactory;
use Keboola\StorageApi\Workspaces;
use PHPUnit\Framework\TestCase;

class ExistingFilesystemWorkspaceProviderFactoryTest extends TestCase
{
    public function testWorkspaceIsFetched(): void
    {
        $workspaceId = '1234';
        $workspaceConnectionString = 'someRandomString';
        $stagingClass = AbsWorkspaceStaging::class;

        $workspaceApi = $this->createMock(Workspaces::class);
        $workspaceApi->method('getWorkspace')->with($workspaceId)->willReturn([
            'id' => '1',
            'connection' => [
                'backend' => $stagingClass::getType(),
                'container' => 'someContainer',
            ],
        ]);

        $factory = new ExistingFilesystemWorkspaceProviderFactory(
            $workspaceApi,
            $workspaceId,
            ABSWorkspaceCredentials::fromPasswordResetArray(['connectionString' => $workspaceConnectionString]),
        );

        $provider = $factory->getProvider($stagingClass);
        $provider->getWorkspaceId();
        $credentials = $provider->getCredentials();
        self::assertEquals(
            [
                'container' => 'someContainer',
                'connectionString' => 'someRandomString',
            ],
            $credentials,
        );
    }

    public function testWorkspaceBackendTypeIsChecked(): void
    {
        $workspaceId = '1234';
        $workspaceConnectionString = 'someRandomString';
        $stagingClass = SnowflakeWorkspaceStaging::class;

        $workspaceApi = $this->createMock(Workspaces::class);
        $workspaceApi->expects(self::once())->method('getWorkspace')->with($workspaceId)->willReturn([
            'connection' => [
                'backend' => 'foo',
            ],
        ]);

        $factory = new ExistingFilesystemWorkspaceProviderFactory(
            $workspaceApi,
            $workspaceId,
            ABSWorkspaceCredentials::fromPasswordResetArray(['connectionString' => $workspaceConnectionString]),
        );

        $provider = $factory->getProvider($stagingClass);

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage(sprintf(
            'Incompatible workspace type. Expected workspace backend is "%s", actual backend is "%s"',
            $stagingClass::getType(),
            'foo',
        ));

        $provider->getWorkspaceId();
    }

    public function testStagingInstanceIsCached(): void
    {
        $workspaceId = 'my.workspace';
        $workspaceConnectionString = 'someRandomString';
        $stagingClass = SnowflakeWorkspaceStaging::class;

        $workspaceApi = $this->createMock(Workspaces::class);
        $workspaceApi->expects(self::never())->method(self::anything());

        $factory = new ExistingFilesystemWorkspaceProviderFactory(
            $workspaceApi,
            $workspaceId,
            ABSWorkspaceCredentials::fromPasswordResetArray(['connectionString' => $workspaceConnectionString]),
        );

        $provider1 = $factory->getProvider($stagingClass);
        $provider2 = $factory->getProvider($stagingClass);

        self::assertSame($provider1, $provider2);
    }
}
