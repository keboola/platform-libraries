<?php

namespace Keboola\StagingProvider\Tests\WorkspaceProviderFactory;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\StorageApi\Workspaces;
use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Staging\Workspace\SnowflakeWorkspaceStaging;
use Keboola\StagingProvider\WorkspaceProviderFactory\ExistingTableWorkspaceProviderFactory;
use PHPUnit\Framework\TestCase;

class ExistingWorkspaceProviderFactoryTest extends TestCase
{
    public function testWorkspaceIsFetched()
    {
        $workspaceId = 'my.workspace';
        $workspacePassword = 'pwd';
        $stagingClass = SnowflakeWorkspaceStaging::class;

        $workspaceApi = $this->createMock(Workspaces::class);
        $workspaceApi->expects(self::once())->method('getWorkspace')->with($workspaceId)->willReturn([
            'id' => '1',
            'connection' => [
                'backend' => $stagingClass::getType(),
            ],
        ]);

        $factory = new ExistingTableWorkspaceProviderFactory(
            $workspaceApi,
            $workspaceId,
            $workspacePassword
        );

        $provider = $factory->getProvider($stagingClass);
        $provider->getWorkspaceId();

        // no assert, just check the mock expectations were met
    }

    public function testWorkspaceBackendTypeIsChecked()
    {
        $workspaceId = 'my.workspace';
        $workspacePassword = 'pwd';
        $stagingClass = SnowflakeWorkspaceStaging::class;

        $workspaceApi = $this->createMock(Workspaces::class);
        $workspaceApi->expects(self::once())->method('getWorkspace')->with($workspaceId)->willReturn([
            'connection' => [
                'backend' => 'foo',
            ],
        ]);

        $factory = new ExistingTableWorkspaceProviderFactory(
            $workspaceApi,
            $workspaceId,
            $workspacePassword
        );

        $provider = $factory->getProvider($stagingClass);

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage(sprintf(
            'Incompatible workspace type. Expected workspace backend is "%s", actual backend is "%s"',
            $stagingClass::getType(),
            'foo'
        ));

        $provider->getWorkspaceId();
    }

    public function testStagingInstanceIsCached()
    {
        $workspaceId = 'my.workspace';
        $workspacePassword = 'pwd';
        $stagingClass = SnowflakeWorkspaceStaging::class;

        $workspaceApi = $this->createMock(Workspaces::class);
        $workspaceApi->expects(self::never())->method(self::anything());

        $factory = new ExistingTableWorkspaceProviderFactory(
            $workspaceApi,
            $workspaceId,
            $workspacePassword
        );

        $provider1 = $factory->getProvider($stagingClass);
        $provider2 = $factory->getProvider($stagingClass);

        self::assertSame($provider1, $provider2);
    }
}
