<?php

namespace Keboola\WorkspaceProvider\Tests\WorkspaceProviderFactory;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\StorageApi\Workspaces;
use Keboola\WorkspaceProvider\Exception\StagingProviderException;
use Keboola\WorkspaceProvider\Staging\Workspace\SnowflakeWorkspaceStaging;
use Keboola\WorkspaceProvider\WorkspaceProviderFactory\ExistingWorkspaceProviderFactory;
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

        $factory = new ExistingWorkspaceProviderFactory(
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

        $factory = new ExistingWorkspaceProviderFactory(
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

        $factory = new ExistingWorkspaceProviderFactory(
            $workspaceApi,
            $workspaceId,
            $workspacePassword
        );

        $provider1 = $factory->getProvider($stagingClass);
        $provider2 = $factory->getProvider($stagingClass);

        self::assertSame($provider1, $provider2);
    }
}
