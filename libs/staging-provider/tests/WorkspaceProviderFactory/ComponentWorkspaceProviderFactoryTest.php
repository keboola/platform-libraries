<?php

namespace Keboola\StagingProvider\Tests\WorkspaceProviderFactory;

use Keboola\StagingProvider\WorkspaceProviderFactory\Configuration\WorkspaceBackendConfig;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Workspaces;
use Keboola\StagingProvider\Staging\Workspace\SnowflakeWorkspaceStaging;
use Keboola\StagingProvider\WorkspaceProviderFactory\ComponentWorkspaceProviderFactory;
use PHPUnit\Framework\TestCase;

class ComponentWorkspaceProviderFactoryTest extends TestCase
{
    public function testNewWorkspaceIsCreatedWithConfigIdProvided()
    {
        $componentId = 'my.component';
        $configId = 'some-config';
        $stagingClass = SnowflakeWorkspaceStaging::class;

        $componentsApi = $this->createMock(Components::class);
        $componentsApi->expects(self::once())->method('createConfigurationWorkspace')->with($componentId, $configId, [
            'backend' => $stagingClass::getType(),
        ])->willReturn([
            'id' => '1',
            'connection' => [
                'backend' => $stagingClass::getType(),
            ],
        ]);

        $workspaceApi = $this->createMock(Workspaces::class);
        $workspaceApi->expects(self::never())->method(self::anything());

        $factory = new ComponentWorkspaceProviderFactory(
            $componentsApi,
            $workspaceApi,
            $componentId,
            $configId,
            new WorkspaceBackendConfig(null)
        );

        $provider = $factory->getProvider($stagingClass);
        $provider->getWorkspaceId();

        // no assert, just check the mock expectations were met
    }

    public function testNewWorkspaceIsCreatedWithoutConfigIdProvided()
    {
        $componentId = 'my.component';
        $configId = null;
        $stagingClass = SnowflakeWorkspaceStaging::class;

        $componentsApi = $this->createMock(Components::class);
        $componentsApi->expects(self::never())->method(self::anything());

        $workspaceApi = $this->createMock(Workspaces::class);
        $workspaceApi->expects(self::once())->method('createWorkspace')->with([
            'backend' => $stagingClass::getType(),
        ])->willReturn([
            'id' => 'test-workspace',
            'connection' => [
                'backend' => $stagingClass::getType(),
            ],
        ]);

        $factory = new ComponentWorkspaceProviderFactory(
            $componentsApi,
            $workspaceApi,
            $componentId,
            $configId,
            new WorkspaceBackendConfig(null)
        );

        $provider = $factory->getProvider($stagingClass);
        $provider->getWorkspaceId();

        // no assert, just check the mock expectations were met
    }

    public function testStagingInstanceIsCached()
    {
        $componentId = 'my.component';
        $configId = null;
        $stagingClass = SnowflakeWorkspaceStaging::class;

        $componentsApi = $this->createMock(Components::class);
        $componentsApi->expects(self::never())->method(self::anything());

        $workspaceApi = $this->createMock(Workspaces::class);
        $workspaceApi->expects(self::never())->method(self::anything());

        $factory = new ComponentWorkspaceProviderFactory(
            $componentsApi,
            $workspaceApi,
            $componentId,
            $configId,
            new WorkspaceBackendConfig(null)
        );

        $provider1 = $factory->getProvider($stagingClass);
        $provider2 = $factory->getProvider($stagingClass);

        self::assertSame($provider1, $provider2);
    }
}
