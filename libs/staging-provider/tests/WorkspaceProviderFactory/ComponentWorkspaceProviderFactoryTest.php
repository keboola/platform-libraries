<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\WorkspaceProviderFactory;

use Keboola\StagingProvider\Staging\Workspace\SnowflakeWorkspaceStaging;
use Keboola\StagingProvider\WorkspaceProviderFactory\ComponentWorkspaceProviderFactory;
use Keboola\StagingProvider\WorkspaceProviderFactory\Configuration\WorkspaceBackendConfig;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Workspaces;
use PHPUnit\Framework\TestCase;

class ComponentWorkspaceProviderFactoryTest extends TestCase
{
    public function testNewWorkspaceIsCreatedWithConfigIdProvided(): void
    {
        $componentId = 'my.component';
        $configId = 'some-config';
        $stagingClass = SnowflakeWorkspaceStaging::class;

        $componentsApi = $this->createMock(Components::class);
        $componentsApi->expects(self::once())->method('createConfigurationWorkspace')->with(
            $componentId,
            $configId,
            [
                'backend' => $stagingClass::getType(),
            ],
            true,
        )->willReturn([
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
            new WorkspaceBackendConfig(null),
        );

        $provider = $factory->getProvider($stagingClass);
        $provider->getWorkspaceId();

        // no assert, just check the mock expectations were met
    }

    public function testNewWorkspaceIsCreatedWithoutConfigIdProvided(): void
    {
        $componentId = 'my.component';
        $configId = null;
        $stagingClass = SnowflakeWorkspaceStaging::class;

        $componentsApi = $this->createMock(Components::class);
        $componentsApi->expects(self::never())->method(self::anything());

        $workspaceApi = $this->createMock(Workspaces::class);
        $workspaceApi->expects(self::once())->method('createWorkspace')->with(
            [
                'backend' => $stagingClass::getType(),
            ],
            true,
        )->willReturn([
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
            new WorkspaceBackendConfig(null),
        );

        $provider = $factory->getProvider($stagingClass);
        $provider->getWorkspaceId();

        // no assert, just check the mock expectations were met
    }

    public function testStagingInstanceIsCached(): void
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
            new WorkspaceBackendConfig(null),
        );

        $provider1 = $factory->getProvider($stagingClass);
        $provider2 = $factory->getProvider($stagingClass);

        self::assertSame($provider1, $provider2);
    }

    public function testSnowflakeWorkspaceReceivesBackendSize(): void
    {
        $componentId = 'my.component';
        $configId = null;
        $stagingClass = SnowflakeWorkspaceStaging::class;
        $backendSize = 'custom';

        $componentsApi = $this->createMock(Components::class);
        $componentsApi->expects(self::never())->method(self::anything());

        $workspaceApi = $this->createMock(Workspaces::class);
        $workspaceApi->expects(self::once())->method('createWorkspace')->with([
            'backend' => $stagingClass::getType(),
            'backendSize' => $backendSize,
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
            new WorkspaceBackendConfig($backendSize),
        );

        $provider = $factory->getProvider($stagingClass);
        $provider->getWorkspaceId();

        // no assert, just check the $workspaceApi mock expectations were met
    }

    public function testReadOnlyRoleFlagTrue(): void
    {
        $componentId = 'my.component';
        $configId = null;
        $stagingClass = SnowflakeWorkspaceStaging::class;

        $componentsApi = $this->createMock(Components::class);
        $componentsApi->expects(self::never())->method(self::anything());

        $workspaceApi = $this->createMock(Workspaces::class);
        $workspaceApi->expects(self::once())->method('createWorkspace')->with([
            'backend' => $stagingClass::getType(),
            'readOnlyStorageAccess' => true,
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
            new WorkspaceBackendConfig(null),
            true,
        );

        $provider = $factory->getProvider($stagingClass);
        $provider->getWorkspaceId();

        // no assert, just check the $workspaceApi mock expectations were met
    }

    public function testReadOnlyRoleFlagFalse(): void
    {
        $componentId = 'my.component';
        $configId = null;
        $stagingClass = SnowflakeWorkspaceStaging::class;

        $componentsApi = $this->createMock(Components::class);
        $componentsApi->expects(self::never())->method(self::anything());

        $workspaceApi = $this->createMock(Workspaces::class);
        $workspaceApi->expects(self::once())->method('createWorkspace')->with([
            'backend' => $stagingClass::getType(),
            'readOnlyStorageAccess' => false,
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
            new WorkspaceBackendConfig(null),
            false,
        );

        $provider = $factory->getProvider($stagingClass);
        $provider->getWorkspaceId();

        // no assert, just check the $workspaceApi mock expectations were met
    }
}
