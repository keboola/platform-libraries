<?php

namespace Keboola\StagingProvider\Tests\WorkspaceProviderFactory;

use Keboola\StagingProvider\Staging\Workspace\AbsWorkspaceStaging;
use Keboola\StagingProvider\WorkspaceProviderFactory\ExistingFilesystemWorkspaceProviderFactory;
use Keboola\StorageApi\Workspaces;
use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Staging\Workspace\SnowflakeWorkspaceStaging;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;

class ExistingFilesystemWorkspaceProviderFactoryTest extends TestCase
{
    public function testWorkspaceIsFetched()
    {
        $workspaceId = 'my.workspace';
        $workspaceConnectionString = 'someRandomString';
        $stagingClass = AbsWorkspaceStaging::class;

        $workspaceApi = $this->createMock(Workspaces::class);
        $workspaceApi->method('getWorkspace')->with($workspaceId)->willReturn([
            'id' => '1',
            'connection' => [
                'backend' => $stagingClass::getType(),
            ],
        ]);

        $factory = new ExistingFilesystemWorkspaceProviderFactory(
            $workspaceApi,
            $workspaceId,
            $workspaceConnectionString
        );

        $provider = $factory->getProvider($stagingClass);
        $provider->getWorkspaceId();
        $method = new ReflectionMethod($factory, 'getWorkspaceData');
        $method->setAccessible(true);
        $result = $method->invoke($factory, $stagingClass);
        self::assertEquals(
            [
                'id' => 1,
                'connection' => [
                    'backend' => 'abs',
                    'connectionString' => 'someRandomString',
                ],
            ],
            $result
        );
    }

    public function testWorkspaceBackendTypeIsChecked()
    {
        $workspaceId = 'my.workspace';
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
            $workspaceConnectionString
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
        $workspaceConnectionString = 'someRandomString';
        $stagingClass = SnowflakeWorkspaceStaging::class;

        $workspaceApi = $this->createMock(Workspaces::class);
        $workspaceApi->expects(self::never())->method(self::anything());

        $factory = new ExistingFilesystemWorkspaceProviderFactory(
            $workspaceApi,
            $workspaceId,
            $workspaceConnectionString
        );

        $provider1 = $factory->getProvider($stagingClass);
        $provider2 = $factory->getProvider($stagingClass);

        self::assertSame($provider1, $provider2);
    }
}
