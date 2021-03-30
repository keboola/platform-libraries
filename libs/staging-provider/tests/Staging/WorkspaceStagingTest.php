<?php

namespace Keboola\WorkspaceProvider\Tests\Staging;

use Keboola\WorkspaceProvider\Exception\StagingProviderException;
use Keboola\WorkspaceProvider\Staging\Workspace\AbsWorkspaceStaging;
use Keboola\WorkspaceProvider\Staging\Workspace\RedshiftWorkspaceStaging;
use Keboola\WorkspaceProvider\Staging\Workspace\SnowflakeWorkspaceStaging;
use Keboola\WorkspaceProvider\Staging\Workspace\SynapseWorkspaceStaging;
use PHPUnit\Framework\TestCase;

class WorkspaceStagingTest extends TestCase
{
    /**
     * @dataProvider provideWorkspacesWithTypes
     */
    public function testBackendTypeIsChecked($workspaceClass, $expectedType)
    {
        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage(sprintf(
            'Backend configuration does not match the workspace type. Expected "%s", got "dummy"',
            $expectedType
        ));

        new $workspaceClass([
            'id' => 'test',
            'connection' => [
                'backend' => 'dummy',
            ],
        ]);
    }

    public function provideWorkspacesWithTypes()
    {
        yield 'snowflake' => [SnowflakeWorkspaceStaging::class, 'snowflake'];
        yield 'synapse' => [SynapseWorkspaceStaging::class, 'synapse'];
        yield 'redshift' => [RedshiftWorkspaceStaging::class, 'redshift'];
        yield 'abs' => [AbsWorkspaceStaging::class, 'abs'];
    }

    public function testWorkspaceIdIsReturned()
    {
        $workspaceId = 'test';
        $workspace = new SnowflakeWorkspaceStaging([
            'id' => $workspaceId,
            'connection' => [
                'backend' => SnowflakeWorkspaceStaging::getType(),
            ]
        ]);

        self::assertSame($workspaceId, $workspace->getWorkspaceId());
    }

    public function testCredentialsAreReturned()
    {
        $credentials = [
            'host' => 'host',
            'warehouse' => 'warehouse',
            'database' => 'database',
            'schema' => 'schema',
            'user' => 'user',
            'password' => 'password',
        ];

        $workspace = new SnowflakeWorkspaceStaging([
            'connection' => $credentials + [
                'backend' => SnowflakeWorkspaceStaging::getType(),
                'extra' => 'dummy'
            ],
        ]);

        self::assertSame($credentials, $workspace->getCredentials());
    }
}
