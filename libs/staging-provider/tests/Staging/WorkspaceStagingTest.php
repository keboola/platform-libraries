<?php

namespace Keboola\StagingProvider\Tests\Staging;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Staging\Workspace\AbsWorkspaceStaging;
use Keboola\StagingProvider\Staging\Workspace\ExasolWorkspaceStaging;
use Keboola\StagingProvider\Staging\Workspace\RedshiftWorkspaceStaging;
use Keboola\StagingProvider\Staging\Workspace\SnowflakeWorkspaceStaging;
use Keboola\StagingProvider\Staging\Workspace\SynapseWorkspaceStaging;
use Keboola\StagingProvider\Staging\Workspace\TeradataWorkspaceStaging;
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
        yield 'exasol' => [ExasolWorkspaceStaging::class, 'exasol'];
        yield 'teradata' => [TeradataWorkspaceStaging::class, 'teradata'];
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

    public function testBackendSizeIsReturned()
    {
        $backendSize = 'large';
        $workspace = new SnowflakeWorkspaceStaging([
            'backendSize' => $backendSize,
            'connection' => [
                'backend' => SnowflakeWorkspaceStaging::getType(),
            ]
        ]);

        self::assertSame($backendSize, $workspace->getBackendSize());
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
