<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Staging;

use Generator;
use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Staging\Workspace\AbsWorkspaceStaging;
use Keboola\StagingProvider\Staging\Workspace\BigQueryWorkspaceStaging;
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
     * @param class-string $workspaceClass
     */
    public function testBackendTypeIsChecked(string $workspaceClass, string $expectedType): void
    {
        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage(sprintf(
            'Backend configuration does not match the workspace type. Expected "%s", got "dummy"',
            $expectedType,
        ));

        new $workspaceClass([
            'id' => 'test',
            'connection' => [
                'backend' => 'dummy',
            ],
        ]);
    }

    public function provideWorkspacesWithTypes(): Generator
    {
        yield 'snowflake' => [SnowflakeWorkspaceStaging::class, 'snowflake'];
        yield 'synapse' => [SynapseWorkspaceStaging::class, 'synapse'];
        yield 'redshift' => [RedshiftWorkspaceStaging::class, 'redshift'];
        yield 'abs' => [AbsWorkspaceStaging::class, 'abs'];
        yield 'exasol' => [ExasolWorkspaceStaging::class, 'exasol'];
        yield 'teradata' => [TeradataWorkspaceStaging::class, 'teradata'];
        yield 'bigquery' => [BigQueryWorkspaceStaging::class, 'bigquery'];
    }

    public function testWorkspaceIdIsReturned(): void
    {
        $workspaceId = 'test';
        $workspace = new SnowflakeWorkspaceStaging([
            'id' => $workspaceId,
            'connection' => [
                'backend' => SnowflakeWorkspaceStaging::getType(),
            ],
        ]);

        self::assertSame($workspaceId, $workspace->getWorkspaceId());
    }

    public function testBackendSizeIsReturned(): void
    {
        $backendSize = 'large';
        $workspace = new SnowflakeWorkspaceStaging([
            'backendSize' => $backendSize,
            'connection' => [
                'backend' => SnowflakeWorkspaceStaging::getType(),
            ],
        ]);

        self::assertSame($backendSize, $workspace->getBackendSize());
    }

    public function testCredentialsAreReturned(): void
    {
        $credentials = [
            'host' => 'host',
            'warehouse' => 'warehouse',
            'database' => 'database',
            'schema' => 'schema',
            'user' => 'user',
            'password' => 'password',
            'account' => 'host',
        ];

        $workspace = new SnowflakeWorkspaceStaging([
            'connection' => $credentials + [
                'backend' => SnowflakeWorkspaceStaging::getType(),
                'extra' => 'dummy',
            ],
        ]);

        self::assertSame($credentials, $workspace->getCredentials());
    }
}
