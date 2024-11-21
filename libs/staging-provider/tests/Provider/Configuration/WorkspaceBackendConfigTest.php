<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider\Configuration;

use Generator;
use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\Configuration\NetworkPolicy;
use Keboola\StagingProvider\Provider\Configuration\WorkspaceBackendConfig;
use PHPUnit\Framework\TestCase;

class WorkspaceBackendConfigTest extends TestCase
{
    public function testGetters(): void
    {
        $config = new WorkspaceBackendConfig('workspace-snowflake', 'large', true, NetworkPolicy::SYSTEM);

        self::assertSame('workspace-snowflake', $config->getStagingType());
        self::assertSame('snowflake', $config->getStorageApiWorkspaceType());
        self::assertSame('large', $config->getStorageApiWorkspaceSize());
        self::assertSame(true, $config->getUseReadonlyRole());
        self::assertSame('system', $config->getNetworkPolicy());

        $config = new WorkspaceBackendConfig('workspace-snowflake', null, null, NetworkPolicy::USER);

        self::assertSame('workspace-snowflake', $config->getStagingType());
        self::assertSame('snowflake', $config->getStorageApiWorkspaceType());
        self::assertSame(null, $config->getStorageApiWorkspaceSize());
        self::assertSame(null, $config->getUseReadonlyRole());
        self::assertSame('user', $config->getNetworkPolicy());
    }

    /**
     * @dataProvider stagingTypeProvider
     */
    public function testGetStorageApiWorkspaceType(string $stagingType, string $expectedWorkspaceType): void
    {
        $config = new WorkspaceBackendConfig($stagingType, null, null, NetworkPolicy::SYSTEM);

        self::assertSame($stagingType, $config->getStagingType());
        self::assertSame($expectedWorkspaceType, $config->getStorageApiWorkspaceType());
    }

    public function stagingTypeProvider(): Generator
    {
        yield ['workspace-abs', 'abs'];
        yield ['workspace-bigquery', 'bigquery'];
        yield ['workspace-exasol', 'exasol'];
        yield ['workspace-redshift', 'redshift'];
        yield ['workspace-snowflake', 'snowflake'];
        yield ['workspace-synapse', 'synapse'];
        yield ['workspace-teradata', 'teradata'];
    }

    public function testGetInvalidStorageApiWorkspaceType(): void
    {
        $config = new WorkspaceBackendConfig('invalid', null, null, NetworkPolicy::SYSTEM);

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Unknown staging type "invalid"');
        $config->getStorageApiWorkspaceType();
    }
}
