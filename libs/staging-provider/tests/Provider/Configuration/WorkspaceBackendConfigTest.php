<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider\Configuration;

use Generator;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\Configuration\NetworkPolicy;
use Keboola\StagingProvider\Provider\Configuration\WorkspaceBackendConfig;
use Keboola\StorageApi\WorkspaceLoginType;
use PHPUnit\Framework\TestCase;

class WorkspaceBackendConfigTest extends TestCase
{
    public function testGetters(): void
    {
        $config = new WorkspaceBackendConfig(
            'workspace-snowflake',
            'large',
            true,
            NetworkPolicy::SYSTEM,
            WorkspaceLoginType::SNOWFLAKE_PERSON_SSO,
            null,
        );

        self::assertSame('workspace-snowflake', $config->getStagingType());
        self::assertSame('snowflake', $config->getStorageApiWorkspaceType());
        self::assertSame('large', $config->getStorageApiWorkspaceSize());
        self::assertSame(true, $config->getUseReadonlyRole());
        self::assertSame('system', $config->getNetworkPolicy());
        self::assertSame(WorkspaceLoginType::SNOWFLAKE_PERSON_SSO, $config->getLoginType());
        self::assertNull($config->getPublicKey());

        $config = new WorkspaceBackendConfig(
            'workspace-snowflake',
            null,
            null,
            NetworkPolicy::USER,
            WorkspaceLoginType::SNOWFLAKE_PERSON_KEYPAIR,
            'public-key',
        );

        self::assertSame('workspace-snowflake', $config->getStagingType());
        self::assertSame('snowflake', $config->getStorageApiWorkspaceType());
        self::assertSame(null, $config->getStorageApiWorkspaceSize());
        self::assertSame(null, $config->getUseReadonlyRole());
        self::assertSame('user', $config->getNetworkPolicy());
        self::assertSame(WorkspaceLoginType::SNOWFLAKE_PERSON_KEYPAIR, $config->getLoginType());
        self::assertSame('public-key', $config->getPublicKey());
    }

    /**
     * @param AbstractStrategyFactory::WORKSPACE_* $stagingType
     * @dataProvider stagingTypeProvider
     */
    public function testGetStorageApiWorkspaceType(string $stagingType, string $expectedWorkspaceType): void
    {
        $config = new WorkspaceBackendConfig(
            $stagingType,
            null,
            null,
            NetworkPolicy::SYSTEM,
            null,
            null,
        );

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
        $config = new WorkspaceBackendConfig(
            'invalid', // @phpstan-ignore-line we're testing invalid value
            null,
            null,
            NetworkPolicy::SYSTEM,
            null,
            null,
        );

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Unknown staging type "invalid"');
        $config->getStorageApiWorkspaceType();
    }
}
