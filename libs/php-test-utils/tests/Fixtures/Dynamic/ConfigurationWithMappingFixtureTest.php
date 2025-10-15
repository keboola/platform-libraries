<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Tests\Fixtures\Dynamic;

use Keboola\PhpTestUtils\AssertArrayPropertySameTrait;
use Keboola\PhpTestUtils\Fixtures\Dynamic\ConfigurationWithMappingFixture;
use Keboola\PhpTestUtils\TestEnvVarsTrait;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use PHPUnit\Framework\TestCase;

class ConfigurationWithMappingFixtureTest extends TestCase
{
    use TestEnvVarsTrait;
    use AssertArrayPropertySameTrait;

    public function testInitializeCreatesResourcesAndCleanupDeletesThem(): void
    {
        $fixture = new ConfigurationWithMappingFixture();
        $fixture->createStorageClientWrapper(
            self::getRequiredEnv('HOSTNAME_SUFFIX'),
            self::getRequiredEnv('TEST_STORAGE_API_TOKEN_SNOWFLAKE'),
        );

        $fixture->initialize();

        // IDs from fixture
        $configurationId = $fixture->getConfigurationId();
        $componentId = $fixture->getComponentId();
        $defaultBranchId = $fixture->getDefaultBranchId();
        $bucketId = $fixture->getBucketId();
        $tableId1 = $fixture->getTableId1();
        $tableId2 = $fixture->getTableId2();

        self::assertNotEmpty($configurationId);
        self::assertSame('keboola.runner-config-test', $componentId);
        self::assertSame(
            $fixture->getStorageClientWrapper()->getDefaultBranch()->id,
            $defaultBranchId,
        );
        self::assertNotEmpty($bucketId);
        self::assertNotEmpty($tableId1);
        self::assertNotEmpty($tableId2);

        // Verify configuration exists directly
        $componentsApi = new Components($fixture->getStorageClientWrapper()->getClientForDefaultBranch());
        $configuration = $componentsApi->getConfiguration($componentId, $configurationId);
        self::assertArrayPropertySame($configurationId, $configuration, 'id');

        // Verify bucket and tables exist
        $client = $fixture->getStorageClientWrapper()->getClientForDefaultBranch();
        self::assertTrue($client->bucketExists($bucketId));
        self::assertTrue($client->tableExists($tableId1));
        self::assertTrue($client->tableExists($tableId2));

        // Cleanup
        $fixture->cleanUp();

        // Verify configuration removed
        try {
            $componentsApi->getConfiguration($componentId, $configurationId);
            self::fail('Configuration should be deleted.');
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }

        // Verify bucket removed
        self::assertFalse($client->bucketExists($bucketId));
    }
}
