<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Tests\Fixtures\Dynamic;

use Keboola\PhpTestUtils\AssertArrayPropertySameTrait;
use Keboola\PhpTestUtils\Fixtures\Dynamic\ConfigurationWithRowFixture;
use Keboola\PhpTestUtils\TestEnvVarsTrait;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use PHPUnit\Framework\TestCase;

class ConfigurationWithRowFixtureTest extends TestCase
{
    use TestEnvVarsTrait;
    use AssertArrayPropertySameTrait;

    public function testInitializeCreatesResourcesAndCleanupDeletesThem(): void
    {
        $fixture = new ConfigurationWithRowFixture();
        $fixture->createStorageClientWrapper(
            self::getRequiredEnv('HOSTNAME_SUFFIX'),
            self::getRequiredEnv('TEST_STORAGE_API_TOKEN_SNOWFLAKE'),
        );

        $fixture->initialize();

        $configurationId = $fixture->getConfigurationId();
        $componentId = $fixture->getComponentId();
        $defaultBranchId = $fixture->getDefaultBranchId();
        $rowId = $fixture->getRowId();

        self::assertNotEmpty($configurationId);
        self::assertSame('keboola.runner-config-test', $componentId);
        self::assertSame(
            $fixture->getStorageClientWrapper()->getDefaultBranch()->id,
            $defaultBranchId,
        );
        self::assertNotEmpty($rowId);

        // Verify configuration exists and row exists
        $componentsApi = new Components($fixture->getStorageClientWrapper()->getClientForDefaultBranch());
        $configuration = $componentsApi->getConfiguration($componentId, $configurationId);
        self::assertArrayPropertySame($configurationId, $configuration, 'id');

        $row = $componentsApi->getConfigurationRow($componentId, $configurationId, $rowId);
        self::assertArrayPropertySame($rowId, $row, 'id');

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
    }
}
