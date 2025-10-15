<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Tests\Fixtures\Dynamic;

use Keboola\PhpTestUtils\AssertArrayPropertySameTrait;
use Keboola\PhpTestUtils\Fixtures\Dynamic\ConfigurationFixture;
use Keboola\PhpTestUtils\TestEnvVarsTrait;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use PHPUnit\Framework\TestCase;

class ConfigurationFixtureTest extends TestCase
{
    use TestEnvVarsTrait;
    use AssertArrayPropertySameTrait;

    public function testInitializeCreatesConfigurationAndCleanupDeletesIt(): void
    {
        $fixture = new ConfigurationFixture();
        $fixture->createStorageClientWrapper(
            self::getRequiredEnv('HOSTNAME_SUFFIX'),
            self::getRequiredEnv('TEST_STORAGE_API_TOKEN_SNOWFLAKE'),
        );

        $fixture->initialize();

        $configurationId = $fixture->getConfigurationId();
        self::assertNotEmpty($configurationId);

        $componentId = $fixture->getComponentId();
        self::assertSame('keboola.runner-config-test', $componentId);

        $defaultBranchId = $fixture->getDefaultBranchId();
        self::assertSame(
            $fixture->getStorageClientWrapper()->getDefaultBranch()->id,
            $defaultBranchId,
        );

        $componentsApi = new Components($fixture->getStorageClientWrapper()->getClientForDefaultBranch());

        // Verify configuration exists
        $configuration = $componentsApi->getConfiguration($componentId, $configurationId);
        self::assertArrayPropertySame($configurationId, $configuration, 'id');

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
