<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Tests\Fixtures\Dynamic;

use Keboola\PhpTestUtils\AssertArrayPropertySameTrait;
use Keboola\PhpTestUtils\Fixtures\Dynamic\SharedCodeFixture;
use Keboola\PhpTestUtils\TestEnvVarsTrait;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use PHPUnit\Framework\TestCase;

class SharedCodeFixtureTest extends TestCase
{
    use TestEnvVarsTrait;
    use AssertArrayPropertySameTrait;

    public function testInitializeCreatesResourcesAndCleanupDeletesThem(): void
    {
        $fixture = new SharedCodeFixture();
        $fixture->createStorageClientWrapper(
            self::getRequiredEnv('HOSTNAME_SUFFIX'),
            self::getRequiredEnv('TEST_STORAGE_API_TOKEN_SNOWFLAKE'),
        );

        $fixture->initialize();

        $configId = $fixture->getConfigId();
        $rowId = $fixture->getConfigRowId();

        self::assertNotEmpty($configId);
        self::assertNotEmpty($rowId);

        // Verify configuration exists under keboola.shared-code
        $componentsApi = new Components($fixture->getStorageClientWrapper()->getClientForDefaultBranch());
        $configuration = $componentsApi->getConfiguration('keboola.shared-code', $configId);
        self::assertArrayPropertySame($configId, $configuration, 'id');

        // Verify the configuration row exists
        $row = $componentsApi->getConfigurationRow('keboola.shared-code', $configId, $rowId);
        self::assertArrayPropertySame($rowId, $row, 'id');

        // Cleanup
        $fixture->cleanUp();

        // Verify configuration removed
        try {
            $componentsApi->getConfiguration('keboola.shared-code', $configId);
            self::fail('Configuration should be deleted.');
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }
    }
}
