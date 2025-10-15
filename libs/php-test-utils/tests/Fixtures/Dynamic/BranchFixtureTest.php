<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Tests\Fixtures\Dynamic;

use Keboola\PhpTestUtils\AssertArrayPropertySameTrait;
use Keboola\PhpTestUtils\Fixtures\Dynamic\BranchFixture;
use Keboola\PhpTestUtils\TestEnvVarsTrait;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use PHPUnit\Framework\TestCase;

class BranchFixtureTest extends TestCase
{
    use TestEnvVarsTrait;
    use AssertArrayPropertySameTrait;

    public function testInitializeCreatesBranchAndCleanupDeletesIt(): void
    {
        $fixture = new BranchFixture();
        $fixture->createStorageClientWrapper(
            self::getRequiredEnv('HOSTNAME_SUFFIX'),
            self::getRequiredEnv('TEST_STORAGE_API_TOKEN_SNOWFLAKE'),
        );

        $fixture->initialize();
        $branchId = $fixture->getBranchId();

        self::assertNotEmpty($branchId);

        $branchesApi = new DevBranches($fixture->getStorageClientWrapper()->getBasicClient());
        $branch = $branchesApi->getBranch((int) $branchId);
        self::assertArrayPropertySame((int) $branchId, $branch, 'id');

        $fixture->cleanUp();

        // verify it no longer exists
        try {
            $branchesApi->getBranch((int) $branchId);
            self::fail('Branch should be deleted.');
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }
    }
}
