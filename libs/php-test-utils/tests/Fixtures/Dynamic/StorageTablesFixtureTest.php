<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Tests\Fixtures\Dynamic;

use Keboola\PhpTestUtils\Fixtures\Dynamic\StorageTablesFixture;
use Keboola\PhpTestUtils\TestEnvVarsTrait;
use PHPUnit\Framework\TestCase;

class StorageTablesFixtureTest extends TestCase
{
    use TestEnvVarsTrait;

    public function testInitializeCreatesTableAndCleanupDeletesBucket(): void
    {
        $fixture = new StorageTablesFixture();
        $fixture->createStorageClientWrapper(
            self::getRequiredEnv('HOSTNAME_SUFFIX'),
            self::getRequiredEnv('TEST_STORAGE_API_TOKEN_SNOWFLAKE'),
        );

        $fixture->initialize();

        $bucketId = $fixture->getBucketId();
        $tableId = $fixture->getTableId();
        $defaultBranchId = $fixture->getDefaultBranchId();

        self::assertNotEmpty($bucketId);
        self::assertNotEmpty($tableId);
        self::assertSame(
            $fixture->getStorageClientWrapper()->getDefaultBranch()->id,
            $defaultBranchId,
        );

        $client = $fixture->getStorageClientWrapper()->getClientForDefaultBranch();

        self::assertTrue($client->bucketExists($bucketId));
        self::assertTrue($client->tableExists($tableId));

        // Cleanup and verify bucket removal
        $fixture->cleanUp();

        self::assertFalse($client->bucketExists($bucketId));
    }
}
