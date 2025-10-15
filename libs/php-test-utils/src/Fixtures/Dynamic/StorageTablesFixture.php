<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Fixtures\Dynamic;

use Keboola\Csv\CsvFile;
use Keboola\PhpTestUtils\Fixtures\FixtureTraits\StorageApiAwareTrait;

class StorageTablesFixture implements FixtureInterface
{
    use StorageApiAwareTrait;

    private string $bucketId;
    private string $tableId;
    private string $defaultBranchId;

    public function initialize(): void
    {
        $storageClient = $this->getStorageClientWrapper()->getClientForDefaultBranch();
        $this->bucketId = $storageClient->createBucket(uniqid('AutomationConfigurationWithTables', false), 'in');

        $this->tableId = $storageClient->createTableDefinition($this->bucketId, [
            'name' => uniqid('customer'),
            'primaryKeysNames' => ['id'],
            'columns' => [
                [
                    'name' => 'id',
                    'definition' => ['type' => 'INT'],
                ],
                [
                    'name' => 'name',
                    'definition' => ['type' => 'NVARCHAR'],
                ],
                [
                    'name' => 'email',
                    'definition' => ['type' => 'NVARCHAR'],
                ],
                [
                    'name' => 'address',
                    'definition' => ['type' => 'NVARCHAR', 'nullable' => true],
                ],
            ],
        ]);

        $storageClient->writeTableAsync(
            $this->tableId,
            new CsvFile(__DIR__ . '/../Static/customer.csv', skipLines: 0),
        );

        $this->defaultBranchId = $this->getStorageClientWrapper()->getDefaultBranch()->id;
    }

    public function cleanUp(): void
    {
        $client = $this->getStorageClientWrapper()->getClientForDefaultBranch();
        $client->dropBucket($this->bucketId, ['force' => true]);
    }

    public function getDefaultBranchId(): string
    {
        return $this->defaultBranchId;
    }

    public function getBucketId(): string
    {
        return $this->bucketId;
    }

    public function getTableId(): string
    {
        return $this->tableId;
    }
}
