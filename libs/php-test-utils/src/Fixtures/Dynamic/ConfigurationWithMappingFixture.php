<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Fixtures\Dynamic;

use Keboola\PhpTestUtils\Fixtures\FixtureTraits\StorageApiAwareTrait;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Components\Configuration;

class ConfigurationWithMappingFixture implements FixtureInterface
{
    use StorageApiAwareTrait;

    private const string TEST_COMPONENT_ID = 'keboola.runner-config-test';
    private string $configurationId;
    private string $componentId;
    private string $defaultBranchId;
    private string $bucketId;
    private string $tableId1;
    private string $tableId2;

    public function initialize(): void
    {
        $storageClient = $this->getStorageClientWrapper()->getClientForDefaultBranch();
        $this->bucketId = $storageClient->createBucket(uniqid('configuration-with-mapping-fixture', false), 'in');

        $this->tableId1= $storageClient->createTableDefinition($this->bucketId, [
            'name' => 'customer1',
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
        $this->tableId2 = $storageClient->createTableDefinition($this->bucketId, [
            'name' => 'orders1',
            'primaryKeysNames' => ['id'],
            'columns' => [
                [
                    'name' => 'id',
                    'definition' => ['type' => 'INT'],
                ],
                [
                    'name' => 'customerId',
                    'definition' => ['type' => 'INT'],
                ],
                [
                    'name' => 'amount',
                    'definition' => ['type' => 'INT'],
                ],
            ],
        ]);

        new Metadata($storageClient)->postColumnMetadata(
            $this->tableId1 . '.id',
            'user',
            [['key' => 'KBC.description', 'value' => 'ID of the customer']],
        );

        $componentsApi = new Components($this->getStorageClientWrapper()->getClientForDefaultBranch());
        $config = (new Configuration)
            ->setComponentId(self::TEST_COMPONENT_ID)
            ->setName(self::class)
            ->setConfiguration([
                'parameters' => ['foo' => 'bar'],
                'storage' => [
                    'input' => [
                        'tables' => [
                            [
                                'source' => $this->tableId1,
                                'destination' => 'customer',
                                'where_column' => '',
                                'where_values' => [],
                                'where_operator' => 'eq',
                                'columns' => [],
                                'keep_internal_timestamp_column' => false,
                            ],
                            [
                                'source' => $this->tableId2,
                                'destination' => 'orders',
                                'where_column' => '',
                                'where_values' => [],
                                'where_operator' => 'eq',
                                'columns' => [
                                    'id',
                                    'customerId',
                                    'amount',
                                ],
                                'keep_internal_timestamp_column' => false,
                            ],
                        ],
                    ],
                ],
            ]);
        $configuration = $componentsApi->addConfiguration($config);
        assert(is_array($configuration));
        assert(is_scalar($configuration['id']));
        $this->configurationId = (string) $configuration['id'];

        $this->componentId = self::TEST_COMPONENT_ID;
        $this->defaultBranchId = $this->getStorageClientWrapper()->getDefaultBranch()->id;
    }

    public function cleanUp(): void
    {
        $client = $this->getStorageClientWrapper()->getClientForDefaultBranch();
        $componentsApi = new Components($client);
        // delete
        $componentsApi->deleteConfiguration(self::TEST_COMPONENT_ID, $this->configurationId);
        // purge
        $componentsApi->deleteConfiguration(self::TEST_COMPONENT_ID, $this->configurationId);
        $client->dropBucket($this->bucketId, ['force' => true]);
    }

    public function getConfigurationId(): string
    {
        return $this->configurationId;
    }

    public function getComponentId(): string
    {
        return $this->componentId;
    }

    public function getDefaultBranchId(): string
    {
        return $this->defaultBranchId;
    }

    public function getBucketId(): string
    {
        return $this->bucketId;
    }

    public function getTableId1(): string
    {
        return $this->tableId1;
    }

    public function getTableId2(): string
    {
        return $this->tableId2;
    }
}
