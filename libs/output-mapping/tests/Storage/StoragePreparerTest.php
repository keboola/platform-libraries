<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Keboola\OutputMapping\Mapping\MappingStorageSources;
use Keboola\OutputMapping\Storage\StoragePreparer;
use Keboola\OutputMapping\Storage\TableChangesStore;
use Keboola\OutputMapping\Storage\TableInfo;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\OutputMapping\Tests\Needs\NeedsRemoveBucket;
use Keboola\OutputMapping\Tests\Needs\NeedsTestTables;

class StoragePreparerTest extends AbstractTestCase
{
    #[NeedsRemoveBucket('in.c-main')]
    public function testPrepareBucket(): void
    {
        $expectedBucketId = 'in.c-main';
        $storagePreparer = new StoragePreparer(
            clientWrapper: $this->clientWrapper,
            logger: $this->testLogger,
            hasNewNativeTypeFeature: false,
            hasBigQueryNativeTypesFeature: false,
        );

        self::assertFalse($this->clientWrapper->getTableAndFileStorageClient()->bucketExists($expectedBucketId));

        $mappingStorageSources = $storagePreparer->prepareStorageBucketAndTable(
            $this->createMappingFromProcessedConfiguration(),
            $this->createSystemMetadata(),
            new TableChangesStore(),
        );

        self::assertTrue($this->clientWrapper->getTableAndFileStorageClient()->bucketExists($expectedBucketId));
        self::assertStorageSourcesContainBucket($expectedBucketId, $mappingStorageSources);
        self::assertNull($mappingStorageSources->getTable());
    }

    #[NeedsTestTables(1)]
    public function testPrepareStorageWithExistingTable(): void
    {
        $storagePreparer = new StoragePreparer(
            clientWrapper: $this->clientWrapper,
            logger: $this->testLogger,
            hasNewNativeTypeFeature: false,
            hasBigQueryNativeTypesFeature: false,
        );

        self::assertTrue(
            $this->clientWrapper
                ->getTableAndFileStorageClient()
                ->bucketExists($this->testBucketId),
        );
        $table = $this->clientWrapper
            ->getTableAndFileStorageClient()
            ->getTable($this->firstTableId);

        $mappingStorageSources = $storagePreparer->prepareStorageBucketAndTable(
            $this->createMappingFromProcessedConfiguration([
                'destination' => $this->firstTableId,
            ]),
            $this->createSystemMetadata(),
            new TableChangesStore(),
        );

        $updatedTable = $this->clientWrapper
            ->getTableAndFileStorageClient()
            ->getTable($this->firstTableId);

        self::assertEquals($table, $updatedTable);

        self::assertStorageSourcesContainBucket($this->testBucketId, $mappingStorageSources);
        self::assertNotNull($mappingStorageSources->getTable());
        self::assertTableInfoEquals(
            $this->firstTableId,
            [
                'Id',
                'Name',
                'foo',
                'bar',
            ],
            [],
            false,
            $mappingStorageSources->getTable(),
        );
    }

    #[NeedsTestTables(1)]
    public function testPrepareStorageWithChangeTableStructure(): void
    {
        $storagePreparer = new StoragePreparer(
            clientWrapper: $this->clientWrapper,
            logger: $this->testLogger,
            hasNewNativeTypeFeature: false,
            hasBigQueryNativeTypesFeature: false,
        );

        $storagePreparer->prepareStorageBucketAndTable(
            $this->createMappingFromProcessedConfiguration([
                'destination' => $this->firstTableId,
            ]),
            $this->createSystemMetadata(),
            new TableChangesStore(),
        );

        $table = $this->clientWrapper
            ->getTableAndFileStorageClient()
            ->getTable($this->firstTableId);

        $mappingStorageSources = $storagePreparer->prepareStorageBucketAndTable(
            $this->createMappingFromProcessedConfiguration([
                'destination' => $this->firstTableId,
                'columns' => array_merge($table['columns'], [
                    'newColumn',
                ]),
                'primary_key' => array_merge($table['primaryKey'], [
                    'Id',
                ]),
            ]),
            $this->createSystemMetadata(),
            new TableChangesStore(),
        );

        $updatedTable = $this->clientWrapper
            ->getTableAndFileStorageClient()
            ->getTable($this->firstTableId);

        $expectedTable = array_merge_recursive($table, [
            'columns' => ['newColumn'],
            'primaryKey' => ['Id'],
        ]);

        self::assertEquals($this->dropTimestampParams($expectedTable), $this->dropTimestampParams($updatedTable));

        self::assertStorageSourcesContainBucket($this->testBucketId, $mappingStorageSources);
        self::assertNotNull($mappingStorageSources->getTable());
        self::assertTableInfoEquals(
            $this->firstTableId,
            [
                'Id',
                'Name',
                'foo',
                'bar',
                'newColumn',
            ],
            ['Id'],
            false,
            $mappingStorageSources->getTable(),
        );
    }

    #[NeedsTestTables(1)]
    public function testPrepareStorageWithChangeTableData(): void
    {
        $storagePreparer = new StoragePreparer(
            clientWrapper: $this->clientWrapper,
            logger: $this->testLogger,
            hasNewNativeTypeFeature: false,
            hasBigQueryNativeTypesFeature: false,
        );

        $storagePreparer->prepareStorageBucketAndTable(
            $this->createMappingFromProcessedConfiguration([
                'destination' => $this->firstTableId,
            ]),
            $this->createSystemMetadata(),
            new TableChangesStore(),
        );

        $table = $this->clientWrapper
            ->getTableAndFileStorageClient()
            ->getTable($this->firstTableId);

        $mappingStorageSources = $storagePreparer->prepareStorageBucketAndTable(
            $this->createMappingFromProcessedConfiguration([
                'destination' => $this->firstTableId,
                'delete_where_column' => 'Id',
                'delete_where_operator' => 'eq',
                'delete_where_values' => ['id1', 'id2'],
            ]),
            $this->createSystemMetadata(),
            new TableChangesStore(),
        );

        $updatedTable = $this->clientWrapper
            ->getTableAndFileStorageClient()
            ->getTable($this->firstTableId);

        $expectedTable = $table;
        $expectedTable['rowsCount'] -= 2;
        $expectedTable['bucket']['rowsCount'] -= 2;

        self::assertEquals($this->dropTimestampParams($expectedTable), $this->dropTimestampParams($updatedTable));

        self::assertStorageSourcesContainBucket($this->testBucketId, $mappingStorageSources);
        self::assertNotNull($mappingStorageSources->getTable());
        self::assertTableInfoEquals(
            $this->firstTableId,
            [
                'Id',
                'Name',
                'foo',
                'bar',
            ],
            [],
            false,
            $mappingStorageSources->getTable(),
        );
    }

    #[NeedsTestTables(1)]
    public function testPrepareStorageWithChangeTableDataOnNewNativeTypeFeature(): void
    {
        $storagePreparer = new StoragePreparer(
            clientWrapper: $this->clientWrapper,
            logger: $this->testLogger,
            hasNewNativeTypeFeature: true,
            hasBigQueryNativeTypesFeature: false,
        );

        $table = $this->clientWrapper
            ->getTableAndFileStorageClient()
            ->getTable($this->firstTableId);

        $mappingStorageSources = $storagePreparer->prepareStorageBucketAndTable(
            $this->createMappingFromProcessedConfiguration([
                'schema' => [
                    [
                        'name' => 'Id',
                    ],
                    [
                        'name' => 'Name',
                    ],
                    [
                        'name' => 'Pokus',
                    ],
                ],
                'destination' => $this->firstTableId,
                'delete_where_column' => 'Id',
                'delete_where_operator' => 'eq',
                'delete_where_values' => ['id1', 'id2'],
            ]),
            $this->createSystemMetadata(),
            new TableChangesStore(),
        );

        $updatedTable = $this->clientWrapper
            ->getTableAndFileStorageClient()
            ->getTable($this->firstTableId);

        $expectedTable = $table;
        $expectedTable['rowsCount'] -= 2;
        $expectedTable['bucket']['rowsCount'] -= 2;

        self::assertEquals($this->dropTimestampParams($expectedTable), $this->dropTimestampParams($updatedTable));

        self::assertStorageSourcesContainBucket($this->testBucketId, $mappingStorageSources);
        self::assertNotNull($mappingStorageSources->getTable());
        self::assertTableInfoEquals(
            $this->firstTableId,
            [
                'Id',
                'Name',
                'foo',
                'bar',
            ],
            [],
            false,
            $mappingStorageSources->getTable(),
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testPrepareStorageWithNewColumnOnNewNativeTypeFeature(): void
    {
        $tableId = $this->emptyOutputBucketId . '.test1';

        $this->clientWrapper->getTableAndFileStorageClient()->createTableDefinition($this->emptyOutputBucketId, [
            'name' => 'test1',
            'primaryKeysNames' => [],
            'columns' => [
                [
                    'name' => 'Id',
                    'basetype' => 'STRING',
                ],
                [
                    'name' => 'Name',
                    'basetype' => 'STRING',
                ],
            ],
        ]);

        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($tableId);

        $storagePreparer = new StoragePreparer(
            clientWrapper: $this->clientWrapper,
            logger: $this->testLogger,
            hasNewNativeTypeFeature: true,
            hasBigQueryNativeTypesFeature: false,
        );

        $tableChangeStorage = new TableChangesStore();
        $tableChangeStorage->addMissingColumn(new MappingFromConfigurationSchemaColumn([
            'name' => 'newColumn',
            'data_type' => [
                'base' => [
                    'type' => 'STRING',
                ],
            ],
        ]));

        $mappingStorageSources = $storagePreparer->prepareStorageBucketAndTable(
            $this->createMappingFromProcessedConfiguration([
                'destination' => $tableId,
                'schema' => [
                    [
                        'name' => 'Id',
                        'data_type' => [
                            'base' => [
                                'type' => 'STRING',
                            ],
                        ],
                    ],
                    [
                        'name' => 'Name',
                        'data_type' => [
                            'base' => [
                                'type' => 'STRING',
                            ],
                        ],
                    ],
                    [
                        'name' => 'newColumn',
                        'data_type' => [
                            'base' => [
                                'type' => 'STRING',
                            ],
                        ],
                    ],
                ],
            ]),
            $this->createSystemMetadata(),
            $tableChangeStorage,
        );

        $updatedTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($tableId);
        $updatedTable['columnMetadata']['newColumn'] = array_map(
            fn($v) => $this->dropTimestampParams($v),
            $updatedTable['columnMetadata']['newColumn'],
        );

        $expectedTables = $table;
        $expectedTables['columns'][] = 'newColumn';
        $expectedTables['columnMetadata']['newColumn'] = [
            [
                'key' => 'KBC.datatype.type',
                'value' => 'VARCHAR',
                'provider' => 'storage',

            ],
            [
                'key' => 'KBC.datatype.nullable',
                'value' => '1',
                'provider' => 'storage',

            ],
            [
                'key' => 'KBC.datatype.basetype',
                'value' => 'STRING',
                'provider' => 'storage',

            ],
            [
                'key' => 'KBC.datatype.length',
                'value' => '16777216',
                'provider' => 'storage',
            ],
        ];
        $expectedTables['definition']['columns'][] = [
            'name' => 'newColumn',
            'definition' => [
                'type' => 'VARCHAR',
                'nullable' => true,
                'length' => '16777216',
            ],
            'basetype' => 'STRING',
            'canBeFiltered' => true,
        ];

        self::assertEquals($this->dropTimestampParams($expectedTables), $this->dropTimestampParams($updatedTable));

        self::assertStorageSourcesContainBucket($this->emptyOutputBucketId, $mappingStorageSources);
        self::assertNotNull($mappingStorageSources->getTable());
        self::assertTableInfoEquals(
            $expectedTables['id'],
            [
                'Id',
                'Name',
                'newColumn',
            ],
            [],
            true,
            $mappingStorageSources->getTable(),
        );
    }

    private function createMappingFromProcessedConfiguration(array $newMapping = []): MappingFromProcessedConfiguration
    {
        $mapping = array_merge([
            'destination' => 'in.c-main.table',
            'delimiter' => ',',
            'enclosure' => '"',
        ], $newMapping);

        $source = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);

        return new MappingFromProcessedConfiguration(
            $mapping,
            $source,
        );
    }

    private function createSystemMetadata(): SystemMetadata
    {
        return new SystemMetadata([
            'componentId' => 'keboola.output-mapping',
        ]);
    }

    private function dropTimestampParams(array $table): array
    {
        unset($table['id']);
        unset($table['timestamp']);
        unset($table['lastChangeDate']);
        unset($table['bucket']['lastChangeDate']);
        return $table;
    }

    private static function assertStorageSourcesContainBucket(
        string $expectedBucketId,
        MappingStorageSources $storageSources,
    ): void {
        self::assertNotNull($storageSources->getBucket());
        self::assertSame($expectedBucketId, $storageSources->getBucket()->id);
    }

    private static function assertTableInfoEquals(
        string $expectedTableId,
        array $expectedColumns,
        array $expectedPrimaryKey,
        bool $expectedIsTyped,
        TableInfo $tableInfo,
    ): void {
        self::assertSame($expectedTableId, $tableInfo->getId());
        self::assertSame($expectedColumns, $tableInfo->getColumns());
        self::assertSame($expectedPrimaryKey, $tableInfo->getPrimaryKey());
        self::assertSame($expectedIsTyped, $tableInfo->isTyped());
    }
}
