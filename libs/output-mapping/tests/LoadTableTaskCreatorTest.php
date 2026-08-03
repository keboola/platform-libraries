<?php

declare(strict_types=1);

namespace Keboola\OutputMapping;

use Generator;
use Keboola\OutputMapping\Configuration\Table\DeduplicationStrategy;
use Keboola\OutputMapping\DeferredTasks\TableWriter\CreateAndLoadTableTask;
use Keboola\OutputMapping\DeferredTasks\TableWriter\LoadTableTask;
use Keboola\OutputMapping\Mapping\MappingColumnMetadata;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Keboola\OutputMapping\Mapping\MappingStorageSources;
use Keboola\OutputMapping\Storage\BucketInfo;
use Keboola\OutputMapping\Storage\TableDescription;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\OutputMapping\Tests\Needs\NeedsTestTables;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\OutputMapping\Writer\Table\Strategy\LocalTableStrategy;

class LoadTableTaskCreatorTest extends AbstractTestCase
{

    #[NeedsEmptyOutputBucket]
    public function testNativeTypeLoadTaskTableNotExists(): void
    {
        $settings = self::createMock(OutputMappingSettings::class);
        $settings->expects(self::once())->method('hasNativeTypesFeature')->willReturn(true);
        $settings->expects(self::once())->method('hasBigqueryNativeTypesFeature')->willReturn(false);

        $strategy = self::createMock(LocalTableStrategy::class);
        $strategy->expects(self::once())
            ->method('prepareLoadTaskOptions')
            ->willReturn([]);

        $source = self::createMock(MappingFromProcessedConfiguration::class);
        $source->expects(self::once())->method('hasColumns')->willReturn(true);
        $source->expects(self::once())->method('hasColumnMetadata')->willReturn(true);
        $source->expects(self::once())->method('hasMetadata')->willReturn(false);
        $source->expects(self::exactly(3))->method('getDestination')->willReturn(
            new MappingDestination($this->emptyOutputBucketId . '.destinationTable'),
        );
        $source->expects(self::exactly(2))->method('getPrimaryKey')->willReturn([]);
        $source->expects(self::once())->method('getColumnMetadata')->willReturn([
            new MappingColumnMetadata('col1', [
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'STRING',
                ],
            ]),
            new MappingColumnMetadata('col2', [
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'INTEGER',
                ],
            ]),
        ]);

        $storageSources = self::createMock(MappingStorageSources::class);
        $storageSources->expects(self::exactly(2))->method('didTableExistBefore')->willReturn(false);
        $storageSources->expects(self::once())->method('getBucket')->willReturn(
            new BucketInfo([
                'id' => $this->emptyOutputBucketId,
                'backend' => 'Snowflake',
                'metadata' => [],
            ]),
        );

        $loadTableTaskCreator = new LoadTableTaskCreator(
            $this->clientWrapper,
            $this->testLogger,
        );
        $loadTaskResult = $loadTableTaskCreator->create(
            strategy: $strategy,
            source: $source,
            storageSources: $storageSources,
            settings: $settings,
            descriptions: new TableDescription(
                $this->emptyOutputBucketId . '.destinationTable',
                'table desc',
                ['col1' => 'col1 desc'],
            ),
        );

        // the descriptions are part of the create payload, nothing is left for after the load
        self::assertNull($loadTaskResult->getDescriptionsNotEmbeddedInCreatePayload());

        $loadTask = $loadTaskResult->getLoadTableTask();
        self::assertInstanceOf(LoadTableTask::class, $loadTask);
        self::assertTrue($loadTask->isUsingFreshlyCreatedTable());
        self::assertSame(
            $this->emptyOutputBucketId . '.destinationTable',
            $loadTask->getDestinationTableName(),
        );
        $storageTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId.'.destinationTable',
        );

        self::assertTrue($storageTable['isTyped']);
        self::assertSame('table desc', $storageTable['definition']['description'] ?? null);
        self::assertSame(
            ['col1' => 'col1 desc', 'col2' => null],
            $this->getColumnDescriptions($storageTable['definition']['columns']),
        );
        self::assertSame(
            [
                [
                    'name' => 'col1',
                    'definition' => [
                        'type' => 'VARCHAR',
                        'nullable' => true,
                        'length' => '16777216',
                    ],
                    'basetype' => 'STRING',
                    'canBeFiltered' => true,
                ],
                [
                    'name' => 'col2',
                    'definition' => [
                        'type' => 'NUMBER',
                        'nullable' => true,
                        'length' => '38,0',
                    ],
                    'basetype' => 'INTEGER',
                    'canBeFiltered' => true,
                ],
            ],
            $this->stripColumnDescriptions($storageTable['definition']['columns']),
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testNewNativeTypeLoadTaskTableNotExists(): void
    {
        $settings = self::createMock(OutputMappingSettings::class);
        $settings->expects(self::once())->method('hasNativeTypesFeature')->willReturn(false);
        $settings->expects(self::exactly(2))->method('hasNewNativeTypesFeature')->willReturn(true);

        $strategy = self::createMock(LocalTableStrategy::class);
        $strategy->expects(self::once())
            ->method('prepareLoadTaskOptions')
            ->willReturn([]);

        $source = self::createMock(MappingFromProcessedConfiguration::class);
        $source->expects(self::exactly(3))->method('getDestination')->willReturn(
            new MappingDestination($this->emptyOutputBucketId . '.destinationTable'),
        );
        $source->expects(self::exactly(6))->method('getSchema')->willReturn([
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col1',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
                        'length' => '16777216',
                    ],
                ],
                'primary_key' => true,
            ]),
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col2',
                'data_type' => [
                    'base' => [
                        'type' => 'INTEGER',
                    ],
                ],
            ]),
        ]);

        $storageSources = self::createMock(MappingStorageSources::class);
        $storageSources->expects(self::exactly(2))->method('didTableExistBefore')->willReturn(false);
        $storageSources->expects(self::once())->method('getBucket')->willReturn(
            new BucketInfo([
                'id' => $this->emptyOutputBucketId,
                'backend' => 'Snowflake',
                'metadata' => [],
            ]),
        );

        $loadTableTaskCreator = new LoadTableTaskCreator(
            $this->clientWrapper,
            $this->testLogger,
        );
        $loadTaskResult = $loadTableTaskCreator->create(
            strategy: $strategy,
            source: $source,
            storageSources: $storageSources,
            settings: $settings,
            descriptions: new TableDescription(
                $this->emptyOutputBucketId . '.destinationTable',
                'table desc',
                ['col2' => 'col2 desc'],
            ),
        );

        // the descriptions are part of the create payload, nothing is left for after the load
        self::assertNull($loadTaskResult->getDescriptionsNotEmbeddedInCreatePayload());

        $loadTask = $loadTaskResult->getLoadTableTask();
        self::assertInstanceOf(LoadTableTask::class, $loadTask);
        self::assertTrue($loadTask->isUsingFreshlyCreatedTable());
        self::assertSame(
            $this->emptyOutputBucketId . '.destinationTable',
            $loadTask->getDestinationTableName(),
        );
        $storageTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId.'.destinationTable',
        );

        self::assertTrue($storageTable['isTyped']);
        self::assertSame('table desc', $storageTable['definition']['description'] ?? null);
        self::assertSame(
            ['col1' => null, 'col2' => 'col2 desc'],
            $this->getColumnDescriptions($storageTable['definition']['columns']),
        );
        self::assertSame(
            [
                [
                    'name' => 'col1',
                    'definition' => [
                        'type' => 'VARCHAR',
                        'nullable' => false,
                        'length' => '16777216',
                    ],
                    'basetype' => 'STRING',
                    'canBeFiltered' => true,
                ],
                [
                    'name' => 'col2',
                    'definition' => [
                        'type' => 'NUMBER',
                        'nullable' => true,
                        'length' => '38,0',
                    ],
                    'basetype' => 'INTEGER',
                    'canBeFiltered' => true,
                ],
            ],
            $this->stripColumnDescriptions($storageTable['definition']['columns']),
        );
        self::assertSame(['col1'], $storageTable['definition']['primaryKeysNames']);
    }

    #[NeedsEmptyOutputBucket]
    public function testLoadTaskTableNotExists(): void
    {
        $settings = self::createMock(OutputMappingSettings::class);
        $settings->expects(self::once())->method('hasNativeTypesFeature')->willReturn(false);
        $settings->expects(self::exactly(2))->method('hasNewNativeTypesFeature')->willReturn(false);

        $strategy = self::createMock(LocalTableStrategy::class);
        $strategy->expects(self::once())
            ->method('prepareLoadTaskOptions')
            ->willReturn([]);

        $source = self::createMock(MappingFromProcessedConfiguration::class);
        $source->expects(self::once())->method('hasColumns')->willReturn(true);
        $source->expects(self::exactly(3))->method('getDestination')->willReturn(
            new MappingDestination($this->emptyOutputBucketId . '.destinationTable'),
        );
        // getPrimaryKey/getColumns are read twice - once for the load options, once for the table definition
        $source->expects(self::exactly(2))->method('getPrimaryKey')->willReturn([]);
        $source->expects(self::exactly(2))->method('getColumns')->willReturn(['col1', 'col2']);

        $storageSources = self::createMock(MappingStorageSources::class);
        $storageSources->expects(self::exactly(2))->method('didTableExistBefore')->willReturn(false);

        $loadTableTaskCreator = new LoadTableTaskCreator(
            $this->clientWrapper,
            $this->testLogger,
        );
        $loadTaskResult = $loadTableTaskCreator->create(
            strategy: $strategy,
            source: $source,
            storageSources: $storageSources,
            settings: $settings,
            descriptions: new TableDescription(
                $this->emptyOutputBucketId . '.destinationTable',
                'table desc',
                ['col1' => 'col1 desc'],
            ),
        );

        // the descriptions are part of the create payload, nothing is left for after the load
        self::assertNull($loadTaskResult->getDescriptionsNotEmbeddedInCreatePayload());

        $loadTask = $loadTaskResult->getLoadTableTask();
        self::assertInstanceOf(LoadTableTask::class, $loadTask);
        self::assertTrue($loadTask->isUsingFreshlyCreatedTable());
        self::assertSame(
            $this->emptyOutputBucketId . '.destinationTable',
            $loadTask->getDestinationTableName(),
        );
        $storageTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId.'.destinationTable',
        );

        // a column definition carrying only a description does not make the table typed
        self::assertFalse($storageTable['isTyped']);
        self::assertSame(
            ['col1', 'col2'],
            $storageTable['columns'],
        );
        self::assertSame('table desc', $storageTable['definition']['description'] ?? null);
        self::assertSame(
            ['col1' => 'col1 desc', 'col2' => null],
            $this->getColumnDescriptions($storageTable['definition']['columns']),
        );
    }

    #[NeedsTestTables(count: 1)]
    public function testLoadTaskTableExists(): void
    {
        $settings = self::createMock(OutputMappingSettings::class);
        $settings->expects(self::once())->method('hasNativeTypesFeature')->willReturn(false);
        $settings->expects(self::exactly(2))->method('hasNewNativeTypesFeature')->willReturn(false);

        $strategy = self::createMock(LocalTableStrategy::class);
        $strategy->expects(self::once())
            ->method('prepareLoadTaskOptions')
            ->willReturn([]);

        $source = self::createMock(MappingFromProcessedConfiguration::class);
        $source->expects(self::once())->method('getDestination')->willReturn(
            new MappingDestination($this->testBucketId . '.test0'),
        );
        $source->expects(self::once())->method('getPrimaryKey')->willReturn([]);
        $source->expects(self::once())->method('getColumns')->willReturn(['col1', 'col2']);

        $storageSources = self::createMock(MappingStorageSources::class);
        $storageSources->expects(self::exactly(3))->method('didTableExistBefore')->willReturn(true);

        $loadTableTaskCreator = new LoadTableTaskCreator(
            $this->clientWrapper,
            $this->testLogger,
        );
        $loadTaskResult = $loadTableTaskCreator->create(
            strategy: $strategy,
            source: $source,
            storageSources: $storageSources,
            settings: $settings,
            descriptions: new TableDescription($this->testBucketId . '.test0', 'table desc', []),
        );

        // the table existed before, its description was already handled by StoragePreparer
        self::assertNull($loadTaskResult->getDescriptionsNotEmbeddedInCreatePayload());

        $loadTask = $loadTaskResult->getLoadTableTask();
        self::assertInstanceOf(LoadTableTask::class, $loadTask);
        self::assertFalse($loadTask->isUsingFreshlyCreatedTable());
        self::assertSame(
            $this->testBucketId . '.test0',
            $loadTask->getDestinationTableName(),
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testLoadTaskTableNotExistsManifestNotExists(): void
    {
        $settings = self::createMock(OutputMappingSettings::class);
        $settings->expects(self::once())->method('hasNativeTypesFeature')->willReturn(false);
        $settings->expects(self::exactly(2))->method('hasNewNativeTypesFeature')->willReturn(false);

        $strategy = self::createMock(LocalTableStrategy::class);
        $strategy->expects(self::once())
            ->method('prepareLoadTaskOptions')
            ->willReturn([]);

        $source = self::createMock(MappingFromProcessedConfiguration::class);
        $source->expects(self::once())->method('hasColumns')->willReturn(false);
        $source->expects(self::once())->method('getDestination')->willReturn(
            new MappingDestination($this->emptyOutputBucketId . '.test0'),
        );
        $source->expects(self::once())->method('getPrimaryKey')->willReturn([]);

        $storageSources = self::createMock(MappingStorageSources::class);
        $storageSources->expects(self::exactly(3))->method('didTableExistBefore')->willReturn(false);

        $loadTableTaskCreator = new LoadTableTaskCreator(
            $this->clientWrapper,
            $this->testLogger,
        );
        $descriptions = new TableDescription(
            $this->emptyOutputBucketId . '.test0',
            'table desc',
            ['col1' => 'col1 desc'],
        );
        $loadTaskResult = $loadTableTaskCreator->create(
            strategy: $strategy,
            source: $source,
            storageSources: $storageSources,
            settings: $settings,
            descriptions: $descriptions,
        );

        // the table is created by the load job itself, there is no create payload to embed the descriptions in
        self::assertSame($descriptions, $loadTaskResult->getDescriptionsNotEmbeddedInCreatePayload());

        $loadTask = $loadTaskResult->getLoadTableTask();
        self::assertInstanceOf(CreateAndLoadTableTask::class, $loadTask);
        self::assertTrue($loadTask->isUsingFreshlyCreatedTable());
        self::assertSame(
            $this->emptyOutputBucketId . '.test0',
            $loadTask->getDestinationTableName(),
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testLoadTaskWithoutManifestAndWithoutDescription(): void
    {
        $settings = self::createMock(OutputMappingSettings::class);
        $settings->expects(self::once())->method('hasNativeTypesFeature')->willReturn(false);
        $settings->expects(self::exactly(2))->method('hasNewNativeTypesFeature')->willReturn(false);

        $strategy = self::createMock(LocalTableStrategy::class);
        $strategy->expects(self::once())
            ->method('prepareLoadTaskOptions')
            ->willReturn([]);

        $source = self::createMock(MappingFromProcessedConfiguration::class);
        $source->expects(self::once())->method('hasColumns')->willReturn(false);
        $source->expects(self::once())->method('getDestination')->willReturn(
            new MappingDestination($this->emptyOutputBucketId . '.test0'),
        );
        $source->expects(self::once())->method('getPrimaryKey')->willReturn([]);

        $storageSources = self::createMock(MappingStorageSources::class);
        $storageSources->expects(self::exactly(3))->method('didTableExistBefore')->willReturn(false);

        $loadTableTaskCreator = new LoadTableTaskCreator(
            $this->clientWrapper,
            $this->testLogger,
        );
        $loadTaskResult = $loadTableTaskCreator->create(
            strategy: $strategy,
            source: $source,
            storageSources: $storageSources,
            settings: $settings,
            descriptions: new TableDescription($this->emptyOutputBucketId . '.test0', null, []),
        );

        // there is no description at all, so there is nothing to store after the load either
        self::assertNull($loadTaskResult->getDescriptionsNotEmbeddedInCreatePayload());
        self::assertInstanceOf(CreateAndLoadTableTask::class, $loadTaskResult->getLoadTableTask());
    }

    /**
     * Storage returns the column description nested in the column definition, splitting it off keeps the
     * assertions of the column types independent of the position of the description key.
     *
     * @param array<mixed> $columns
     * @return array<string, string|null> column name => description
     */
    private function getColumnDescriptions(array $columns): array
    {
        $descriptions = [];
        foreach ($columns as $column) {
            self::assertIsArray($column);
            $columnName = $column['name'];
            self::assertIsString($columnName);

            $definition = $column['definition'] ?? [];
            self::assertIsArray($definition);
            $description = $definition['description'] ?? null;

            $descriptions[$columnName] = is_string($description) ? $description : null;
        }

        return $descriptions;
    }

    /**
     * @param array<mixed> $columns
     * @return list<mixed>
     */
    private function stripColumnDescriptions(array $columns): array
    {
        $strippedColumns = [];
        foreach ($columns as $column) {
            self::assertIsArray($column);

            $definition = $column['definition'] ?? null;
            if (is_array($definition)) {
                unset($definition['description']);
                $column['definition'] = $definition;
            }
            $strippedColumns[] = $column;
        }

        return $strippedColumns;
    }

    /**
     * @dataProvider buildLoadOptionsDataProvider
     */
    public function testBuildLoadOptions(
        array $sourceData,
        bool $didTableExistBefore,
        bool $hasNewNativeTypesFeature,
        ?array $treatValuesAsNullConfiguration,
        array $expectedLoadOptions,
    ): void {

        // not necessary for this test
        $strategy = self::createMock(LocalTableStrategy::class);
        $strategy->expects(self::once())
            ->method('prepareLoadTaskOptions')
            ->willReturn([]);

        $mappingStorageSources = self::createMock(MappingStorageSources::class);
        $mappingStorageSources->expects(self::once())
            ->method('didTableExistBefore')
            ->willReturn($didTableExistBefore);

        $loadTableTaskCreator = new LoadTableTaskCreator(
            $this->clientWrapper,
            $this->testLogger,
        );

        $source = new MappingFromProcessedConfiguration(
            $sourceData,
            $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class),
        );

        $loadOptions = $loadTableTaskCreator->buildLoadOptions(
            source: $source,
            strategy: $strategy,
            storageSources: $mappingStorageSources,
            hasNewNativeTypesFeature: $hasNewNativeTypesFeature,
            treatValuesAsNullConfiguration: $treatValuesAsNullConfiguration,
        );

        self::assertSame($expectedLoadOptions, $loadOptions);
    }

    public function buildLoadOptionsDataProvider(): Generator
    {
        yield 'no columns' => [
            'sourceData' => [
                'destination' => 'in.c-bucket.destinationTable',
                'delimiter' => ',',
            ],
            'didTableExistBefore' => false,
            'hasNewNativeTypesFeature' => false,
            'treatValuesAsNullConfiguration' => null,
            'expectedLoadOptions' => [
                'columns' => [],
                'primaryKey' => '',
                'incremental' => false,
            ],
        ];

        yield 'columns' => [
            'sourceData' => [
                'destination' => 'in.c-bucket.destinationTable',
                'delimiter' => ',',
                'columns' => ['col1', 'col2'],
                'primary_key' => ['col1'],
            ],
            'didTableExistBefore' => false,
            'hasNewNativeTypesFeature' => false,
            'treatValuesAsNullConfiguration' => null,
            'expectedLoadOptions' => [
                'columns' => ['col1', 'col2'],
                'primaryKey' => 'col1',
                'incremental' => false,
            ],
        ];

        yield 'distributionKey, tableNotExists' => [
            'sourceData' => [
                'destination' => 'in.c-bucket.destinationTable',
                'delimiter' => ',',
                'columns' => ['col1', 'col2'],
                'primary_key' => ['col1'],
                'distribution_key' => ['col2'],
            ],
            'didTableExistBefore' => false,
            'hasNewNativeTypesFeature' => false,
            'treatValuesAsNullConfiguration' => null,
            'expectedLoadOptions' => [
                'columns' => ['col1', 'col2'],
                'primaryKey' => 'col1',
                'incremental' => false,
                'distributionKey' => 'col2',
            ],
        ];

        yield 'distributionKey, tableExists' => [
            'sourceData' => [
                'destination' => 'in.c-bucket.destinationTable',
                'delimiter' => ',',
                'columns' => ['col1', 'col2'],
                'primary_key' => ['col1'],
                'distribution_key' => ['col2'],
            ],
            'didTableExistBefore' => true,
            'hasNewNativeTypesFeature' => false,
            'treatValuesAsNullConfiguration' => null,
            'expectedLoadOptions' => [
                'columns' => ['col1', 'col2'],
                'primaryKey' => 'col1',
                'incremental' => false,
            ],
        ];

        yield 'schema' => [
            'sourceData' => [
                'destination' => 'in.c-bucket.destinationTable',
                'delimiter' => ',',
                'schema' => [
                    ['name' => 'col1', 'primary_key' => true],
                    ['name' => 'col2'],
                ],
            ],
            'didTableExistBefore' => false,
            'hasNewNativeTypesFeature' => true,
            'treatValuesAsNullConfiguration' => null,
            'expectedLoadOptions' => [
                'columns' => ['col1', 'col2'],
                'primaryKey' => 'col1',
                'incremental' => false,
            ],
        ];

        yield 'schema, treat values as null' => [
            'sourceData' => [
                'destination' => 'in.c-bucket.destinationTable',
                'delimiter' => ',',
                'schema' => [
                    ['name' => 'col1', 'primary_key' => true],
                    ['name' => 'col2'],
                ],
            ],
            'didTableExistBefore' => false,
            'hasNewNativeTypesFeature' => true,
            'treatValuesAsNullConfiguration' => ['col2'],
            'expectedLoadOptions' => [
                'columns' => ['col1', 'col2'],
                'primaryKey' => 'col1',
                'incremental' => false,
                'treatValuesAsNull' => ['col2'],
            ],
        ];

        yield 'schema, distributionKey, tableNotExists' => [
            'sourceData' => [
                'destination' => 'in.c-bucket.destinationTable',
                'delimiter' => ',',
                'schema' => [
                    ['name' => 'col1', 'primary_key' => true],
                    ['name' => 'col2', 'distribution_key' => true],
                ],
            ],
            'didTableExistBefore' => false,
            'hasNewNativeTypesFeature' => true,
            'treatValuesAsNullConfiguration' => null,
            'expectedLoadOptions' => [
                'columns' => ['col1', 'col2'],
                'primaryKey' => 'col1',
                'incremental' => false,
                'distributionKey' => 'col2',
            ],
        ];

        yield 'schema, distributionKey, tableExists' => [
            'sourceData' => [
                'destination' => 'in.c-bucket.destinationTable',
                'delimiter' => ',',
                'schema' => [
                    ['name' => 'col1', 'primary_key' => true],
                    ['name' => 'col2', 'distribution_key' => true],
                ],
            ],
            'didTableExistBefore' => true,
            'hasNewNativeTypesFeature' => true,
            'treatValuesAsNullConfiguration' => null,
            'expectedLoadOptions' => [
                'columns' => ['col1', 'col2'],
                'primaryKey' => 'col1',
                'incremental' => false,
            ],
        ];

        yield 'deduplication strategy not null' => [
            'sourceData' => [
                'destination' => 'in.c-bucket.destinationTable',
                'deduplication_strategy' => 'insert',
            ],
            'didTableExistBefore' => false,
            'hasNewNativeTypesFeature' => false,
            'treatValuesAsNullConfiguration' => null,
            'expectedLoadOptions' => [
                'columns' => [],
                'primaryKey' => '',
                'incremental' => false,
                'deduplicationStrategy' => DeduplicationStrategy::INSERT,
            ],
        ];
    }
}
