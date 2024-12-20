<?php

declare(strict_types=1);

namespace Keboola\OutputMapping;

use Generator;
use Keboola\OutputMapping\Mapping\MappingColumnMetadata;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Keboola\OutputMapping\Mapping\MappingStorageSources;
use Keboola\OutputMapping\Storage\BucketInfo;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyOutputBucket;
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
        $loadTask = $loadTableTaskCreator->create(
            strategy: $strategy,
            source: $source,
            storageSources: $storageSources,
            settings: $settings,
        );

        self::assertTrue($loadTask->isUsingFreshlyCreatedTable());
        self::assertSame(
            'out.c-testNativeTypeLoadTaskTableNotExistsEmpty.destinationTable',
            $loadTask->getDestinationTableName(),
        );
        $storageTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId.'.destinationTable',
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
            $storageTable['definition']['columns'],
        );
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
    }
}
