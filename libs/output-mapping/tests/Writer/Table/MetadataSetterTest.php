<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table;

use Generator;
use Keboola\OutputMapping\DeferredTasks\Metadata\ColumnsMetadata;
use Keboola\OutputMapping\DeferredTasks\Metadata\SchemaColumnMetadata;
use Keboola\OutputMapping\DeferredTasks\Metadata\TableMetadata;
use Keboola\OutputMapping\DeferredTasks\TableWriter\LoadTableTask;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Mapping\MappingStorageSources;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\OutputMapping\Writer\Table\MetadataSetter;
use PHPUnit\Framework\TestCase;

class MetadataSetterTest extends TestCase
{
    /**
     * @dataProvider provideSetTableMetadata
     */
    public function testSetTableMetadata(
        MappingFromProcessedConfiguration $mockMappingFromProcessedConfiguration,
        bool $didTableExistBefore,
        int $expectedCountMetadata,
        array $expectedMetadata = [],
    ): void {
        $mappingDestination = new MappingDestination('in.c-main.table');

        $mockMappingStorageSources = $this->createMock(MappingStorageSources::class);
        $mockMappingStorageSources->method('didTableExistBefore')->willReturn($didTableExistBefore);

        $metadataSetter = new MetadataSetter();
        $loadTask = $metadataSetter->setTableMetadata(
            loadTask: new LoadTableTask($mappingDestination, [], false),
            processedSource: $mockMappingFromProcessedConfiguration,
            storageSources: $mockMappingStorageSources,
            systemMetadata: new SystemMetadata(['componentId' => 'keboola.test']),
        );

        self::assertCount($expectedCountMetadata, $loadTask->getMetadata());
        self::assertEquals($expectedMetadata, $loadTask->getMetadata());
    }

    public function provideSetTableMetadata(): Generator
    {
        $mappingDestination = new MappingDestination('in.c-main.table');
        $tableLastUpdateMetadata = new TableMetadata(
            'in.c-main.table',
            'system',
            [
                [
                    'key' => 'KBC.lastUpdatedBy.component.id',
                    'value' => 'keboola.test',
                ],
            ],
        );
        $tableCreatedByMetadata = new TableMetadata(
            'in.c-main.table',
            'system',
            [
                [
                    'key' => 'KBC.createdBy.component.id',
                    'value' => 'keboola.test',
                ],
            ],
        );
        $tableMetadata = new TableMetadata(
            'in.c-main.table',
            'keboola.test',
            [
                [
                    'key' => 'key1',
                    'value' => 'val1',
                ],
                [
                    'key' => 'key2',
                    'value' => 'val2',
                ],
            ],
        );
        $columnsMetadata = new ColumnsMetadata(
            'in.c-main.table',
            'keboola.test',
            [
                [
                    'key' => 'col-key1',
                    'value' => 'col-val1',
                ],
                [
                    'key' => 'col-key2',
                    'value' => 'col-val2',
                ],
            ],
        );
        $schemaColumnMetadata = new SchemaColumnMetadata(
            'in.c-main.table',
            'keboola.test',
            [
                new MappingFromConfigurationSchemaColumn([
                    'mapping' => [
                        'description' => 'column desc',
                        'metadata' => [
                            'key1' => 'val1',
                            'key2' => 'val2',
                        ],
                    ],
                ]),
            ],
        );

        $mockMappingFromProcessedConfiguration = $this->createMock(MappingFromProcessedConfiguration::class);
        $mockMappingFromProcessedConfiguration->method('getDestination')->willReturn($mappingDestination);
        $mockMappingFromProcessedConfiguration->method('hasMetadata')->willReturn(false);
        $mockMappingFromProcessedConfiguration->method('hasColumnMetadata')->willReturn(false);
        $mockMappingFromProcessedConfiguration->method('hasSchemaColumnMetadata')->willReturn(false);

        yield 'set-minimal' => [
            $mockMappingFromProcessedConfiguration,
            true,
            1,
            [$tableLastUpdateMetadata],
        ];

        $mockMappingFromProcessedConfiguration = $this->createMock(MappingFromProcessedConfiguration::class);
        $mockMappingFromProcessedConfiguration->method('getDestination')->willReturn($mappingDestination);
        $mockMappingFromProcessedConfiguration->method('hasMetadata')->willReturn(true);
        $mockMappingFromProcessedConfiguration->method('getMetadata')->willReturn([
            [
                'key' => 'key1',
                'value' => 'val1',
            ],
            [
                'key' => 'key2',
                'value' => 'val2',
            ],
        ]);
        $mockMappingFromProcessedConfiguration->method('hasColumnMetadata')->willReturn(false);
        $mockMappingFromProcessedConfiguration->method('hasSchemaColumnMetadata')->willReturn(false);

        yield 'set-metadata' => [
            $mockMappingFromProcessedConfiguration,
            true,
            2,
            [$tableLastUpdateMetadata, $tableMetadata],
        ];

        $mockMappingFromProcessedConfiguration = $this->createMock(MappingFromProcessedConfiguration::class);
        $mockMappingFromProcessedConfiguration->method('getDestination')->willReturn($mappingDestination);
        $mockMappingFromProcessedConfiguration->method('hasTableMetadata')->willReturn(true);
        $mockMappingFromProcessedConfiguration->method('getTableMetadata')->willReturn([
            'key1' => 'val1', // new format with key-value pairs
            'key2' => 'val2',
        ]);
        $mockMappingFromProcessedConfiguration->method('hasColumnMetadata')->willReturn(false);
        $mockMappingFromProcessedConfiguration->method('hasSchemaColumnMetadata')->willReturn(false);

        yield 'set-table-metadata' => [
            $mockMappingFromProcessedConfiguration,
            true,
            2,
            [$tableLastUpdateMetadata, $tableMetadata],
        ];

        $mockMappingFromProcessedConfiguration = $this->createMock(MappingFromProcessedConfiguration::class);
        $mockMappingFromProcessedConfiguration->method('getDestination')->willReturn($mappingDestination);
        $mockMappingFromProcessedConfiguration->method('hasMetadata')->willReturn(false);
        $mockMappingFromProcessedConfiguration->method('hasColumnMetadata')->willReturn(true);
        $mockMappingFromProcessedConfiguration->method('getColumnMetadata')->willReturn([
            [
                'key' => 'col-key1',
                'value' => 'col-val1',
            ],
            [
                'key' => 'col-key2',
                'value' => 'col-val2',
            ],
        ]);
        $mockMappingFromProcessedConfiguration->method('hasSchemaColumnMetadata')->willReturn(false);

        yield 'set-column-metadata' => [
            $mockMappingFromProcessedConfiguration,
            true,
            2,
            [$tableLastUpdateMetadata, $columnsMetadata],
        ];

        $mockMappingFromProcessedConfiguration = $this->createMock(MappingFromProcessedConfiguration::class);
        $mockMappingFromProcessedConfiguration->method('getDestination')->willReturn($mappingDestination);
        $mockMappingFromProcessedConfiguration->method('hasMetadata')->willReturn(false);
        $mockMappingFromProcessedConfiguration->method('hasColumnMetadata')->willReturn(false);
        $mockMappingFromProcessedConfiguration->method('hasSchemaColumnMetadata')->willReturn(true);
        $mockMappingFromProcessedConfiguration->method('getSchema')->willReturn([
            new MappingFromConfigurationSchemaColumn([
                'mapping' => [
                    'description' => 'column desc',
                    'metadata' => [
                        'key1' => 'val1',
                        'key2' => 'val2',
                    ],
                ],
            ]),
        ]);

        yield 'set-schema-column-metadata' => [
            $mockMappingFromProcessedConfiguration,
            true,
            2,
            [$tableLastUpdateMetadata, $schemaColumnMetadata],
        ];

        $mockMappingFromProcessedConfiguration = $this->createMock(MappingFromProcessedConfiguration::class);
        $mockMappingFromProcessedConfiguration->method('getDestination')->willReturn($mappingDestination);
        $mockMappingFromProcessedConfiguration->method('hasMetadata')->willReturn(true);
        $mockMappingFromProcessedConfiguration->method('getMetadata')->willReturn([
            [
                'key' => 'key1',
                'value' => 'val1',
            ],
            [
                'key' => 'key2',
                'value' => 'val2',
            ],
        ]);
        $mockMappingFromProcessedConfiguration->method('hasColumnMetadata')->willReturn(true);
        $mockMappingFromProcessedConfiguration->method('getColumnMetadata')->willReturn([
            [
                'key' => 'col-key1',
                'value' => 'col-val1',
            ],
            [
                'key' => 'col-key2',
                'value' => 'col-val2',
            ],
        ]);
        $mockMappingFromProcessedConfiguration->method('hasSchemaColumnMetadata')->willReturn(true);
        $mockMappingFromProcessedConfiguration->method('getSchema')->willReturn([
            new MappingFromConfigurationSchemaColumn([
                'mapping' => [
                    'description' => 'column desc',
                    'metadata' => [
                        'key1' => 'val1',
                        'key2' => 'val2',
                    ],
                ],
            ]),
        ]);

        yield 'set-all' => [
            $mockMappingFromProcessedConfiguration,
            false,
            5,
            [
                $tableCreatedByMetadata,
                $tableLastUpdateMetadata,
                $tableMetadata,
                $columnsMetadata,
                $schemaColumnMetadata,
            ],
        ];
    }
}
