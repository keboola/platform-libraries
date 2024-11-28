<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Mapping;

use Generator;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalData;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Keboola\OutputMapping\Writer\FileItem;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\OutputMapping\Writer\Table\Source\WorkspaceItemSource;
use PHPUnit\Framework\TestCase;

class MappingFromProcessedConfigurationTest extends TestCase
{
    public function testBasic(): void
    {
        $mapping = [
            'destination' => 'in.c-main.table',
            'delimiter' => ',',
            'enclosure' => '"',
        ];

        $sourceMock = $this->createMock(MappingFromRawConfigurationAndPhysicalData::class);
        $sourceMock->method('isSliced')->willReturn(false);
        $sourceMock->method('getPathName')->willReturn('sourcePathName');
        $sourceMock->method('getPath')->willReturn('sourcePath');
        $sourceMock->method('getManifestName')->willReturn('sourceManifestName');
        $sourceMock->method('getConfiguration')->willReturn(null);
        $sourceMock->method('getWorkspaceId')->willReturn('workspaceId');
        $sourceMock->method('getDataObject')->willReturn('dataObject');
        $sourceMock->method('getSourceName')->willReturn('sourceName');
        $sourceMock->method('getItemSourceClass')->willReturn(WorkspaceItemSource::class);

        $fileItemMock = $this->createMock(FileItem::class);

        $physicalDataWithManifest = new MappingFromRawConfigurationAndPhysicalDataWithManifest(
            $sourceMock,
            $fileItemMock,
        );
        $mapping = new MappingFromProcessedConfiguration($mapping, $physicalDataWithManifest);

        self::assertEquals('sourceName', $mapping->getSourceName());
        self::assertEquals('workspaceId', $mapping->getWorkspaceId());
        self::assertEquals('dataObject', $mapping->getDataObject());
        self::assertEquals('sourcePathName', $mapping->getPathName());
        self::assertEquals('eq', $mapping->getDeleteWhereOperator());
        self::assertEquals(',', $mapping->getDelimiter());
        self::assertEquals('"', $mapping->getEnclosure());
        self::assertEquals([], $mapping->getDeleteWhereValues());
        self::assertEquals([], $mapping->getColumnMetadata());
        self::assertEquals([], $mapping->getColumns());
        self::assertEquals([], $mapping->getDistributionKey());
        self::assertEquals([], $mapping->getMetadata());
        self::assertEquals([], $mapping->getTableMetadata());
        self::assertEquals([], $mapping->getPrimaryKey());
        self::assertEquals([], $mapping->getTags());
        self::assertNull($mapping->getDeleteWhereColumn());
        self::assertFalse($mapping->isSliced());
        self::assertFalse($mapping->hasColumnMetadata());
        self::assertFalse($mapping->hasColumns());
        self::assertFalse($mapping->hasDistributionKey());
        self::assertFalse($mapping->hasMetadata());
        self::assertFalse($mapping->hasTableMetadata());
        self::assertFalse($mapping->hasSchemaColumnMetadata());
        self::assertFalse($mapping->hasWriteAlways());
        self::assertFalse($mapping->isIncremental());
        self::assertEquals(WorkspaceItemSource::class, $mapping->getItemSourceClass());
        self::assertInstanceOf(MappingDestination::class, $mapping->getDestination());
    }

    public function testTableMetadata(): void
    {
        $mapping = [
            'destination' => 'in.c-main.table',
            'delimiter' => ',',
            'enclosure' => '"',
            'description' => 'table desc',
            'table_metadata' => [
                'key1' => 'val1',
                'key2' => 'val2',
            ],
        ];

        $physicalDataWithManifest = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $mapping = new MappingFromProcessedConfiguration($mapping, $physicalDataWithManifest);

        self::assertEquals([
            'key1' => 'val1',
            'key2' => 'val2',
            'KBC.description' => 'table desc',
        ], $mapping->getTableMetadata());
        self::assertTrue($mapping->hasTableMetadata());
    }

    public function testHasSchemaConfiguration(): void
    {
        $mapping = [
            'destination' => 'in.c-main.table',
            'delimiter' => ',',
            'enclosure' => '"',
            'schema' => [
                [
                    'name' => 'col1',
                ],
                [
                    'name' => 'col2',
                    'metadata' => [
                        'key1' => 'val1',
                    ],
                ],
            ],
        ];

        $physicalDataWithManifest = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $mapping = new MappingFromProcessedConfiguration($mapping, $physicalDataWithManifest);

        self::assertNotNull($mapping->getSchema());
        self::assertCount(2, $mapping->getSchema());

        $schema = $mapping->getSchema();
        self::assertEquals('col1', $schema[0]->getName());
        self::assertEquals('col2', $schema[1]->getName());

        self::assertTrue($mapping->hasSchemaColumnMetadata());
    }

    /**
     * @dataProvider deleteWhereParamsDataProvider
     */
    public function testDeleteWhereParams(
        array $mapping,
        ?string $expectedColumn,
        string $expectedOperator,
        array $expectedValues,
    ): void {
        $physicalDataWithManifest = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);

        $mapping = new MappingFromProcessedConfiguration($mapping, $physicalDataWithManifest);

        self::assertEquals($expectedColumn, $mapping->getDeleteWhereColumn());
        self::assertEquals($expectedOperator, $mapping->getDeleteWhereOperator());
        self::assertEquals($expectedValues, $mapping->getDeleteWhereValues());
    }

    public function testRemoveRestrictedColumnAndMetadata(): void
    {
        $mapping = [
            'destination' => 'in.c-main.table',
            'delimiter' => ',',
            'enclosure' => '"',
            'columns' => [
                'col1',
                '_timestamp',
            ],
            'column_metadata' => [
                'col1' => [
                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'INT',
                    ],
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'INTEGER',
                    ],
                ],
                '_timestamp' => [
                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'INT',
                    ],
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'TIMESTAMP',
                    ],
                ],
            ],
        ];

        $physicalDataWithManifest = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);

        $mapping = new MappingFromProcessedConfiguration($mapping, $physicalDataWithManifest);

        self::assertSame(['col1'], $mapping->getColumns());

        $metadataList = $mapping->getColumnMetadata();
        self::assertCount(1, $metadataList);

        self::assertSame('col1', $metadataList[0]->getColumnName());
        self::assertSame(
            [
                [
                    'key' => 'KBC.datatype.type',
                    'value' => 'INT',
                ],
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'INTEGER',
                ],
            ],
            $metadataList[0]->getMetadata(),
        );
    }

    public function deleteWhereParamsDataProvider(): Generator
    {
        yield 'basic' => [
            [
                'destination' => 'in.c-main.table',
                'delete_where_column' => 'col1',
                'delete_where_operator' => 'eq',
                'delete_where_values' => ['val1', 'val2'],
            ],
            'col1',
            'eq',
            ['val1', 'val2'],
        ];

        yield 'column-empty' => [
            [
                'destination' => 'in.c-main.table',
                'delete_where_column' => '',
                'delete_where_operator' => 'eq',
                'delete_where_values' => ['val1', 'val2'],
            ],
            null,
            'eq',
            ['val1', 'val2'],
        ];

        yield 'column-null' => [
            [
                'destination' => 'in.c-main.table',
                'delete_where_column' => null,
                'delete_where_operator' => 'eq',
                'delete_where_values' => ['val1', 'val2'],
            ],
            null,
            'eq',
            ['val1', 'val2'],
        ];

        yield 'column-not-set' => [
            [
                'destination' => 'in.c-main.table',
                'delete_where_operator' => 'eq',
                'delete_where_values' => ['val1', 'val2'],
            ],
            null,
            'eq',
            ['val1', 'val2'],
        ];
    }

    public function testPrimaryKeyAndColumsAreConvertedToStrings(): void
    {
        $mapping = [
            'destination' => 'in.c-main.table',
            'columns' => [
                null,
                123,
                'col1',
                'col2',
            ],
            'primary_key' => [
                null,
                123,
                'col1',
            ],
        ];
        $physicalDataWithManifest = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);

        $mapping = new MappingFromProcessedConfiguration($mapping, $physicalDataWithManifest);

        self::assertSame(['', '123', 'col1', 'col2'], $mapping->getColumns());
        self::assertSame(['', '123', 'col1'], $mapping->getPrimaryKey());
    }
}
