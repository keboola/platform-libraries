<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Generator;
use Keboola\Csv\CsvFile;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingColumnMetadata;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Storage\BucketInfo;
use Keboola\OutputMapping\Storage\TableInfo;
use Keboola\OutputMapping\Storage\TableStructureModifier;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsTestTables;
use Keboola\OutputMapping\Writer\Table\MappingDestination;

class TableStructureModifierTest extends AbstractTestCase
{
    private TableStructureModifier $modifier;

    private BucketInfo $destinationBucket;

    private array $destinationTableInfo;

    private MappingDestination $destination;

    public function setUp(): void
    {
        parent::setUp();
        $this->modifier = new TableStructureModifier($this->clientWrapper, $this->testLogger);
        $bucket = $this->clientWrapper->getTableAndFileStorageClient()->getBucket($this->testBucketId);
        $this->destinationBucket = new BucketInfo($bucket);
        $this->destinationTableInfo = $this
            ->clientWrapper
            ->getTableAndFileStorageClient()
            ->getTable($this->firstTableId);
        $this->destination = new MappingDestination($this->firstTableId);
    }

    #[NeedsTestTables]
    public function testUpdateTableStructureNoChanges(): void
    {
        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getPrimaryKey')->willReturn($this->destinationTableInfo['primaryKey']);
        $source->method('getColumns')->willReturn($this->destinationTableInfo['columns']);
        $source->method('getMetadata')->willReturn([]);
        $source->method('getColumnMetadata')->willReturn([]);

        $this->modifier->updateTableStructure(
            $this->destinationBucket,
            new TableInfo($this->destinationTableInfo),
            $source,
            $this->destination,
            false,
        );

        $newTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId);

        $this->assertEquals(
            $this->dropTimestampAttributes($this->destinationTableInfo),
            $this->dropTimestampAttributes($newTable),
        );
    }

    /**
     * @dataProvider newColumnNamesProvider
     */
    #[NeedsTestTables]
    public function testUpdateTableStructureAddColumns(string $newColumnName): void
    {
        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getPrimaryKey')->willReturn($this->destinationTableInfo['primaryKey']);
        $source->method('getColumns')->willReturn(array_merge(
            $this->destinationTableInfo['columns'],
            [$newColumnName],
        ));
        $source->method('getMetadata')->willReturn([]);
        $source->method('getColumnMetadata')->willReturn([]);

        $this->modifier->updateTableStructure(
            $this->destinationBucket,
            new TableInfo($this->destinationTableInfo),
            $source,
            $this->destination,
            false,
        );

        $newTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId);
        $expectedTable = array_merge_recursive($this->destinationTableInfo, [
            'columns' => [$newColumnName],
        ]);

        $this->assertEquals($this->dropTimestampAttributes($expectedTable), $this->dropTimestampAttributes($newTable));
    }

    #[NeedsTestTables]
    public function testUpdateTableStructureAddPK(): void
    {
        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getPrimaryKey')->willReturn(array_merge(
            $this->destinationTableInfo['primaryKey'],
            ['Id', 'Name'],
        ));
        $source->method('getColumns')->willReturn($this->destinationTableInfo['columns']);
        $source->method('getMetadata')->willReturn([]);
        $source->method('getColumnMetadata')->willReturn([]);

        $this->modifier->updateTableStructure(
            $this->destinationBucket,
            new TableInfo($this->destinationTableInfo),
            $source,
            $this->destination,
            false,
        );

        $newTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId);
        $expectedTable = array_merge_recursive($this->destinationTableInfo, [
            'primaryKey' => ['Id', 'Name'],
        ]);

        $this->assertEquals($this->dropTimestampAttributes($expectedTable), $this->dropTimestampAttributes($newTable));
    }

    #[NeedsTestTables]
    public function testErrorUpdateTableStructureBadPK(): void
    {
        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getPrimaryKey')->willReturn(array_merge(
            $this->destinationTableInfo['primaryKey'],
            ['invalidPK'],
        ));
        $source->method('getColumns')->willReturn($this->destinationTableInfo['columns']);
        $source->method('getMetadata')->willReturn([]);
        $source->method('getColumnMetadata')->willReturn([]);

        $this->modifier->updateTableStructure(
            $this->destinationBucket,
            new TableInfo($this->destinationTableInfo),
            $source,
            $this->destination,
            false,
        );

        $expectedErrorMessage = 'Error changing primary key of table in.c-testErrorUpdateTableStructureBadPKTest.test1';
        $expectedErrorMessage .= ': Primary key columns "invalidPK" not found in "Id, Name, foo, bar"';
        $this->assertTrue($this->testHandler->hasWarning($expectedErrorMessage));
    }

    #[NeedsTestTables]
    public function testErrorUpdateTableStructureAddPKOnWrongData(): void
    {
        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload.csv');
        $csv->writeRow(['Id', 'Name', 'foo', 'bar']);
        $csv->writeRow(['id5', 'name5', 'foo5', 'bar5']);
        $csv->writeRow(['id5', 'name5', 'foo5', 'bar5']);

        $this->clientWrapper->getTableAndFileStorageClient()->writeTableAsync(
            $this->firstTableId,
            $csv,
        );

        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getPrimaryKey')->willReturn(array_merge(
            $this->destinationTableInfo['primaryKey'],
            ['Id'],
        ));
        $source->method('getColumns')->willReturn($this->destinationTableInfo['columns']);
        $source->method('getMetadata')->willReturn([]);
        $source->method('getColumnMetadata')->willReturn([]);

        $this->modifier->updateTableStructure(
            $this->destinationBucket,
            new TableInfo($this->destinationTableInfo),
            $source,
            $this->destination,
            false,
        );

        $this->assertTrue($this->testHandler->hasWarningThatContains(
            'The new primary key cannot be created; duplicate values in primary key columns exist.',
        ));
    }

    #[NeedsTestTables]
    public function testErrorUpdateTableStructureEmptyColumnName(): void
    {
        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getPrimaryKey')->willReturn($this->destinationTableInfo['primaryKey']);
        $source->method('getColumns')->willReturn(array_merge(
            $this->destinationTableInfo['columns'],
            [''],
        ));
        $source->method('getMetadata')->willReturn([]);
        $source->method('getColumnMetadata')->willReturn([]);

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Invalid parameters - name: This value should not be blank.');
        $this->modifier->updateTableStructure(
            $this->destinationBucket,
            new TableInfo($this->destinationTableInfo),
            $source,
            $this->destination,
            false,
        );
    }

    /**
     * @dataProvider newColumnNamesProvider
     */
    #[NeedsTestTables(typedTable: true)]
    public function testUpdateTableStructureAddColumnsFromMetadata(string $newColumnName): void
    {
        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getPrimaryKey')->willReturn($this->destinationTableInfo['primaryKey']);
        $source->method('getColumns')->willReturn([]);
        $source->method('getMetadata')->willReturn([]);
        $source->method('getColumnMetadata')->willReturn([
            new MappingColumnMetadata(
                $newColumnName,
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
            ),
        ]);

        $this->modifier->updateTableStructure(
            $this->destinationBucket,
            new TableInfo($this->destinationTableInfo),
            $source,
            $this->destination,
            false,
        );

        $newTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId);

        $expectedNewColumnDefinition = [
            'name' => $newColumnName,
            'definition' => [
                'type' => 'NUMBER',
                'nullable' => true,
                'length' => '38,0',
            ],
            'basetype' => 'INTEGER',
            'canBeFiltered' => true,
        ];
        $expectedTable = array_merge_recursive($this->destinationTableInfo, [
            'columns' => [$newColumnName],
            'definition' => [
                'columns' => [
                    $expectedNewColumnDefinition,
                ],
            ],
        ]);

        // must be validated separately because the definition columns in $newTable do not guarantee the order.
        $newColumnDefinition = null;
        foreach ($newTable['definition']['columns'] as $column) {
            if ($column['name'] === $newColumnName) {
                $newColumnDefinition = $column;
                break;
            }
        }

        self::assertSame($expectedNewColumnDefinition, $newColumnDefinition);

        $this->assertEquals(
            $this->dropMetadataAndDefinitionAttributes($this->dropTimestampAttributes($expectedTable)),
            $this->dropMetadataAndDefinitionAttributes($this->dropTimestampAttributes($newTable)),
        );
    }

    public function updateTableStructureAddColumnsFromMetadataWithNativeTypesDataProvider(): Generator
    {
        yield 'native types' => [
            'enforceBaseTypes' => false,
            'expectedLength' => '10,5',
        ];

        yield 'enforced base types' => [
            'enforceBaseTypes' => true,
            'expectedLength' => '38,9',
        ];
    }

    /**
     * @dataProvider updateTableStructureAddColumnsFromMetadataWithNativeTypesDataProvider
     */
    #[NeedsTestTables(typedTable: true)]
    public function testUpdateTableStructureAddColumnsFromMetadataWithNativeTypes(
        bool $enforceBaseTypes,
        string $expectedLength,
    ): void {
        $newColumnName = 'new_column';

        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getPrimaryKey')->willReturn($this->destinationTableInfo['primaryKey']);
        $source->method('getColumns')->willReturn([]);
        $source->method('getMetadata')->willReturn([
            [
                'key' => 'KBC.datatype.backend',
                'value' => 'snowflake',
            ],
        ]);
        $source->method('getColumnMetadata')->willReturn([
            new MappingColumnMetadata($newColumnName, [
                [
                    'key' => 'KBC.datatype.type',
                    'value' => 'NUMBER',
                ],
                [
                    'key' => 'KBC.datatype.length',
                    'value' => '10,5',
                ],
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'NUMERIC',
                ],
            ]),
        ]);

        $this->modifier->updateTableStructure(
            $this->destinationBucket,
            new TableInfo($this->destinationTableInfo),
            $source,
            $this->destination,
            $enforceBaseTypes,
        );

        $newTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId);

        $expectedNewColumnDefinition = [
            'name' => $newColumnName,
            'definition' => [
                'type' => 'NUMBER',
                'nullable' => true,
                'length' => $expectedLength,
            ],
            'basetype' => 'NUMERIC',
            'canBeFiltered' => true,
        ];
        $expectedTable = array_merge_recursive($this->destinationTableInfo, [
            'columns' => [$newColumnName],
            'definition' => [
                'columns' => [
                    $expectedNewColumnDefinition,
                ],
            ],
        ]);

        // must be validated separately because the definition columns in $newTable do not guarantee the order.
        $newColumnDefinition = null;
        foreach ($newTable['definition']['columns'] as $column) {
            if ($column['name'] === $newColumnName) {
                $newColumnDefinition = $column;
                break;
            }
        }

        self::assertSame($expectedNewColumnDefinition, $newColumnDefinition);

        $this->assertEquals(
            $this->dropMetadataAndDefinitionAttributes($this->dropTimestampAttributes($expectedTable)),
            $this->dropMetadataAndDefinitionAttributes($this->dropTimestampAttributes($newTable)),
        );
    }

    #[NeedsTestTables(typedTable: true)]
    public function testUpdateTableStructureAddColumnsFailureThrowsInvalidOutputException(): void
    {
        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getColumns')->willReturn(array_merge(
            $this->destinationTableInfo['columns'],
            ['newColumn'],
        ));
        $source->method('getMetadata')->willReturn([
            [
                'key' => 'KBC.datatype.backend',
                'value' => 'snowflake',
            ],
        ]);
        $source->method('getColumnMetadata')->willReturn([
            new MappingColumnMetadata(
                'newColumn',
                [
                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'VARCHAR',
                    ],
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'STRING',
                    ],
                    [
                        'key' => 'KBC.datatype.nullable',
                        'value' => false,
                    ],
                ],
            ),
        ]);

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage(
            'Non-nullable column "newColumn" cannot be added to non-empty table '
            . '"test1" unless it has a non-null default value.',
        );

        $this->modifier->updateTableStructure(
            $this->destinationBucket,
            new TableInfo($this->destinationTableInfo),
            $source,
            $this->destination,
            false,
        );
    }

    public function newColumnNamesProvider(): Generator
    {
        yield ['new_column'];
        yield ['123'];
    }

    private function dropMetadataAndDefinitionAttributes(array $table): array
    {
        unset($table['definition']['columns']);
        unset($table['columnMetadata']);
        unset($table['metadata']);
        return $table;
    }

    private function dropTimestampAttributes(array $table): array
    {
        unset($table['lastChangeDate']);
        unset($table['bucket']['lastChangeDate']);
        return $table;
    }
}
