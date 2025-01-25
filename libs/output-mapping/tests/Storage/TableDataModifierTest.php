<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Generator;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationDeleteWhere;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Storage\TableDataModifier;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsTestTables;
use Keboola\OutputMapping\Writer\Table\MappingDestination;

class TableDataModifierTest extends AbstractTestCase
{
    #[NeedsTestTables(count: 1)]
    public function testDeleteTableRows(): void
    {
        $tableDataModifier = new TableDataModifier($this->clientWrapper);

        $destination = new MappingDestination($this->firstTableId);

        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getDeleteWhereColumn')->willReturn('Id');
        $source->method('getDeleteWhereOperator')->willReturn('eq');
        $source->method('getDeleteWhereValues')->willReturn(['id1', 'id2']);

        $tableDataModifier->updateTableData($source, $destination);

        $newTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId);

        $this->assertEquals(1, $newTable['rowsCount']);
    }

    public static function provideDeleteTableRowsFromDeleteWhereConfig(): Generator
    {
        yield 'single delete where' => [
            'deleteWhere' => [
                // single tableRowsDelete job
                new MappingFromConfigurationDeleteWhere([
                    'where_filters' => [
                        [
                            'column' => 'Id',
                            'operator' => 'eq',
                            'values_from_set' => ['id1', 'id2'],
                        ],
                    ],
                ]),
            ],
            'expectedRowsCount' => 1,
        ];

        yield 'multiple delete where' => [
            'deleteWhere' => [
                // multiple tableRowsDelete jobs
                new MappingFromConfigurationDeleteWhere([
                    'where_filters' => [
                        [
                            'column' => 'Id',
                            'operator' => 'eq',
                            'values_from_set' => ['id1'],
                        ],
                    ],
                ]),
                new MappingFromConfigurationDeleteWhere([
                    'where_filters' => [
                        [
                            'column' => 'Id',
                            'operator' => 'eq',
                            'values_from_set' => ['id2'],
                        ],
                    ],
                ]),
            ],
            'expectedRowsCount' => 1,
        ];

        yield 'multiple where_filters' => [
            'deleteWhere' => [
                // single tableRowsDelete - multiple conditions
                new MappingFromConfigurationDeleteWhere([
                    'where_filters' => [
                        [
                            'column' => 'Id',
                            'operator' => 'eq',
                            'values_from_set' => ['id1'],
                        ],
                        [
                            'column' => 'Id',
                            'operator' => 'eq',
                            'values_from_set' => ['id2'],
                        ],
                    ],
                ]),
            ],
            'expectedRowsCount' => 3, // Condition Id IN('id1') AND Id IN('id2') will never be true
        ];
    }

    /**
     * @dataProvider provideDeleteTableRowsFromDeleteWhereConfig
     */
    #[NeedsTestTables(count: 1)]
    public function testDeleteTableRowsFromDeleteWhereConfig(array $deleteWhere, int $expectedRowsCount): void
    {
        $tableDataModifier = new TableDataModifier($this->clientWrapper);

        $destination = new MappingDestination($this->firstTableId);

        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getDeleteWhere')->willReturn($deleteWhere);

        $tableDataModifier->updateTableData($source, $destination);

        $newTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId);

        $this->assertEquals($expectedRowsCount, $newTable['rowsCount']);

        //@TODO: Validate also Storage job params and results
    }

    #[NeedsTestTables(count: 1)]
    public function testDeleteTableRowsWithUnexistColumn(): void
    {
        $tableDataModifier = new TableDataModifier($this->clientWrapper);

        $destination = new MappingDestination($this->firstTableId);

        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getDeleteWhereColumn')->willReturn('UnexistColumn');
        $source->method('getDeleteWhereOperator')->willReturn('eq');
        $source->method('getDeleteWhereValues')->willReturn(['id1', 'id2']);

        $expectedMessage = 'Cannot delete rows ' .
            'from table "in.c-TableDataModifierTest_testDeleteTableRowsWithUnexistColumn.test1" ' .
            'in Storage: exceptions.storage.tables.columnNotExists';

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage($expectedMessage);
        $tableDataModifier->updateTableData($source, $destination);
    }

    #[NeedsTestTables(count: 1)]
    public function testWhereColumnNotSet(): void
    {
        $tableDataModifier = new TableDataModifier($this->clientWrapper);

        $destination = new MappingDestination($this->firstTableId);

        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getDeleteWhereColumn')->willReturn(null);
        $source->method('getDeleteWhereOperator')->willReturn('eq');
        $source->method('getDeleteWhereValues')->willReturn(['id1', 'id2']);

        $tableDataModifier->updateTableData($source, $destination);

        $newTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId);

        $this->assertEquals(3, $newTable['rowsCount']);
    }

    public function testaDeleteTableRowsFromDeleteWhereConfigWithWorkspace(): void
    {
        $this->markTestIncomplete('Not implemented yet on Storage API side.');
    }
}
