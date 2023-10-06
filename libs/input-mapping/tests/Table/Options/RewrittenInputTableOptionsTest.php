<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Options;

use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptions;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptionsList;
use PHPUnit\Framework\TestCase;

class RewrittenInputTableOptionsTest extends TestCase
{
    public function testGetters(): void
    {
        $definition = new RewrittenInputTableOptions(
            [
                'source' => 'test',
                'source_branch_id' => 123,
            ],
            'source',
            24,
            ['a' => 'b'],
        );
        self::assertSame('source', $definition->getSource());
        self::assertSame(24, $definition->getSourceBranchId());
        self::assertSame(['a' => 'b'], $definition->getTableInfo());
    }


    public function testGetExportOptionsSimpleColumns(): void
    {
        $definition = new RewrittenInputTableOptions(
            [
                'source' => 'test',
                'destination' => 'dest',
                'columns' => ['col1', 'col2'],
                'changed_since' => '-1 days',
                'where_column' => 'col1',
                'where_operator' => 'ne',
                'where_values' => ['1', '2'],
                'limit' => 100,
            ],
            'source',
            12345,
            ['a' => 'b'],
        );
        self::assertEquals([
            'columns' => ['col1', 'col2'],
            'changedSince' => '-1 days',
            'whereColumn' => 'col1',
            'whereValues' => ['1', '2'],
            'whereOperator' => 'ne',
            'limit' => 100,
            'overwrite' => false,
            'sourceBranchId' => 12345,
        ], $definition->getStorageApiExportOptions(new InputTableStateList([])));
    }

    public function testGetExportOptionsExtendColumns(): void
    {
        $definition = new RewrittenInputTableOptions(
            [
                'source' => 'test',
                'destination' => 'dest',
                'column_types' => [
                    [
                        'source' => 'col1',
                        'type' => 'VARCHAR',
                        'length' => '200',
                        'destination' => 'colone',
                        'nullable' => false,
                        'convert_empty_values_to_null' => true,
                    ],
                    [
                        'source' => 'col2',
                        'type' => 'VARCHAR',
                        'nullable' => true,
                        'convert_empty_values_to_null' => false,
                    ],
                ],
                'changed_since' => '-1 days',
                'where_column' => 'col1',
                'where_operator' => 'ne',
                'where_values' => ['1', '2'],
                'limit' => 100,
            ],
            'source',
            12345,
            ['a' => 'b'],
        );
        self::assertEquals([
            'columns' => ['col1', 'col2'],
            'changedSince' => '-1 days',
            'whereColumn' => 'col1',
            'whereValues' => ['1', '2'],
            'whereOperator' => 'ne',
            'limit' => 100,
            'overwrite' => false,
            'sourceBranchId' => 12345,
        ], $definition->getStorageApiExportOptions(new InputTableStateList([])));
    }

    public function testGetExportOptionsSourceBranchId(): void
    {
        $definition = new RewrittenInputTableOptions(
            [
                'source' => 'test',
                'destination' => 'dest',
                'columns' => ['col1', 'col2'],
                'limit' => 100,
            ],
            'source',
            12345,
            ['a' => 'b'],
        );

        self::assertEquals(
            [
                'columns' => ['col1', 'col2'],
                'limit' => 100,
                'overwrite' => false,
                'sourceBranchId' => 12345,
            ],
            $definition->getStorageApiExportOptions(new InputTableStateList([])),
        );
    }

    public function testGetExportOptionsEmptyValue(): void
    {
        $definition = new RewrittenInputTableOptions(
            ['source' => 'test'],
            'source',
            12345,
            ['a' => 'b'],
        );
        self::assertEquals(
            ['overwrite' => false, 'sourceBranchId' => 12345],
            $definition->getStorageApiExportOptions(new InputTableStateList([])),
        );
    }

    public function testGetExportOptionsDays(): void
    {
        $definition = new RewrittenInputTableOptions(
            [
                'source' => 'test',
                'days' => 2,
            ],
            'source',
            12345,
            ['a' => 'b'],
        );
        self::assertEquals(
            [
                'changedSince' => '-2 days',
                'overwrite' => false,
                'sourceBranchId' => 12345,
            ],
            $definition->getStorageApiExportOptions(new InputTableStateList([])),
        );
    }

    public function testGetExportOptionsAdaptiveInputMapping(): void
    {
        $definition = new RewrittenInputTableOptions(
            [
                'source' => 'test',
                'changed_since' => InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE,
            ],
            'test',
            12345,
            ['a' => 'b'],
        );
        $tablesState = new InputTableStateList([[
            'source' => 'test',
            'lastImportDate' => '1989-11-17T21:00:00+0200',
        ]]);
        self::assertEquals(
            [
                'changedSince' => '1989-11-17T21:00:00+0200',
                'overwrite' => false,
                'sourceBranchId' => 12345,
            ],
            $definition->getStorageApiExportOptions($tablesState),
        );
    }

    public function testGetExportOptionsAdaptiveInputMappingMissingTable(): void
    {
        $definition = new RewrittenInputTableOptions(
            [
                'source' => 'test',
                'changed_since' => InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE,
            ],
            'source',
            12345,
            ['a' => 'b'],
        );
        $tablesState = new InputTableStateList([]);
        self::assertEquals(
            [
                'overwrite' => false,
                'sourceBranchId' => 12345,
            ],
            $definition->getStorageApiExportOptions($tablesState),
        );
    }
}
