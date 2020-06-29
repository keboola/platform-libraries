<?php

namespace Keboola\InputMapping\Tests\Reader\Options;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader\Options\InputTableOptions;
use Keboola\InputMapping\Reader\State\InputTableStateList;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class InputTableOptionsTest extends \PHPUnit_Framework_TestCase
{

    public function testGetSource()
    {
        $definition = new InputTableOptions(['source' => 'test']);
        self::assertEquals('test', $definition->getSource());
    }

    public function testGetDestination()
    {
        $definition = new InputTableOptions(['source' => 'test', 'destination' => 'dest']);
        self::assertEquals('dest', $definition->getDestination());
    }

    public function testGetDefinition()
    {
        $definition = new InputTableOptions(['source' => 'test', 'destination' => 'dest']);
        self::assertEquals([
            'source' => 'test',
            'destination' => 'dest',
            'columns' => [],
            'where_values' => [],
            'where_operator' => 'eq',
        ], $definition->getDefinition());
    }

    public function testGetColumns()
    {
        $definition = new InputTableOptions(['source' => 'test', 'columns' => ['col1', 'col2']]);
        self::assertEquals(['col1', 'col2'], $definition->getColumnNames());
    }

    public function testGetColumnsExtended()
    {
        $definition = new InputTableOptions(['source' => 'test', 'columns' => [['source' => 'col1'], ['source' => 'col2']]]);
        self::assertEquals(['col1', 'col2'], $definition->getColumnNames());
    }

    public function testConstructorMissingSource()
    {
        self::expectException(InvalidConfigurationException::class);
        self::expectExceptionMessage('Either "source" or "source_search" must be configured.');
        new InputTableOptions([]);
    }

    public function testConstructorDaysAndChangedSince()
    {
        self::expectException(InvalidInputException::class);
        self::expectExceptionMessage('Cannot set both parameters "days" and "changed_since".');
        new InputTableOptions(['source' => 'test', 'days' => 1, 'changed_since' => '-2 days']);
    }

    public function testGetExportOptionsEmptyValue()
    {
        $definition = new InputTableOptions(['source' => 'test']);
        self::assertEquals([], $definition->getStorageApiExportOptions(new InputTableStateList([])));
    }

    public function testGetExportOptionsSimpleColumns()
    {
        $definition = new InputTableOptions([
            'source' => 'test',
            'destination' => 'dest',
            'columns' => ['col1', 'col2'],
            'changed_since' => '-1 days',
            'where_column' => 'col1',
            'where_operator' => 'ne',
            'where_values' => ['1', '2'],
            'limit' => 100,
        ]);
        self::assertEquals([
            'columns' => ['col1', 'col2'],
            'changedSince' => '-1 days',
            'whereColumn' => 'col1',
            'whereValues' => ['1', '2'],
            'whereOperator' => 'ne',
            'limit' => 100,
        ], $definition->getStorageApiExportOptions(new InputTableStateList([])));
    }

    public function testGetExportOptionsExtendColumns()
    {
        $definition = new InputTableOptions([
            'source' => 'test',
            'destination' => 'dest',
            'columns' => [
                [
                    'source' => 'col1',
                    'type' => 'VARCHAR',
                ],
                [
                    'source' => 'col2',
                    'type' => 'VARCHAR',
                ],
            ],
            'changed_since' => '-1 days',
            'where_column' => 'col1',
            'where_operator' => 'ne',
            'where_values' => ['1', '2'],
            'limit' => 100,
        ]);
        self::assertEquals([
            'columns' => ['col1', 'col2'],
            'changedSince' => '-1 days',
            'whereColumn' => 'col1',
            'whereValues' => ['1', '2'],
            'whereOperator' => 'ne',
            'limit' => 100,
        ], $definition->getStorageApiExportOptions(new InputTableStateList([])));
    }

    public function testGetLoadOptionsSimpleColumns()
    {
        $definition = new InputTableOptions([
            'source' => 'test',
            'destination' => 'dest',
            'columns' => ['col1', 'col2'],
            'changed_since' => '-1 days',
            'where_column' => 'col1',
            'where_operator' => 'ne',
            'where_values' => ['1', '2'],
            'limit' => 100,
        ]);
        self::assertEquals([
            'columns' => ['col1', 'col2'],
            'seconds' => '86400',
            'whereColumn' => 'col1',
            'whereValues' => ['1', '2'],
            'whereOperator' => 'ne',
            'rows' => 100,
        ], $definition->getStorageApiLoadOptions(new InputTableStateList([])));
    }

    public function testGetLoadOptionsExtendedColumns()
    {
        $definition = new InputTableOptions([
            'source' => 'test',
            'destination' => 'dest',
            'columns' => [
                [
                    'source' => 'col1',
                    'type' => 'VARCHAR',
                ],
                [
                    'source' => 'col2',
                    'type' => 'VARCHAR',
                ],
            ],
            'changed_since' => '-1 days',
            'where_column' => 'col1',
            'where_operator' => 'ne',
            'where_values' => ['1', '2'],
            'limit' => 100,
        ]);
        self::assertEquals([
            'columns' => [
                [
                    'source' => 'col1',
                    'type' => 'VARCHAR',
                ],
                [
                    'source' => 'col2',
                    'type' => 'VARCHAR',
                ],
            ],
            'seconds' => '86400',
            'whereColumn' => 'col1',
            'whereValues' => ['1', '2'],
            'whereOperator' => 'ne',
            'rows' => 100,
        ], $definition->getStorageApiLoadOptions(new InputTableStateList([])));
    }

    public function testInvalidColumns()
    {
        self::expectException(InvalidInputException::class);
        self::expectExceptionMessage(
            'Columns must be either specified as array of strings or as array of objects with type, but not both'
        );
        $definition = new InputTableOptions([
            'source' => 'test',
            'destination' => 'dest',
            'columns' => [
                'col1',
                [
                    'source' => 'col2',
                    'type' => 'VARCHAR',
                ],
            ],
            'changed_since' => '-1 days',
            'where_column' => 'col1',
            'where_operator' => 'ne',
            'where_values' => ['1', '2'],
            'limit' => 100,
        ]);
    }

    public function testGetExportOptionsDays()
    {
        $definition = new InputTableOptions([
            'source' => 'test',
            'days' => 2,
        ]);
        self::assertEquals([
            'changedSince' => '-2 days',
        ], $definition->getStorageApiExportOptions(new InputTableStateList([])));
    }

    public function testGetExportOptionsAdaptiveInputMapping()
    {
        $definition = new InputTableOptions([
            'source' => 'test',
            'changed_since' => InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE
        ]);
        $tablesState = new InputTableStateList([
            [
                'source' => 'test',
                'lastImportDate' => '1989-11-17T21:00:00+0200'
            ]
        ]);
        self::assertEquals([
            'changedSince' => '1989-11-17T21:00:00+0200',
        ], $definition->getStorageApiExportOptions($tablesState));
    }

    public function testGetExportOptionsAdaptiveInputMappingMissingTable()
    {
        $definition = new InputTableOptions([
            'source' => 'test',
            'changed_since' => InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE
        ]);
        $tablesState = new InputTableStateList([]);
        self::assertEquals([], $definition->getStorageApiExportOptions($tablesState));
    }

    public function testGetLoadOptionsAdaptiveInputMapping()
    {
        $definition = new InputTableOptions([
            'source' => 'test',
            'changed_since' => InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE
        ]);
        $tablesState = new InputTableStateList([
            [
                'source' => 'test',
                'lastImportDate' => '1989-11-17T21:00:00+0200'
            ]
        ]);
        self::expectExceptionMessage('Adaptive input mapping is not supported on input mapping to workspace.');
        self::expectException(InvalidInputException::class);
        $definition->getStorageApiLoadOptions(new InputTableStateList([]));
    }

    public function testGetLoadOptionsDaysMapping()
    {
        $definition = new InputTableOptions([
            'source' => 'test',
            'days' => 2,
        ]);
        self::expectExceptionMessage('Days option is not supported on workspace, use changed_since instead.');
        self::expectException(InvalidInputException::class);
        $definition->getStorageApiLoadOptions(new InputTableStateList([]));
    }
}
