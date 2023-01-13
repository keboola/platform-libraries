<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Options;

use Generator;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptions;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class InputTableOptionsTest extends TestCase
{

    public function testGetSource(): void
    {
        $definition = new InputTableOptions(['source' => 'test']);
        self::assertEquals('test', $definition->getSource());
    }

    public function testGetDestination(): void
    {
        $definition = new InputTableOptions(['source' => 'test', 'destination' => 'dest']);
        self::assertEquals('dest', $definition->getDestination());
    }

    /**
     * @dataProvider definitionProvider
     */
    public function testGetDefinition(array $input, array $expected): void
    {
        $definition = new InputTableOptions($input);
        self::assertEquals($expected, $definition->getDefinition());
    }

    public function definitionProvider(): Generator
    {
        yield 'no columns' => [
            [
                'source' => 'test',
                'destination' => 'dest',
            ],
            [
                'source' => 'test',
                'destination' => 'dest',
                'columns' => [],
                'where_values' => [],
                'where_operator' => 'eq',
                'column_types' => [],
                'overwrite' => false,
                'use_view' => false,
            ],
        ];
        yield 'simple columns' => [
            [
                'source' => 'test',
                'destination' => 'dest',
                'columns' => ['a', 'b'],
            ],
            [
                'source' => 'test',
                'destination' => 'dest',
                'columns' => ['a', 'b'],
                'where_values' => [],
                'where_operator' => 'eq',
                'column_types' => [
                    ['source' => 'a'],
                    ['source' => 'b'],
                ],
                'overwrite' => false,
                'use_view' => false,
            ],
        ];
        yield 'complex columns' => [
            [
                'source' => 'test',
                'destination' => 'dest',
                'column_types' => [
                    [
                        'source' => 'a',
                        'destination' => 'a',
                    ],
                    [
                        'source' => 'b',
                    ],
                ],
            ],
            [
                'source' => 'test',
                'destination' => 'dest',
                'columns' => ['a', 'b'],
                'where_values' => [],
                'where_operator' => 'eq',
                'column_types' => [
                    [
                        'source' => 'a',
                        'destination' => 'a',
                    ],
                    [
                        'source' => 'b',
                    ],
                ],
                'overwrite' => false,
                'use_view' => false,
            ],
        ];
    }

    public function testGetColumns(): void
    {
        $definition = new InputTableOptions(['source' => 'test', 'columns' => ['col1', 'col2']]);
        self::assertEquals(['col1', 'col2'], $definition->getColumnNamesFromTypes());
    }

    public function testGetColumnsExtended(): void
    {
        $definition = new InputTableOptions(
            ['source' => 'test', 'column_types' => [['source' => 'col1'], ['source' => 'col2']]]
        );
        self::assertEquals(['col1', 'col2'], $definition->getColumnNamesFromTypes());
    }

    public function testConstructorMissingSource(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('Either "source" or "source_search" must be configured.');
        new InputTableOptions([]);
    }

    public function testConstructorDaysAndChangedSince(): void
    {
        $this->expectException(InvalidInputException::class);
        $this->expectExceptionMessage('Cannot set both parameters "days" and "changed_since".');
        new InputTableOptions(['source' => 'test', 'days' => 1, 'changed_since' => '-2 days']);
    }

    public function testGetExportOptionsEmptyValue(): void
    {
        $definition = new InputTableOptions(['source' => 'test']);
        self::assertEquals(
            ['overwrite' => false],
            $definition->getStorageApiExportOptions(new InputTableStateList([]))
        );
    }

    public function testGetExportOptionsSimpleColumns(): void
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
            'overwrite' => false,
        ], $definition->getStorageApiExportOptions(new InputTableStateList([])));
    }

    public function testGetExportOptionsExtendColumns(): void
    {
        $definition = new InputTableOptions([
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
        ]);
        self::assertEquals([
            'columns' => ['col1', 'col2'],
            'changedSince' => '-1 days',
            'whereColumn' => 'col1',
            'whereValues' => ['1', '2'],
            'whereOperator' => 'ne',
            'limit' => 100,
            'overwrite' => false,
        ], $definition->getStorageApiExportOptions(new InputTableStateList([])));
    }

    public function testGetLoadOptionsSimpleColumns(): void
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
            'columns' => [
                ['source' => 'col1'],
                ['source' => 'col2'],
            ],
            'seconds' => 86400,
            'whereColumn' => 'col1',
            'whereValues' => ['1', '2'],
            'whereOperator' => 'ne',
            'rows' => 100,
            'overwrite' => false,
        ], $definition->getStorageApiLoadOptions(new InputTableStateList([])));
    }

    public function testGetLoadOptionsExtendedColumns(): void
    {
        $definition = new InputTableOptions([
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
        ]);
        self::assertEquals([
            'columns' => [
                [
                    'source' => 'col1',
                    'type' => 'VARCHAR',
                    'length' => '200',
                    'destination' => 'colone',
                    'nullable' => false,
                    'convertEmptyValuesToNull' => true,
                ],
                [
                    'source' => 'col2',
                    'type' => 'VARCHAR',
                    'nullable' => true,
                    'convertEmptyValuesToNull' => false,
                ],
            ],
            'seconds' => 86400,
            'whereColumn' => 'col1',
            'whereValues' => ['1', '2'],
            'whereOperator' => 'ne',
            'rows' => 100,
            'overwrite' => false,
        ], $definition->getStorageApiLoadOptions(new InputTableStateList([])));
    }

    public function testInvalidColumnsMissing(): void
    {
        $this->expectException(InvalidInputException::class);
        $this->expectExceptionMessage(
            'Both "columns" and "column_types" are specified, "columns" field contains surplus columns: "col1".'
        );
        new InputTableOptions([
            'source' => 'test',
            'destination' => 'dest',
            'columns' => ['col2', 'col1'],
            'column_types' => [
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

    public function testInvalidColumnSurplus(): void
    {
        $this->expectException(InvalidInputException::class);
        $this->expectExceptionMessage(
            'Both "columns" and "column_types" are specified, "column_types" field contains surplus columns: "col2".'
        );
        new InputTableOptions([
            'source' => 'test',
            'destination' => 'dest',
            'columns' => ['col1'],
            'column_types' => [
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
    }

    public function testGetExportOptionsDays(): void
    {
        $definition = new InputTableOptions([
            'source' => 'test',
            'days' => 2,
        ]);
        self::assertEquals([
            'changedSince' => '-2 days',
            'overwrite' => false,
        ], $definition->getStorageApiExportOptions(new InputTableStateList([])));
    }

    public function testGetExportOptionsAdaptiveInputMapping(): void
    {
        $definition = new InputTableOptions([
            'source' => 'test',
            'changed_since' => InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE,
        ]);
        $tablesState = new InputTableStateList([
            [
                'source' => 'test',
                'lastImportDate' => '1989-11-17T21:00:00+0200',
            ],
        ]);
        self::assertEquals([
            'changedSince' => '1989-11-17T21:00:00+0200',
            'overwrite' => false,
        ], $definition->getStorageApiExportOptions($tablesState));
    }

    public function testGetExportOptionsAdaptiveInputMappingMissingTable(): void
    {
        $definition = new InputTableOptions([
            'source' => 'test',
            'changed_since' => InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE,
        ]);
        $tablesState = new InputTableStateList([]);
        self::assertEquals(['overwrite' => false], $definition->getStorageApiExportOptions($tablesState));
    }

    public function testGetLoadOptionsAdaptiveInputMapping(): void
    {
        $definition = new InputTableOptions([
            'source' => 'test',
            'changed_since' => InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE,
        ]);
        $this->expectExceptionMessage('Adaptive input mapping is not supported on input mapping to workspace.');
        $this->expectException(InvalidInputException::class);
        $definition->getStorageApiLoadOptions(new InputTableStateList([]));
    }

    public function testGetLoadOptionsDaysMapping(): void
    {
        $definition = new InputTableOptions([
            'source' => 'test',
            'days' => 2,
        ]);
        $this->expectExceptionMessage('Days option is not supported on workspace, use changed_since instead.');
        $this->expectException(InvalidInputException::class);
        $definition->getStorageApiLoadOptions(new InputTableStateList([]));
    }

    public function testIsUseView(): void
    {
        $definition = new InputTableOptions([
            'source' => 'test',
            'use_view' => true,
        ]);

        self::assertTrue($definition->isUseView());
    }
}
