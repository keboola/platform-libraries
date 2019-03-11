<?php

namespace Keboola\InputMapping\Tests;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader\Definition\TableDefinition;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class TableDefinitionTest extends \PHPUnit_Framework_TestCase
{

    public function testConstructorEmptyValues()
    {
        $definition = new TableDefinition(['source' => 'test']);
        self::assertEquals('test', $definition->getSource());
    }

    public function testConstructorMissingSource()
    {
        self::expectException(InvalidConfigurationException::class);
        self::expectExceptionMessage('The child node "source" at path "table" must be configured.');
        new TableDefinition([]);
    }

    public function testConstructorDaysAndChangedSince()
    {
        self::expectException(InvalidInputException::class);
        self::expectExceptionMessage('Cannot set both parameters "days" and "changed_since".');
        new TableDefinition(['source' => 'test', 'days' => 1, 'changed_since' => '-2 days']);
    }

    public function testGetExportOptionsEmptyValue()
    {
        $definition = new TableDefinition(['source' => 'test']);
        self::assertEquals([], $definition->getExportOptions());
    }

    public function testGetExportOptions()
    {
        $definition = new TableDefinition([
            'source' => 'test',
            'columns' => ['col1'],
            'changed_since' => '-1 days',
            'where_column' => 'col1',
            'where_operator' => 'ne',
            'where_values' => ['1', '2'],
            'limit' => 100,
        ]);
        self::assertEquals([
            'columns' => ['col1'],
            'changedSince' => '-1 days',
            'whereColumn' => 'col1',
            'whereValues' => ['1', '2'],
            'whereOperator' => 'ne',
            'limit' => 100,
        ], $definition->getExportOptions());
    }

    public function testGetExportOptionsDays()
    {
        $definition = new TableDefinition([
            'source' => 'test',
            'days' => 2,
        ]);
        self::assertEquals([
            'changedSince' => '-2 days',
        ], $definition->getExportOptions());
    }
}
