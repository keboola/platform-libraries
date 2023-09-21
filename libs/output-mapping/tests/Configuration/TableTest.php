<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Configuration;

use Keboola\OutputMapping\Configuration\Table;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class TableTest extends TestCase
{
    public function testBasicConfiguration(): void
    {
        $config = [
            'source' => 'data.csv',
            'destination' => 'in.c-main.test',
        ];

        $expectedArray = [
            'source' => 'data.csv',
            'destination' => 'in.c-main.test',
            'primary_key' => [],
            'distribution_key' => [],
            'columns' => [],
            'incremental' => false,
            'delete_where_values' => [],
            'delete_where_operator' => 'eq',
            'delimiter' => ',',
            'enclosure' => '"',
            'metadata' => [],
            'column_metadata' => [],
            'write_always' => false,
        ];

        $processedConfiguration = (new Table())->parse(['config' => $config]);
        self::assertEquals($expectedArray, $processedConfiguration);
    }

    public function testComplexConfiguration(): void
    {
        $config = [
            'source' => 'test',
            'destination' => 'in.c-main.test',
            'incremental' => true,
            'primary_key' => ['Id', 'Name'],
            'distribution_key' => [],
            'columns' => ['Id', 'Name', 'status'],
            'delete_where_column' => 'status',
            'delete_where_values' => ['val1', 'val2'],
            'delete_where_operator' => 'ne',
            'delimiter' => '\t',
            'enclosure' => '\'',
            'metadata' => [],
            'column_metadata' => [],
            'write_always' => false,
        ];

        $expectedArray = $config;

        $processedConfiguration = (new Table())->parse(['config' => $config]);
        self::assertEquals($expectedArray, $processedConfiguration);
    }

    public function testInvalidWhereOperator(): void
    {
        $config = [
            'destination' => 'in.c-main.test',
            'delete_where_operator' => 'abc',
        ];

        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage(
            'Invalid configuration for path "table.delete_where_operator": ' .
            'Invalid operator in delete_where_operator "abc"',
        );
        (new Table())->parse(['config' => $config]);
    }

    public function testEmptyConfiguration(): void
    {
        self::expectException(InvalidConfigurationException::class);
        self::expectExceptionMessage('The child config "source" under "table" must be configured.');

        (new Table())->parse(['config' => []]);
    }

    public function testPrimaryKeyEmptyString(): void
    {
        $config = [
            'source' => 'test',
            'destination' => 'in.c-main.test',
            'primary_key' => [''],
        ];

        $expectedArray = [
            'source' => 'test',
            'destination' => 'in.c-main.test',
            'primary_key' => [''],
            'distribution_key' => [],
            'columns' => [],
            'incremental' => false,
            'delete_where_values' => [],
            'delete_where_operator' => 'eq',
            'delimiter' => ',',
            'enclosure' => '"',
            'metadata' => [],
            'column_metadata' => [],
            'write_always' => false,
        ];
        $processedConfiguration = (new Table())->parse(['config' => $config]);
        self::assertEquals($expectedArray, $processedConfiguration);
    }

    public function testEmptyDeleteWhereOp(): void
    {
        $config = [
            'source' => 'data.csv',
            'destination' => 'in.c-main.test',
            'delete_where_operator' => '',
        ];

        $expectedArray = [
            'source' => 'data.csv',
            'destination' => 'in.c-main.test',
            'primary_key' => [],
            'distribution_key' => [],
            'columns' => [],
            'incremental' => false,
            'delete_where_values' => [],
            'delete_where_operator' => 'eq',
            'delimiter' => ',',
            'enclosure' => '"',
            'metadata' => [],
            'column_metadata' => [],
            'write_always' => false,
        ];

        $processedConfiguration = (new Table())->parse(['config' => $config]);
        self::assertEquals($expectedArray, $processedConfiguration);
    }
}
