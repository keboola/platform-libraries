<?php

namespace Keboola\OutputMapping\Tests;

use Keboola\OutputMapping\Configuration\Table;

class OutputTableConfigurationTest extends \PHPUnit_Framework_TestCase
{
    public function testBasicConfiguration()
    {
        $config = [
            "source" => "data.csv",
            "destination" => "in.c-main.test"
        ];

        $expectedArray = [
            "source" => "data.csv",
            "destination" => "in.c-main.test",
            "primary_key" => [],
            "columns" => [],
            "incremental" => false,
            "delete_where_values" => [],
            "delete_where_operator" => "eq",
            "delimiter" => ",",
            "enclosure" => "\"",
            "metadata" => [],
            "column_metadata" => []
        ];

        $processedConfiguration = (new Table())->parse(["config" => $config]);
        $this->assertEquals($expectedArray, $processedConfiguration);
    }

    public function testComplexConfiguration()
    {
        $config = [
            "source" => "test",
            "destination" => "in.c-main.test",
            "incremental" => true,
            "primary_key" => ["Id", "Name"],
            "columns" => ["Id", "Name", "status"],
            "delete_where_column" => "status",
            "delete_where_values" => ["val1", "val2"],
            "delete_where_operator" => "ne",
            "delimiter" => "\t",
            "enclosure" => "'",
            "metadata" => [],
            "column_metadata" => []
        ];

        $expectedArray = $config;

        $processedConfiguration = (new Table())->parse(["config" => $config]);
        $this->assertEquals($expectedArray, $processedConfiguration);
    }

    /**
     * @expectedException \Symfony\Component\Config\Definition\Exception\InvalidConfigurationException
     * @expectedExceptionMessage Invalid configuration for path "table.delete_where_operator": Invalid operator in delete_where_operator "abc"
     */
    public function testInvalidWhereOperator()
    {
        $config = [
            "destination" => "in.c-main.test",
            "delete_where_operator" => 'abc'
        ];

        (new Table())->parse(["config" => $config]);
    }

    /**
     * @expectedException \Symfony\Component\Config\Definition\Exception\InvalidConfigurationException
     * @expectedExceptionMessage The child node "source" at path "table" must be configured
     */
    public function testEmptyConfiguration()
    {
        (new Table())->parse(["config" => []]);
    }

    public function testPrimaryKeyEmptyString()
    {
        $config = [
            "source" => "test",
            "destination" => "in.c-main.test",
            "primary_key" => [""],
        ];

        $expectedArray = [
            "source" => "test",
            "destination" => "in.c-main.test",
            "primary_key" => [""],
            "columns" => [],
            "incremental" => false,
            "delete_where_values" => [],
            "delete_where_operator" => "eq",
            "delimiter" => ",",
            "enclosure" => "\"",
            "metadata" => [],
            "column_metadata" => []
        ];
        $processedConfiguration = (new Table())->parse(["config" => $config]);
        $this->assertEquals($expectedArray, $processedConfiguration);
    }

    public function testEmptyDeleteWhereOp()
    {
        $config = [
            "source" => "data.csv",
            "destination" => "in.c-main.test",
            "delete_where_operator" => "",
        ];

        $expectedArray = [
            "source" => "data.csv",
            "destination" => "in.c-main.test",
            "primary_key" => [],
            "columns" => [],
            "incremental" => false,
            "delete_where_values" => [],
            "delete_where_operator" => "eq",
            "delimiter" => ",",
            "enclosure" => "\"",
            "metadata" => [],
            "column_metadata" => []
        ];

        $processedConfiguration = (new Table())->parse(["config" => $config]);
        $this->assertEquals($expectedArray, $processedConfiguration);
    }
}
