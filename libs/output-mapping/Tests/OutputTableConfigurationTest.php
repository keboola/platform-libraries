<?php

namespace Keboola\OutputMapping\Tests;

use Keboola\OutputMapping\Configuration\Output\Table;

class OutputTableConfigurationTest extends \PHPUnit_Framework_TestCase
{
    public function testBasicConfiguration()
    {
        $config = array(
            "source" => "data.csv",
            "destination" => "in.c-main.test"
        );

        $expectedArray = array(
            "source" => "data.csv",
            "destination" => "in.c-main.test",
            "primary_key" => array(),
            "columns" => array(),
            "incremental" => false,
            "delete_where_values" => array(),
            "delete_where_operator" => "eq",
            "delimiter" => ",",
            "enclosure" => "\"",
            "escaped_by" => "",
            "metadata" => [],
            "column_metadata" => []
        );

        $processedConfiguration = (new Table())->parse(array("config" => $config));
        $this->assertEquals($expectedArray, $processedConfiguration);
    }

    public function testComplexConfiguration()
    {
        $config = array(
            "source" => "test",
            "destination" => "in.c-main.test",
            "incremental" => true,
            "primary_key" => array("Id", "Name"),
            "columns" => array("Id", "Name", "status"),
            "delete_where_column" => "status",
            "delete_where_values" => array("val1", "val2"),
            "delete_where_operator" => "ne",
            "delimiter" => "\t",
            "enclosure" => "'",
            "escaped_by" => "\\",
            "metadata" => [],
            "column_metadata" => []
        );

        $expectedArray = $config;

        $processedConfiguration = (new Table())->parse(array("config" => $config));
        $this->assertEquals($expectedArray, $processedConfiguration);
    }

    /**
     * @expectedException \Symfony\Component\Config\Definition\Exception\InvalidConfigurationException
     * @expectedExceptionMessage Invalid configuration for path "table.delete_where_operator": Invalid operator in delete_where_operator "abc"
     */
    public function testInvalidWhereOperator()
    {
        $config = array(
            "destination" => "in.c-main.test",
            "delete_where_operator" => 'abc'
        );

        (new Table())->parse(array("config" => $config));
    }

    /**
     * @expectedException \Symfony\Component\Config\Definition\Exception\InvalidConfigurationException
     * @expectedExceptionMessage The child node "source" at path "table" must be configured
     */
    public function testEmptyConfiguration()
    {
        (new Table())->parse(array("config" => array()));
    }

    public function testPrimaryKeyEmptyString()
    {
        $config = array(
            "source" => "test",
            "destination" => "in.c-main.test",
            "primary_key" => array(""),
        );

        $expectedArray = array(
            "source" => "test",
            "destination" => "in.c-main.test",
            "primary_key" => array(""),
            "columns" => array(),
            "incremental" => false,
            "delete_where_values" => array(),
            "delete_where_operator" => "eq",
            "delimiter" => ",",
            "enclosure" => "\"",
            "escaped_by" => "",
            "metadata" => [],
            "column_metadata" => []
        );
        $processedConfiguration = (new Table())->parse(array("config" => $config));
        $this->assertEquals($expectedArray, $processedConfiguration);
    }
}
