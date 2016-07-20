<?php

namespace Keboola\InputMapping\Tests;

use Keboola\InputMapping\Configuration\Table;

class InputTableConfigurationTest extends \PHPUnit_Framework_TestCase
{

    /**
     *
     */
    public function testBasicConfiguration()
    {
        $config = array(
            "source" => "in.c-main.test"
        );

        $expectedArray = array(
            "source" => "in.c-main.test",
            "columns" => array(),
            "where_values" => array(),
            "where_operator" => "eq"
        );
        $processedConfiguration = (new Table())->parse(array("config" => $config));
        $this->assertEquals($expectedArray, $processedConfiguration);
    }

    /**
     *
     */
    public function testComplexConfiguration()
    {
        $config = array(
            "source" => "in.c-main.test",
            "destination" => "test",
            "days" => 1,
            "columns" => array("Id", "Name"),
            "where_column" => "status",
            "where_values" => array("val1", "val2"),
            "where_operator" => "ne"
        );

        $expectedArray = $config;

        $processedConfiguration = (new Table())->parse(array("config" => $config));
        $this->assertEquals($expectedArray, $processedConfiguration);
    }

    /**
     * @expectedException \Symfony\Component\Config\Definition\Exception\InvalidConfigurationException
     * @expectedExceptionMessage Invalid configuration for path "table.where_operator": Invalid operator in where_operator "abc"
     */
    public function testInvalidWhereOperator()
    {
        $config = array(
            "source" => "in.c-main.test",
            "where_operator" => 'abc'
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
}
