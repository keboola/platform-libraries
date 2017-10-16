<?php

namespace Keboola\InputMapping\Tests;

use Keboola\InputMapping\Configuration\Table;

class InputTableConfigurationTest extends \PHPUnit_Framework_TestCase
{
    public function testBasicConfiguration()
    {
        $config = [
            "source" => "in.c-main.test"
        ];

        $expectedArray = [
            "source" => "in.c-main.test",
            "columns" => [],
            "where_values" => [],
            "where_operator" => "eq"
        ];
        $processedConfiguration = (new Table())->parse(["config" => $config]);
        self::assertEquals($expectedArray, $processedConfiguration);
    }

    public function testComplexConfiguration()
    {
        $config = [
            "source" => "in.c-main.test",
            "destination" => "test",
            "changed_since" => "-1 days",
            "columns" => ["Id", "Name"],
            "where_column" => "status",
            "where_values" => ["val1", "val2"],
            "where_operator" => "ne"
        ];

        $expectedArray = $config;
        $processedConfiguration = (new Table())->parse(["config" => $config]);
        self::assertEquals($expectedArray, $processedConfiguration);
    }

    public function testDaysNullConfiguration()
    {
        $config = [
            "source" => "in.c-main.test",
            "destination" => "test",
            "days" => null,
            "columns" => ["Id", "Name"],
            "where_column" => "status",
            "where_values" => ["val1", "val2"],
            "where_operator" => "ne"
        ];

        $expectedArray = $config;
        $processedConfiguration = (new Table())->parse(["config" => $config]);
        self::assertEquals($expectedArray, $processedConfiguration);
    }

    public function testDaysConfiguration()
    {
        $config = [
            "source" => "in.c-main.test",
            "destination" => "test",
            "days" => 1,
            "columns" => ["Id", "Name"],
            "where_column" => "status",
            "where_values" => ["val1", "val2"],
            "where_operator" => "ne"
        ];

        $expectedArray = $config;
        $processedConfiguration = (new Table())->parse(["config" => $config]);
        self::assertEquals($expectedArray, $processedConfiguration);
    }

    public function testChangedSinceNullConfiguration()
    {
        $config = [
            "source" => "in.c-main.test",
            "destination" => "test",
            "changed_since" => null,
            "columns" => ["Id", "Name"],
            "where_column" => "status",
            "where_values" => ["val1", "val2"],
            "where_operator" => "ne"
        ];

        $expectedArray = $config;
        $processedConfiguration = (new Table())->parse(["config" => $config]);
        self::assertEquals($expectedArray, $processedConfiguration);
    }

    public function testChangedSinceConfiguration()
    {
        $config = [
            "source" => "in.c-main.test",
            "destination" => "test",
            "changed_since" => "-1 days",
            "columns" => ["Id", "Name"],
            "where_column" => "status",
            "where_values" => ["val1", "val2"],
            "where_operator" => "ne"
        ];

        $expectedArray = $config;
        $processedConfiguration = (new Table())->parse(["config" => $config]);
        self::assertEquals($expectedArray, $processedConfiguration);
    }

    /**
     * @expectedException \Symfony\Component\Config\Definition\Exception\InvalidConfigurationException
     * @expectedExceptionMessage Invalid configuration for path "table.where_operator": Invalid operator in where_operator "abc"
     */
    public function testInvalidWhereOperator()
    {
        $config = [
            "source" => "in.c-main.test",
            "where_operator" => 'abc'
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
}
