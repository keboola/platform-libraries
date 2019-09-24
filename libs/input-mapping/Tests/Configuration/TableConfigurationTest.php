<?php

namespace Keboola\InputMapping\Tests\Configuration;

use Keboola\InputMapping\Configuration\Table;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class TableConfigurationTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @return mixed[][]
     */
    public function provideValidConfigs()
    {
        return [
            'BasicConfiguration' => [
                [
                    "source" => "in.c-main.test",
                ],
                [
                    "source" => "in.c-main.test",
                    "columns" => [],
                    "where_values" => [],
                    "where_operator" => "eq",
                ],
            ],
            'ComplexConfiguration' => [
                [
                    "source" => "in.c-main.test",
                    "destination" => "test",
                    "changed_since" => "-1 days",
                    "columns" => ["Id", "Name"],
                    "where_column" => "status",
                    "where_values" => ["val1", "val2"],
                    "where_operator" => "ne",
                ],
                [
                    "source" => "in.c-main.test",
                    "destination" => "test",
                    "changed_since" => "-1 days",
                    "columns" => ["Id", "Name"],
                    "where_column" => "status",
                    "where_values" => ["val1", "val2"],
                    "where_operator" => "ne",
                ],
            ],
            'DaysNullConfiguration' => [
                [
                    "source" => "in.c-main.test",
                    "destination" => "test",
                    "days" => null,
                    "columns" => ["Id", "Name"],
                    "where_column" => "status",
                    "where_values" => ["val1", "val2"],
                    "where_operator" => "ne",
                ],
                [
                    "source" => "in.c-main.test",
                    "destination" => "test",
                    "days" => null,
                    "columns" => ["Id", "Name"],
                    "where_column" => "status",
                    "where_values" => ["val1", "val2"],
                    "where_operator" => "ne",
                ],
            ],
            'DaysConfiguration' => [
                [
                    "source" => "in.c-main.test",
                    "destination" => "test",
                    "days" => 1,
                    "columns" => ["Id", "Name"],
                    "where_column" => "status",
                    "where_values" => ["val1", "val2"],
                    "where_operator" => "ne",
                ],
                [
                    "source" => "in.c-main.test",
                    "destination" => "test",
                    "days" => 1,
                    "columns" => ["Id", "Name"],
                    "where_column" => "status",
                    "where_values" => ["val1", "val2"],
                    "where_operator" => "ne",
                ],
            ],
            'ChangedSinceNullConfiguration' => [
                [
                    "source" => "in.c-main.test",
                    "destination" => "test",
                    "changed_since" => null,
                    "columns" => ["Id", "Name"],
                    "where_column" => "status",
                    "where_values" => ["val1", "val2"],
                    "where_operator" => "ne",
                ],
                [
                    "source" => "in.c-main.test",
                    "destination" => "test",
                    "changed_since" => null,
                    "columns" => ["Id", "Name"],
                    "where_column" => "status",
                    "where_values" => ["val1", "val2"],
                    "where_operator" => "ne",
                ],
            ],
            'ChangedSinceConfiguration' => [
                [
                    "source" => "in.c-main.test",
                    "destination" => "test",
                    "changed_since" => "-1 days",
                    "columns" => ["Id", "Name"],
                    "where_column" => "status",
                    "where_values" => ["val1", "val2"],
                    "where_operator" => "ne",
                ],
                [
                    "source" => "in.c-main.test",
                    "destination" => "test",
                    "changed_since" => "-1 days",
                    "columns" => ["Id", "Name"],
                    "where_column" => "status",
                    "where_values" => ["val1", "val2"],
                    "where_operator" => "ne",
                ],
            ],
        ];
    }

    /**
     * @dataProvider provideValidConfigs
     */
    public function testValidConfigDefinition(
        array $config,
        array $expected
    ) {
        $processedConfiguration = (new Table())->parse(["config" => $config]);
        self::assertEquals($expected, $processedConfiguration);
    }

    /**
     * @return mixed[][]
     */
    public function provideInvalidConfigs()
    {
        return [
            'InvalidWhereOperator' => [
                [
                    "source" => "in.c-main.test",
                    "where_operator" => 'abc',
                ],
                InvalidConfigurationException::class,
                'Invalid configuration for path "table.where_operator": Invalid operator in where_operator "abc"',
            ],
            'testEmptyConfiguration' => [
                [],
                InvalidConfigurationException::class,
                'The child node "source" at path "table" must be configured.',
            ],
            'EmptySourceConfiguration' => [
                ["source" => ""],
                InvalidConfigurationException::class,
                'The path "table.source" cannot contain an empty value, but got "".',
            ],
        ];
    }

    /**
     * @dataProvider provideInvalidConfigs
     */
    public function testInvalidConfigDefinition(
        array $config,
        $exception,
        $exceptionMessage
    ) {
        $this->expectException($exception);
        $this->expectExceptionMessage($exceptionMessage);
        (new Table())->parse(["config" => $config]);
    }

    public function testEmptyWhereOperator()
    {
        $config = [
            "source" => "in.c-main.test",
            "where_operator" => "",
        ];

        $expectedArray = $config;
        $expectedArray["where_operator"] = "eq";
        $expectedArray["columns"] = [];
        $expectedArray["where_values"] = [];
        $processedConfiguration = (new Table())->parse(["config" => $config]);
        self::assertEquals($expectedArray, $processedConfiguration);
    }
}
