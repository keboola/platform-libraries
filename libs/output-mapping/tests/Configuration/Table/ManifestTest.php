<?php

namespace Keboola\OutputMapping\Tests\Configuration\Table;

use Keboola\OutputMapping\Configuration\Table;

class ManifestTest extends \PHPUnit_Framework_TestCase
{
    public function testBasicConfiguration()
    {
        $config = [
            'destination' => 'in.c-main.test',
        ];

        $expectedArray = [
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
        ];

        $processedConfiguration = (new Table\Manifest())->parse(['config' => $config]);
        $this->assertEquals($expectedArray, $processedConfiguration);
    }

    /**
     *
     */
    public function testComplexConfiguration()
    {
        $config = [
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
        ];

        $expectedArray = $config;

        $processedConfiguration = (new Table\Manifest())->parse(['config' => $config]);
        $this->assertEquals($expectedArray, $processedConfiguration);
    }


    /**
     * @expectedException \Symfony\Component\Config\Definition\Exception\InvalidConfigurationException
     * @expectedExceptionMessage Invalid configuration for path "table.delete_where_operator": Invalid operator in delete_where_operator "abc"
     */
    public function testInvalidWhereOperator()
    {
        $config = [
            'destination' => 'in.c-main.test',
            'delete_where_operator' => 'abc',
        ];
        (new Table\Manifest())->parse(['config' => $config]);
    }

    public function testTableMetadataConfiguration()
    {
        $config = [
            'destination' => 'in.c-main.test',
            'metadata' => [
                [
                    'key' => 'table.key.one',
                    'value' => 'table value one',
                ],
                [
                    'key' => 'table.key.two',
                    'value' => 'table value two',
                ],
            ],
        ];

        $expectedArray = [
            'destination' => 'in.c-main.test',
            'primary_key' => [],
            'distribution_key' => [],
            'columns' => [],
            'incremental' => false,
            'delete_where_values' => [],
            'delete_where_operator' => 'eq',
            'delimiter' => ',',
            'enclosure' => '"',
            'column_metadata' => [],
        ];
        $expectedArray['metadata'] = $config['metadata'];

        $parsedConfig = (new Table\Manifest())->parse(['config' => $config]);
        $this->assertEquals($expectedArray, $parsedConfig);
    }

    public function testColumnMetadataConfiguration()
    {
        $config = [
            'destination' => 'in.c-main.test',
            'column_metadata' => [
                'colA' => [
                    [
                        'key' => 'column.key.one',
                        'value' => 'column value A',
                    ],
                    [
                        'key' => 'column.key.two',
                        'value' => 'column value A',
                    ],
                ],
                'colB' => [
                    [
                        'key' => 'column.key.one',
                        'value' => 'column value B',
                    ],
                    [
                        'key' => 'column.key.two',
                        'value' => 'column value B',
                    ],
                ],
            ],
        ];

        $expectedArray = [
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
        ];

        $expectedArray['column_metadata'] = $config['column_metadata'];

        $parsedConfig = (new Table\Manifest())->parse(['config' => $config]);

        $this->assertEquals($expectedArray, $parsedConfig);
    }
}
