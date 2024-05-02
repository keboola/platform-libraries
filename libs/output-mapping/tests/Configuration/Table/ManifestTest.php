<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Configuration\Table;

use Keboola\OutputMapping\Configuration\Table;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ManifestTest extends TestCase
{
    public function testBasicConfiguration(): void
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
            'write_always' => false,
            'tags' => [],
            'schema' => [],
        ];

        $processedConfiguration = (new Table\Manifest())->parse(['config' => $config]);
        self::assertEquals($expectedArray, $processedConfiguration);
    }

    public function testComplexConfiguration(): void
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
            'write_always' => false,
            'tags' => [],
            'schema' => [],
            'manifest_type' => 'table',
        ];

        $expectedArray = $config;

        $processedConfiguration = (new Table\Manifest())->parse(['config' => $config]);
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
        (new Table\Manifest())->parse(['config' => $config]);
    }

    public function testTableMetadataConfiguration(): void
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
            'write_always' => false,
            'tags' => [],
            'schema' => [],
        ];
        $expectedArray['metadata'] = $config['metadata'];

        $parsedConfig = (new Table\Manifest())->parse(['config' => $config]);
        self::assertEquals($expectedArray, $parsedConfig);
    }

    public function testColumnMetadataConfiguration(): void
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
            'write_always' => false,
            'tags' => [],
            'schema' => [],
        ];

        $expectedArray['column_metadata'] = $config['column_metadata'];

        $parsedConfig = (new Table\Manifest())->parse(['config' => $config]);

        self::assertEquals($expectedArray, $parsedConfig);
    }

    /** @dataProvider provideDeleteWhereColumnData */
    public function testWhereColumnSetButWhereValuesInvalid(array $config): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage(
            'Invalid configuration for path "table": When "delete_where_column" option is set, ' .
            'then the "delete_where_values" is required.',
        );
        (new Table\Manifest())->parse(['config' => $config]);
    }

    public function provideDeleteWhereColumnData(): array
    {
        return [
            'Empty delete_where_values' => [
                'config' => [
                    'delete_where_column' => 'col',
                    'delete_where_values' => [],
                ],
            ],
            'Empty delete_where_values items' => [
                'config' => [
                    'delete_where_column' => 'col',
                ],
            ],
        ];
    }
}
