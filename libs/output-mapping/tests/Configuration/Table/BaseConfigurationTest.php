<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Configuration\Table;

use Generator;
use Keboola\OutputMapping\Configuration\Table;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class BaseConfigurationTest extends TestCase
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

        $this->testManifestAndConfig($config, $expectedArray);
    }

    public function testMinimalSchemaConfig(): void
    {
        $config = [
            'destination' => 'in.c-main.test',
            'schema' => [
                [
                    'name' => 'test',
                    'data_type' => [
                        'base' => [
                            'type' => 'string',
                        ],
                    ],
                ],
            ],
        ];

        $expectedArray = [
            'destination' => 'in.c-main.test','primary_key' => [],
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
            'schema' => [
                [
                    'name' => 'test',
                    'data_type' => [
                        'base' => [
                            'type' => 'string',
                        ],
                    ],
                    'nullable' => true,
                    'primary_key' => false,
                    'distribution_key' => false,
                ],
            ],
        ];

        $this->testManifestAndConfig($config, $expectedArray);
    }

    public function testNormalizeDefaultAndLengthIntValue(): void
    {
        $config = [
            'destination' => 'in.c-main.test',
            'schema' => [
                [
                    'name' => 'test',
                    'data_type' => [
                        'base' => [
                            'type' => 'string',
                            'default' => 1234,
                            'length' => 1234,
                        ],
                        'snowflake' => [
                            'type' => 'string',
                            'default' => 5678,
                            'length' => 5678,
                        ],
                    ],
                ],
            ],
        ];

        $expectedArray = [
            'destination' => 'in.c-main.test','primary_key' => [],
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
            'schema' => [
                [
                    'name' => 'test',
                    'data_type' => [
                        'base' => [
                            'type' => 'string',
                            'default' => '1234',
                            'length' => '1234',
                        ],
                        'snowflake' => [
                            'type' => 'string',
                            'default' => '5678',
                            'length' => '5678',
                        ],
                    ],
                    'nullable' => true,
                    'primary_key' => false,
                    'distribution_key' => false,
                ],
            ],
        ];

        $this->testManifestAndConfig($config, $expectedArray);
    }

    public function testNormalizeDefaultAndLengthBoolValue(): void
    {
        $config = [
            'destination' => 'in.c-main.test',
            'schema' => [
                [
                    'name' => 'test',
                    'data_type' => [
                        'base' => [
                            'type' => 'string',
                            'default' => true,
                            'length' => false,
                        ],
                        'snowflake' => [
                            'type' => 'string',
                            'default' => false,
                            'length' => true,
                        ],
                    ],
                ],
            ],
        ];

        $expectedArray = [
            'destination' => 'in.c-main.test','primary_key' => [],
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
            'schema' => [
                [
                    'name' => 'test',
                    'data_type' => [
                        'base' => [
                            'type' => 'string',
                            'default' => 'true',
                            'length' => 'false',
                        ],
                        'snowflake' => [
                            'type' => 'string',
                            'default' => 'false',
                            'length' => 'true',
                        ],
                    ],
                    'nullable' => true,
                    'primary_key' => false,
                    'distribution_key' => false,
                ],
            ],
        ];

        $this->testManifestAndConfig($config, $expectedArray);
    }

    public function testSchemaOptionalBackendDataType(): void
    {
        $config = [
            'destination' => 'in.c-main.test',
            'schema' => [
                [
                    'name' => 'test',
                    'data_type' => [
                        'base' => [
                            'type' => 'string',
                        ],
                        'snowflake' => [
                            'type' => 'string',
                        ],
                    ],
                ],
            ],
        ];

        $expectedArray = [
            'destination' => 'in.c-main.test','primary_key' => [],
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
            'schema' => [
                [
                    'name' => 'test',
                    'data_type' => [
                        'base' => [
                            'type' => 'string',
                        ],
                        'snowflake' => [
                            'type' => 'string',
                        ],
                    ],
                    'nullable' => true,
                    'primary_key' => false,
                    'distribution_key' => false,
                ],
            ],
        ];

        $this->testManifestAndConfig(
            $config,
            $expectedArray,
        );
    }

    public function testFullConfig(): void
    {
        $config = [
            'destination' => 'in.c-main.test',
            'table_metadata' => [
                'KBC.table_metadata' => 'test_metadata',
            ],
            'schema' => [
                [
                    'name' => 'test',
                    'data_type' => [
                        'base' => [
                            'type' => 'string',
                        ],
                        'snowflake' => [
                            'type' => 'string',
                        ],
                        'bigquery' => [
                            'type' => 'string',
                        ],
                    ],
                    'nullable' => false,
                    'primary_key' => true,
                    'distribution_key' => false,
                    'description' => 'test',
                    'metadata' => [
                        'KBC.metadata' => 'test_metadata',
                    ],
                ],
            ],
            'incremental' => true,
            'delete_where_column' => 'column',
            'delete_where_values' => [
                'test',
            ],
            'delete_where_operator' => 'eq',
            'delimiter' => 'xyz',
            'enclosure' => 'abc',
            'description' => 'descTest',
        ];

        $expectedArray = [
            'destination' => 'in.c-main.test','primary_key' => [],
            'distribution_key' => [],
            'columns' => [],
            'incremental' => true,
            'delete_where_values' => [
                'test',
            ],
            'delete_where_operator' => 'eq',
            'delimiter' => 'xyz',
            'enclosure' => 'abc',
            'metadata' => [],
            'column_metadata' => [],
            'write_always' => false,
            'tags' => [],
            'table_metadata' => [
                'KBC.table_metadata' => 'test_metadata',
            ],
            'schema' => [
                [
                    'name' => 'test',
                    'data_type' => [
                        'base' => [
                            'type' => 'string',
                        ],
                        'snowflake' => [
                            'type' => 'string',
                        ],
                        'bigquery' => [
                            'type' => 'string',
                        ],
                    ],
                    'nullable' => false,
                    'primary_key' => true,
                    'distribution_key' => false,
                    'description' => 'test',
                    'metadata' => [
                        'KBC.metadata' => 'test_metadata',
                    ],
                ],
            ],
            'delete_where_column' => 'column',
            'description' => 'descTest',
        ];

        $this->testManifestAndConfig(
            $config,
            $expectedArray,
        );
    }

    public function testErrorSchemaAndMetadataSetTogether(): void
    {
        $config = [
            'destination' => 'in.c-main.test',
            'schema' => [
                [
                    'name' => 'test',
                    'data_type' => [
                        'base' => [
                            'type' => 'string',
                        ],
                    ],
                ],
            ],
            'metadata' => [
                [
                    'key' => 'test',
                    'value' => 'test',
                ],
            ],
        ];

        $this->testManifestAndConfig(
            $config,
            [],
            'Invalid configuration for path "table": ' .
            'Only one of "schema" or "metadata" can be defined.',
        );
    }

    public function testErrorSchemaAndColumnsSetTogether(): void
    {
        $config = [
            'destination' => 'in.c-main.test',
            'schema' => [
                [
                    'name' => 'test',
                    'data_type' => [
                        'base' => [
                            'type' => 'string',
                        ],
                    ],
                ],
            ],
            'columns' => [
                'test',
            ],
        ];

        $this->testManifestAndConfig(
            $config,
            [],
            'Invalid configuration for path "table": ' .
            'Only one of "schema" or "columns" can be defined.',
        );
    }

    public function testErrorSchemaAndColumnMetadataSetTogether(): void
    {
        $config = [
            'destination' => 'in.c-main.test',
            'schema' => [
                [
                    'name' => 'test',
                    'data_type' => [
                        'base' => [
                            'type' => 'string',
                        ],
                    ],
                ],
            ],
            'column_metadata' => [
                [
                    'name' => 'test',
                    'metadata' => [
                        'key' => 'test',
                        'value' => 'test',
                    ],
                ],
            ],
        ];

        $this->testManifestAndConfig(
            $config,
            [],
            'Invalid configuration for path "table": ' .
            'Only one of "schema" or "column_metadata" can be defined.',
        );
    }

    public function testErrorDescriptionAndTableMetadataDescriptionSetTogether(): void
    {
        $config = [
            'destination' => 'in.c-main.test',
            'description' => 'test',
            'table_metadata' => [
                'KBC.description' => 'description',
            ],
        ];

        $this->testManifestAndConfig(
            $config,
            [],
            'Invalid configuration for path "table": ' .
            'Only one of "description" or "table_metadata.KBC.description" can be defined.',
        );
    }

    public function testErrorPKAndSchemaPKSetTogether(): void
    {
        $config = [
            'destination' => 'in.c-main.test',
            'primary_key' => ['test'],
            'schema' => [
                [
                    'name' => 'test',
                    'data_type' => [
                        'base' => [
                            'type' => 'string',
                        ],
                    ],
                    'primary_key' => true,
                ],
            ],
        ];

        $this->testManifestAndConfig(
            $config,
            [],
            'Only one of "primary_key" or "schema[].primary_key" can be defined.',
        );
    }

    public function testErrorDistributionKeyAndSchemaDistributionKeySetTogether(): void
    {
        $config = [
            'destination' => 'in.c-main.test',
            'distribution_key' => ['test'],
            'schema' => [
                [
                    'name' => 'test',
                    'data_type' => [
                        'base' => [
                            'type' => 'string',
                        ],
                    ],
                    'distribution_key' => true,
                ],
            ],
        ];

        $this->testManifestAndConfig(
            $config,
            [],
            'Only one of "distribution_key" or "schema[].distribution_key" can be defined.',
        );
    }

    public function testErrorSchemaDescriptionAndSchemaMetadataDescriptionSetTogether(): void
    {
        $config = [
            'destination' => 'in.c-main.test',
            'schema' => [
                [
                    'name' => 'test',
                    'data_type' => [
                        'base' => [
                            'type' => 'string',
                        ],
                    ],
                    'description' => 'test',
                    'metadata' => [
                        'KBC.description' => 'test',
                    ],
                ],
            ],
        ];

        $this->testManifestAndConfig(
            $config,
            [],
            'Invalid configuration for path "table.schema.0": ' .
            'Only one of "description" or "metadata.KBC.description" can be defined.',
        );
    }

    public function testErrorSchemaDataTypeBaseTypeNotSet(): void
    {
        $config = [
            'destination' => 'in.c-main.test',
            'schema' => [
                [
                    'name' => 'test',
                    'data_type' => [
                        'base' => [],
                    ],
                ],
            ],
        ];

        $this->testManifestAndConfig(
            $config,
            [],
            'The child config "type" under "table.schema.0.data_type.base" must be configured.',
        );
    }

    public function testErrorSchemaDataTypeOptionalTypeNotSet(): void
    {
        $config = [
            'destination' => 'in.c-main.test',
            'schema' => [
                [
                    'name' => 'test',
                    'data_type' => [
                        'base' => [
                            'type' => 'string',
                        ],
                        'snowflake' => [
                        ],
                    ],
                ],
            ],
        ];

        $this->testManifestAndConfig(
            $config,
            [],
            'The "type" is required for the "snowflake" data type.',
        );
    }

    public function testErrorSchemaDataTypeSetUnknownType(): void
    {
        $config = [
            'destination' => 'in.c-main.test',
            'schema' => [
                [
                    'name' => 'test',
                    'data_type' => [
                        'base' => [
                            'type' => 'string',
                        ],
                        'unknown' => [
                            'type' => 'string',
                        ],
                    ],
                ],
            ],
        ];

        $this->testManifestAndConfig(
            $config,
            [],
            'The "unknown" data type is not supported.',
        );
    }

    public function testTurnOffNormalizeColumnMetadataKeys(): void
    {
        $config = [
            'destination' => 'in.c-main.test',
            'column_metadata' => [
                'phone - client' => [
                    [
                        'key' => 'testKey',
                        'value' => 'testValue',
                    ],
                ],
            ],
            'incremental' => false,
            'primary_key' => [],
            'columns' => [
                'phone - client',
            ],
            'distribution_key' => [],
            'delete_where_values' => [],
            'delete_where_operator' => 'eq',
            'delimiter' => ',',
            'enclosure' => '"',
            'metadata' => [],
            'write_always' => false,
            'tags' => [],
            'schema' => [],
        ];

        $this->testManifestAndConfig(
            $config,
            $config,
        );
    }

    public function provideDeleteWhereConfigurations(): Generator
    {
        yield 'only changed_since' => [
            'deleteWhere' => [
                [
                    'changed_since' => '-7 days',
                ],
            ],
            'expectedDeleteWhereConfig' => [
                [
                    'changed_since' => '-7 days',
                ],
            ],
        ];

        yield 'only changed_until' => [
            'deleteWhere' => [
                [
                    'changed_until' => '2024-03-20 10:00:00',
                ],
            ],
            'expectedDeleteWhereConfig' => [
                [
                    'changed_until' => '2024-03-20 10:00:00',
                ],
            ],
        ];

        yield 'both changed_since and changed_until' => [
            'deleteWhere' => [
                [
                    'changed_since' => '-1 hour',
                    'changed_until' => 'now',
                ],
            ],
            'expectedDeleteWhereConfig' => [
                [
                    'changed_since' => '-1 hour',
                    'changed_until' => 'now',
                ],
            ],
        ];

        yield 'single where_filter with values_from_set' => [
            'deleteWhere' => [
                [
                    'where_filters' => [
                        [
                            'column' => 'status',
                            'values_from_set' => ['pending', 'processing'],
                        ],
                    ],
                ],
            ],
            'expectedDeleteWhereConfig' => [
                [
                    'where_filters' => [
                        [
                            'column' => 'status',
                            'operator' => 'eq',
                            'values_from_set' => ['pending', 'processing'],
                        ],
                    ],
                ],
            ],
        ];

        yield 'single where_filter with values_from_workspace' => [
            'deleteWhere' => [
                [
                    'where_filters' => [
                        [
                            'column' => 'city',
                            'values_from_workspace' => [
                                'workspace_id' => '123',
                                'table' => 'cities',
                                'column' => 'name',
                            ],
                        ],
                    ],
                ],
            ],
            'expectedDeleteWhereConfig' => [
                [
                    'where_filters' => [
                        [
                            'column' => 'city',
                            'operator' => 'eq',
                            'values_from_workspace' => [
                                'workspace_id' => '123',
                                'table' => 'cities',
                                'column' => 'name',
                            ],
                        ],
                    ],
                ],
            ],
        ];

        yield 'single where_filter with values_from_workspace wihout column and workspace_id' => [
            'deleteWhere' => [
                [
                    'where_filters' => [
                        [
                            'column' => 'city',
                            'values_from_workspace' => [
                                'table' => 'cities',
                            ],
                        ],
                    ],
                ],
            ],
            'expectedDeleteWhereConfig' => [
                [
                    'where_filters' => [
                        [
                            'column' => 'city',
                            'operator' => 'eq',
                            'values_from_workspace' => [
                                'table' => 'cities',
                            ],
                        ],
                    ],
                ],
            ],
        ];

        yield 'single where_filter with values_from_storage' => [
            'deleteWhere' => [
                [
                    'where_filters' => [
                        [
                            'column' => 'country',
                            'values_from_storage' => [
                                'bucket_id' => 'in.c-main',
                                'table' => 'countries',
                                'column' => 'name',
                            ],
                        ],
                    ],
                ],
            ],
            'expectedDeleteWhereConfig' => [
                [
                    'where_filters' => [
                        [
                            'column' => 'country',
                            'operator' => 'eq',
                            'values_from_storage' => [
                                'bucket_id' => 'in.c-main',
                                'table' => 'countries',
                                'column' => 'name',
                            ],
                        ],
                    ],
                ],
            ],
        ];

        yield 'multiple where_filters with different operators' => [
            'deleteWhere' => [
                [
                    'where_filters' => [
                        [
                            'column' => 'status',
                            'operator' => 'eq',
                            'values_from_set' => ['active'],
                        ],
                        [
                            'column' => 'type',
                            'operator' => 'ne',
                            'values_from_set' => ['deleted'],
                        ],
                    ],
                ],
            ],
            'expectedDeleteWhereConfig' => [
                [
                    'where_filters' => [
                        [
                            'column' => 'status',
                            'operator' => 'eq',
                            'values_from_set' => ['active'],
                        ],
                        [
                            'column' => 'type',
                            'operator' => 'ne',
                            'values_from_set' => ['deleted'],
                        ],
                    ],
                ],
            ],
        ];

        yield 'combination of all features' => [
            'deleteWhere' => [
                [
                    'changed_since' => '-1 day',
                    'changed_until' => 'now',
                    'where_filters' => [
                        [
                            'column' => 'status',
                            'values_from_set' => ['new'],
                        ],
                        [
                            'column' => 'region',
                            'values_from_workspace' => [
                                'workspace_id' => '456',
                                'table' => 'regions',
                            ],
                            'values_from_set' => [],
                        ],
                        [
                            'column' => 'category',
                            'operator' => 'ne',
                            'values_from_storage' => [
                                'bucket_id' => 'in.c-main',
                                'table' => 'categories',
                                'column' => 'name',
                            ],
                        ],
                    ],
                ],
            ],
            'expectedDeleteWhereConfig' => [
                [
                    'changed_since' => '-1 day',
                    'changed_until' => 'now',
                    'where_filters' => [
                        [
                            'column' => 'status',
                            'operator' => 'eq',
                            'values_from_set' => ['new'],
                        ],
                        [
                            'column' => 'region',
                            'operator' => 'eq',
                            'values_from_workspace' => [
                                'workspace_id' => '456',
                                'table' => 'regions',
                            ],
                        ],
                        [
                            'column' => 'category',
                            'operator' => 'ne',
                            'values_from_storage' => [
                                'bucket_id' => 'in.c-main',
                                'table' => 'categories',
                                'column' => 'name',
                            ],
                        ],
                    ],
                ],
            ],
        ];
    }

    /**
     * @dataProvider provideDeleteWhereConfigurations
     */
    public function testDeleteWhereConfigurations(array $deleteWhere, array $expectedDeleteWhereConfig): void
    {
        $config = [
            'destination' => 'in.c-main.test',
            'delete_where' => $deleteWhere,
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
            'delete_where' => $expectedDeleteWhereConfig,
        ];

        $this->testManifestAndConfig($config, $expectedArray);
    }

    public function provideInvalidDeleteWhereConfigurations(): Generator
    {
        yield 'empty delete_where item' => [
            'deleteWhere' => [
                [
                    // empty item
                ],
            ],
            'expectedError' => 'Invalid configuration for path "table.delete_where.0": '
                . 'At least one of "changed_since", "changed_until", or "where_filters" must be defined.',
        ];

        yield 'missing column in where_filters' => [
            'deleteWhere' => [
                [
                    'where_filters' => [
                        [
                            'values_from_set' => ['value'],
                        ],
                    ],
                ],
            ],
            'expectedError' => 'The child config "column" under "table.delete_where.0.where_filters.0" '
                . 'must be configured.',
        ];

        yield 'empty column in where_filters' => [
            'deleteWhere' => [
                [
                    'where_filters' => [
                        [
                            'column' => '',
                            'values_from_set' => ['value'],
                        ],
                    ],
                ],
            ],
            'expectedError' => 'The path "table.delete_where.0.where_filters.0.column" '
                . 'cannot contain an empty value, but got "".',
        ];

        yield 'invalid operator in where_filters' => [
            'deleteWhere' => [
                [
                    'where_filters' => [
                        [
                            'column' => 'status',
                            'operator' => 'invalid',
                            'values_from_set' => ['value'],
                        ],
                    ],
                ],
            ],
            'expectedError' => 'Invalid configuration for path "table.delete_where.0.where_filters.0.operator": '
                . 'Invalid operator "invalid". Valid values are "eq" or "ne".',
        ];

        yield 'multiple value sources in where_filters' => [
            'deleteWhere' => [
                [
                    'where_filters' => [
                        [
                            'column' => 'status',
                            'values_from_set' => ['value'],
                            'values_from_workspace' => [
                                'workspace_id' => '123',
                                'table' => 'table',
                            ],
                        ],
                    ],
                ],
            ],
            'expectedError' => 'Invalid configuration for path "table.delete_where.0.where_filters.0": '
                . 'Only one of "values_from_set", "values_from_workspace", or "values_from_storage" can be defined.',
        ];

        yield 'missing required fields in values_from_workspace' => [
            'deleteWhere' => [
                [
                    'where_filters' => [
                        [
                            'column' => 'status',
                            'values_from_workspace' => [
                                'workspace_id' => '123',
                                // missing required 'table'
                            ],
                        ],
                    ],
                ],
            ],
            'expectedError' => 'The child config "table" under '
                . '"table.delete_where.0.where_filters.0.values_from_workspace" must be configured.',
        ];

        yield 'missing required fields in values_from_storage' => [
            'deleteWhere' => [
                [
                    'where_filters' => [
                        [
                            'column' => 'status',
                            'values_from_storage' => [
                                'bucket_id' => 'in.c-main',
                                // missing required 'table'
                            ],
                        ],
                    ],
                ],
            ],
            'expectedError' => 'The child config "table" under '
                . '"table.delete_where.0.where_filters.0.values_from_storage" must be configured.',
        ];

        yield 'no value source in where_filters' => [
            'deleteWhere' => [
                [
                    'where_filters' => [
                        [
                            'column' => 'status',
                            // no value source specified
                        ],
                    ],
                ],
            ],
            'expectedError' => 'Invalid configuration for path "table.delete_where.0.where_filters.0": '
                . 'One of "values_from_set", "values_from_workspace", or "values_from_storage" must be defined.',
        ];
    }

    /**
     * @dataProvider provideInvalidDeleteWhereConfigurations
     */
    public function testInvalidDeleteWhereConfigurations(array $deleteWhere, string $expectedError): void
    {
        $config = [
            'destination' => 'in.c-main.test',
            'delete_where' => $deleteWhere,
        ];

        $this->testManifestAndConfig($config, [], $expectedError);
    }

    public function testErrorDeleteWhereAndDeleteWhereColumnSetTogether(): void
    {
        $config = [
            'destination' => 'in.c-main.test',
            'delete_where_column' => 'city',
            'delete_where_values' => ['Prague'],
            'delete_where' => [
                [
                    'changed_since' => '-7 days',
                ],
            ],
        ];

        $this->testManifestAndConfig(
            $config,
            [],
            'Invalid configuration for path "table": Only one of "delete_where_column" '
                . 'or "delete_where" can be defined.',
        );
    }

    /**
     * @dataProvider deduplicationStrategiesProvider
     */
    public function testDeduplicationStrategySuccess(string $input, string $expected): void
    {
        $config = [
            'deduplication_strategy' => $input,
        ];

        $manifest = (new Table\Manifest())->parse(['config' => $config]);
        $this->assertArrayHasKey('deduplication_strategy', $manifest);
        $this->assertSame($expected, $manifest['deduplication_strategy']);

        $configuration = (new Table\Configuration())->parse(['config' => $config]);
        $this->assertArrayHasKey('deduplication_strategy', $configuration);
        $this->assertSame($expected, $configuration['deduplication_strategy']);
    }

    public function deduplicationStrategiesProvider(): Generator
    {
        yield ['insert', Table\DeduplicationStrategy::INSERT->value];
        yield ['upsert', Table\DeduplicationStrategy::UPSERT->value];
    }

    public function testDeduplicationStrategyFail(): void
    {
        $config = [
            'deduplication_strategy' => 'explode',
        ];

        try {
            (new Table\Manifest())->parse(['config' => $config]);
            $this->fail('Exception should be thrown');
        } catch (InvalidConfigurationException $e) {
            self::assertEquals(
                'The value "explode" is not allowed for path "table.deduplication_strategy". Permissible values: "insert", "upsert".', // phpcs:ignore
                $e->getMessage(),
            );
        }

        try {
            (new Table\Configuration())->parse(['config' => $config]);
            $this->fail('Exception should be thrown');
        } catch (InvalidConfigurationException $e) {
            self::assertEquals(
                'The value "explode" is not allowed for path "table.deduplication_strategy". Permissible values: "insert", "upsert".', // phpcs:ignore
                $e->getMessage(),
            );
        }
    }

    public function testUnloadStrategyDirectGrant(): void
    {
        $config = [
            'destination' => 'in.c-main.test',
            'unload_strategy' => 'direct-grant',
        ];

        $expectedArray = [
            'destination' => 'in.c-main.test',
            'unload_strategy' => 'direct-grant',
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

        $this->testManifestAndConfig($config, $expectedArray);
    }

    public function testUnloadStrategyInvalidValue(): void
    {
        $config = [
            'destination' => 'in.c-main.test',
            'unload_strategy' => 'invalid-value',
        ];

        $this->testManifestAndConfig(
            $config,
            [],
            'The value "invalid-value" is not allowed for path "table.unload_strategy". Permissible values: "direct-grant".',
        );
    }

    public function testUnloadStrategyNotSet(): void
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

        $this->testManifestAndConfig($config, $expectedArray);
    }

    private function testManifestAndConfig(
        array $config,
        array $expectedConfig,
        ?string $expectedErrorMessage = null,
    ): void {
        if ($expectedErrorMessage !== null) {
            try {
                (new Table\Manifest())->parse(['config' => $config]);
                self::fail('Exception should be thrown');
            } catch (InvalidConfigurationException $e) {
                self::assertEquals($expectedErrorMessage, $e->getMessage());
            }
            try {
                (new Table\Configuration())->parse(['config' => $config]);
                self::fail('Exception should be thrown');
            } catch (InvalidConfigurationException $e) {
                self::assertEquals($expectedErrorMessage, $e->getMessage());
            }
        } else {
            self::assertEquals($expectedConfig, (new Table\Manifest())->parse(['config' => $config]));
            self::assertEquals($expectedConfig, (new Table\Configuration())->parse(['config' => $config]));
        }
    }
}
