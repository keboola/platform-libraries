<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Configuration\Table;

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
                        'redshift' => [
                            'type' => 'string',
                        ],
                        'snowflake' => [
                            'type' => 'string',
                        ],
                        'synapse' => [
                            'type' => 'string',
                        ],
                        'bigquery' => [
                            'type' => 'string',
                        ],
                        'exasol' => [
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
                        'redshift' => [
                            'type' => 'string',
                        ],
                        'snowflake' => [
                            'type' => 'string',
                        ],
                        'synapse' => [
                            'type' => 'string',
                        ],
                        'bigquery' => [
                            'type' => 'string',
                        ],
                        'exasol' => [
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
