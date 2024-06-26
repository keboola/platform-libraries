<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer;

use Generator;
use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Common;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\OutputMappingSettings;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyInputBucket;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\OutputMapping\Tests\Needs\NeedsTestTables;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\Metadata;

class TableDefinitionV2Test extends AbstractTestCase
{
    public function setUp(): void
    {
        parent::setUp();

        $requiredFeatures = [
            'new-native-types',
        ];

        $tokenData = $this->clientWrapper->getBranchClient()->verifyToken();
        foreach ($requiredFeatures as $requiredFeature) {
            if (!in_array($requiredFeature, $tokenData['owner']['features'])) {
                self::fail(sprintf(
                    '%s is not enabled for project "%s".',
                    ucfirst(str_replace('-', ' ', $requiredFeature)),
                    $tokenData['owner']['id'],
                ));
            }
        }
    }

    /**
     * @dataProvider conflictsConfigurationWithManifestProvider
     */
    public function testConflictSchemaManifestAndNonSchemaConfiguration(
        array $config,
        string $expectedErrorMessage,
    ): void {
        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/tableDefinition.csv',
            "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n",
        );

        file_put_contents(
            $root . '/upload/tableDefinition.csv.manifest',
            json_encode([
                'schema' => [
                    [
                        'name' => 'Id',
                        'primary_key' => true,
                        'data_type' => [
                            'base' => [
                                'type' => 'int',
                            ],
                        ],
                    ],
                    [
                        'name' => 'Name',
                        'data_type' => [
                            'base' => [
                                'type' => 'string',
                            ],
                        ],
                    ],
                ],
            ]),
        );

        $baseConfig = [
            'source' => 'tableDefinition.csv',
            'destination' => 'in.c-test.tableDefinition',
        ];

        $writer = new TableWriter($this->getLocalStagingFactory(logger: $this->testLogger));

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage($expectedErrorMessage);
        $writer->uploadTables(
            'upload',
            ['mapping' => [array_merge($baseConfig, $config)]],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
    }

    #[NeedsEmptyInputBucket]
    public function testValidateTypedTableStructure(): void
    {
        $tableId = $this->emptyInputBucketId . '.tableDefinition';
        $idDatatype = new GenericStorage('int', ['nullable' => false]);
        $nameDatatype = new GenericStorage('varchar', ['length' => '17', 'nullable' => false]);

        $this->clientWrapper->getTableAndFileStorageClient()->createTableDefinition($this->emptyInputBucketId, [
            'name' => 'tableDefinition',
            'primaryKeysNames' => [],
            'columns' => [
                [
                    'name' => 'Id',
                    'basetype' => $idDatatype->getBasetype(),
                ],
                [
                    'name' => 'Name',
                    'basetype' => $nameDatatype->getBasetype(),
                ],
            ],
        ]);

        $config = [
            'source' => 'tableDefinition.csv',
            'destination' => $tableId,
            'schema' => [
                [
                    'name' => 'Id',
                    'data_type' => [
                        'base' => [
                            'type' => 'NUMERIC',
                        ],
                    ],
                ],
                [
                    'name' => 'Name',
                    'data_type' => [
                        'base' => [
                            'type' => 'STRING',
                        ],
                        'snowflake' => [
                            'type' => 'VARCHAR',
                        ],
                    ],
                ],
            ],
        ];

        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/tableDefinition.csv',
            <<< EOT
            "1","bob"
            "2","alice"
            EOT,
        );

        $writer = new TableWriter($this->getLocalStagingFactory());

        $tableQueue = $writer->uploadTables(
            'upload',
            [
                'mapping' => [$config],
            ],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );

        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);
    }

    /**
     * @dataProvider configProvider
     */
    #[NeedsEmptyOutputBucket]
    public function testWriterCreateTableDefinition(
        array $configTemplate,
        array $expectedTypes,
    ): void {
        array_walk_recursive($configTemplate, function (&$value) {
            $value = is_string($value) ? sprintf($value, $this->emptyOutputBucketId) : $value;
        });
        $config = $configTemplate;

        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/tableDefinition.csv',
            <<< EOT
            "1","bob","10.11","2021-12-12 16:45:21"
            "2","alice","5.63","2020-12-12 15:45:21"
            EOT,
        );
        $writer = new TableWriter($this->getLocalStagingFactory());

        $tableQueue =  $writer->uploadTables(
            'upload',
            [
                'mapping' => [$config],
            ],
            ['componentId' => 'foo'],
            'local',
            false,
            'authoritative',
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);
        $tableDetails = $this->clientWrapper->getTableAndFileStorageClient()->getTable($config['destination']);
        self::assertTrue($tableDetails['isTyped']);

        self::assertDataTypeDefinition(
            array_filter($tableDetails['definition']['columns'], fn($v) => $v['name'] === 'Id'),
            $expectedTypes['Id'],
        );
        self::assertDataTypeDefinition(
            array_filter($tableDetails['definition']['columns'], fn($v) => $v['name'] === 'Name'),
            $expectedTypes['Name'],
        );
        self::assertDataTypeDefinition(
            array_filter($tableDetails['definition']['columns'], fn($v) => $v['name'] === 'birthweight'),
            $expectedTypes['birthweight'],
        );
        self::assertDataTypeDefinition(
            array_filter($tableDetails['definition']['columns'], fn($v) => $v['name'] === 'created'),
            $expectedTypes['created'],
        );
    }

    public function configProvider(): Generator
    {
        yield 'base types' => [
            [
                'source' => 'tableDefinition.csv',
                'destination' => '%s.tableDefinition',
                'schema' => [
                    [
                        'name' => 'Id',
                        'data_type' => [
                            'base' => [
                                'type' => BaseType::NUMERIC,
                            ],
                        ],
                        'nullable' => false,
                        'primary_key' => true,
                    ],
                    [
                        'name' => 'Name',
                        'data_type' => [
                            'base' => [
                                'type' => BaseType::STRING,
                            ],
                        ],
                        'nullable' => false,
                        'primary_key' => true,
                    ],
                    [
                        'name' => 'birthweight',
                        'data_type' => [
                            'base' => [
                                'type' => BaseType::NUMERIC,
                                'length' => '38,9',
                            ],
                        ],
                    ],
                    [
                        'name' => 'created',
                        'data_type' => [
                            'base' => [
                                'type' => BaseType::TIMESTAMP,
                            ],
                        ],
                    ],
                ],
            ],
            [
                'Id' => [
                    'type' => Snowflake::TYPE_NUMBER,
                    'length' => '38,0', // default integer length
                    'nullable' => false,
                ],
                'Name' => [
                    'type' => Snowflake::TYPE_VARCHAR,
                    'length' => '16777216', // default varchar length
                    'nullable' => false,
                ],
                'birthweight' => [
                    'type' => Snowflake::TYPE_NUMBER,
                    'length' => '38,9',
                    'nullable' => true,
                ],
                'created' => [
                    'type' => Snowflake::TYPE_TIMESTAMP_LTZ,
                    'length' => '9',
                    'nullable' => true,
                ],
            ],
        ];

        yield 'native snowflake types' => [
            [
                'source' => 'tableDefinition.csv',
                'destination' => '%s.tableDefinition',
                'schema' => [
                    [
                        'name' => 'Id',
                        'data_type' => [
                            'base' => [
                                'type' => BaseType::NUMERIC,
                            ],
                            'snowflake' => [
                                'type' => Snowflake::TYPE_INTEGER,
                            ],
                        ],
                        'nullable' => false,
                        'primary_key' => true,
                    ],
                    [
                        'name' => 'Name',
                        'data_type' => [
                            'base' => [
                                'type' => BaseType::STRING,
                            ],
                            'snowflake' => [
                                'type' => Snowflake::TYPE_TEXT,
                                'length' => '17',
                            ],
                        ],
                        'nullable' => false,
                        'primary_key' => true,
                    ],
                    [
                        'name' => 'birthweight',
                        'data_type' => [
                            'base' => [
                                'type' => BaseType::NUMERIC,
                            ],
                            'snowflake' => [
                                'type' => Snowflake::TYPE_DECIMAL,
                                'length' => '10,2',
                            ],
                        ],
                    ],
                    [
                        'name' => 'created',
                        'data_type' => [
                            'base' => [
                                'type' => BaseType::TIMESTAMP,
                            ],
                            'snowflake' => [
                                'type' => Snowflake::TYPE_TIMESTAMP_TZ,
                            ],
                        ],
                    ],
                ],
            ],
            [
                // INTEGER is an alias of NUMBER in snflk and describe returns the root type
                'Id' => [
                    'type' => Snowflake::TYPE_NUMBER,
                    'length' => '38,0', // default integer length
                    'nullable' => false,
                ],
                // likewise TEXT is an alias of VARCHAR
                'Name' => [
                    'type' => Snowflake::TYPE_VARCHAR,
                    'length' => '17',
                    'nullable' => false,
                ],
                'birthweight' => [
                    'type' => Snowflake::TYPE_NUMBER,
                    'length' => '10,2',
                    'nullable' => true,
                ],
                // this one stays the same because it is not an alias
                'created' => [
                    'type' => Snowflake::TYPE_TIMESTAMP_TZ,
                    'length' => '9', // timestamp_tz has length 9 apparently
                    'nullable' => true,
                ],
            ],
        ];
    }

    private static function assertDataTypeDefinition(array $definitionColumn, array $expectedType): void
    {
        self::assertCount(1, $definitionColumn);
        $definitionColumn = current($definitionColumn);

        self::assertSame($expectedType['type'], $definitionColumn['definition']['type']);
        self::assertSame($expectedType['length'], $definitionColumn['definition']['length']);
        self::assertSame($expectedType['nullable'], $definitionColumn['definition']['nullable']);
    }

    #[NeedsTestTables]
    public function testValidateNonTypedTableStructure(): void
    {
        $tableId = $this->testBucketId . '.test1';

        $config = [
            'source' => 'table.csv',
            'destination' => $tableId,
            'schema' => [
                [
                    'name' => 'Id',
                    'data_type' => [
                        'base' => [
                            'type' => 'STRING',
                        ],
                    ],
                ],
                [
                    'name' => 'Name',
                    'data_type' => [
                        'base' => [
                            'type' => 'STRING',
                        ],
                    ],
                ],
                [
                    'name' => 'foo',
                    'data_type' => [
                        'base' => [
                            'type' => 'STRING',
                        ],
                    ],
                ],
                [
                    'name' => 'bar',
                    'data_type' => [
                        'base' => [
                            'type' => 'STRING',
                        ],
                    ],
                ],
            ],
        ];

        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/table.csv',
            <<< EOT
            "Id","Name","foo","bar"
            "1","bob","firtFoo","firstBar"
            "2","alice","secondFoo","secondBar"
            EOT,
        );

        $writer = new TableWriter($this->getLocalStagingFactory());

        $tableQueue = $writer->uploadTables(
            'upload',
            [
                'mapping' => [$config],
            ],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );

        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);
    }

    #[NeedsEmptyOutputBucket]
    public function testConvertDataTypesToMetadata(): void
    {
        $tableId = $this->emptyOutputBucketId . '.test1';

        $config = [
            'source' => 'table.csv',
            'destination' => $tableId,
            'schema' => [
                [
                    'name' => 'Id',
                    'data_type' => [
                        'base' => [
                            'type' => 'STRING',
                        ],
                    ],
                ],
                [
                    'name' => 'Name',
                    'data_type' => [
                        'base' => [
                            'type' => 'NUMERIC',
                        ],
                    ],
                ],
                [
                    'name' => 'foo',
                    'data_type' => [
                        'base' => [
                            'type' => 'DATE',
                        ],
                    ],
                ],
            ],
        ];

        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/table.csv',
            <<< EOT
            "1","bob","firtFoo"
            "2","alice","secondFoo"
            EOT,
        );

        $writer = new TableWriter($this->getLocalStagingFactory());

        $tableQueue = $writer->uploadTables(
            'upload',
            [
                'mapping' => [$config],
            ],
            ['componentId' => 'foo'],
            'local',
            false,
            OutputMappingSettings::DATA_TYPES_SUPPORT_HINTS,
        );

        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tableDetails = $this->clientWrapper->getTableAndFileStorageClient()->getTable($config['destination']);
        self::assertTrue($tableDetails['isTyped']);

        self::assertDataTypeDefinition(
            array_filter($tableDetails['definition']['columns'], fn($v) => $v['name'] === 'Id'),
            [
                'type' => Snowflake::TYPE_VARCHAR,
                'length' => '16777216',
                'nullable' => true,
            ],
        );
        self::assertDataTypeDefinition(
            array_filter($tableDetails['definition']['columns'], fn($v) => $v['name'] === 'Name'),
            [
                'type' => Snowflake::TYPE_VARCHAR,
                'length' => '16777216',
                'nullable' => true,
            ],
        );
        self::assertDataTypeDefinition(
            array_filter($tableDetails['definition']['columns'], fn($v) => $v['name'] === 'foo'),
            [
                'type' => Snowflake::TYPE_VARCHAR,
                'length' => '16777216',
                'nullable' => true,
            ],
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testSaveTableAndColumnMetadata(): void
    {
        $tableId = $this->emptyOutputBucketId . '.test1';

        $config = [
            'source' => 'table.csv',
            'destination' => $tableId,
            'description' => 'table description',
            'schema' => [
                [
                    'name' => 'Id',
                    'data_type' => [
                        'base' => [
                            'type' => 'STRING',
                        ],
                    ],
                    'metadata' => [
                        'key1' => 'value1',
                        'key2' => 'value2',
                    ],
                ],
                [
                    'name' => 'Name',
                    'data_type' => [
                        'base' => [
                            'type' => 'NUMERIC',
                        ],
                    ],
                    'description' => 'name description',
                ],
                [
                    'name' => 'foo',
                    'data_type' => [
                        'base' => [
                            'type' => 'DATE',
                        ],
                    ],
                    'metadata' => [
                        'key3' => 'value3',
                    ],
                    'description' => 'foo description',
                ],
            ],
        ];

        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/table.csv',
            <<< EOT
            "1","bob","firtFoo"
            "2","alice","secondFoo"
            EOT,
        );

        $writer = new TableWriter($this->getLocalStagingFactory());

        $tableQueue = $writer->uploadTables(
            'upload',
            [
                'mapping' => [$config],
            ],
            ['componentId' => 'foo'],
            'local',
            false,
            OutputMappingSettings::DATA_TYPES_SUPPORT_HINTS,
        );

        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $metadataApi = new Metadata($this->clientWrapper->getTableAndFileStorageClient());

        // Table has only description
        $tableMetadata = $metadataApi->listTableMetadata($tableId);
        $filteredTableMetadata = array_filter(
            $tableMetadata,
            fn($v) => in_array($v['key'], ['KBC.description']),
        );
        self::assertCount(1, $filteredTableMetadata);
        self::assertEquals(
            ['KBC.description' => 'table description'],
            $this->getMetadataValues($filteredTableMetadata),
        );

        // Id column has only metadata
        $columnIdMetadata = $metadataApi->listColumnMetadata($tableId . '.Id');
        $filteredColumnIdMetadata = array_filter(
            $columnIdMetadata,
            fn($v) => in_array($v['key'], ['key1', 'key2']),
        );
        self::assertCount(2, $filteredColumnIdMetadata);
        self::assertEquals(
            [
                'key1' => 'value1',
                'key2' => 'value2',
            ],
            $this->getMetadataValues($filteredColumnIdMetadata),
        );

        // Name column has only description
        $columnNameMetadata = $metadataApi->listColumnMetadata($tableId . '.Name');
        $filteredColumnNameMetadata = array_filter(
            $columnNameMetadata,
            fn($v) => in_array($v['key'], ['KBC.description']),
        );
        self::assertCount(1, $filteredColumnNameMetadata);
        self::assertEquals(
            ['KBC.description' => 'name description'],
            $this->getMetadataValues($filteredColumnNameMetadata),
        );

        // foo column has metadata and description
        $columnFooMetadata = $metadataApi->listColumnMetadata($tableId . '.foo');
        $filteredColumnFooMetadata = array_filter(
            $columnFooMetadata,
            fn($v) => in_array($v['key'], ['key3', 'KBC.description']),
        );
        self::assertCount(2, $filteredColumnFooMetadata);
        self::assertEquals(
            [
                'key3' => 'value3',
                'KBC.description' => 'foo description',
            ],
            $this->getMetadataValues($filteredColumnFooMetadata),
        );
    }

    public function conflictsConfigurationWithManifestProvider(): Generator
    {
        yield 'conflict-columns' => [
            [
                'columns' => ['Id', 'Name'],
            ],
            'Only one of "schema" or "columns" can be defined.',
        ];

        yield 'conflict-primary_keys' => [
            [
                'primary_key' => ['Id', 'Name'],
            ],
            'Only one of "primary_key" or "schema[].primary_key" can be defined.',
        ];

        yield 'conflict-distribution_key' => [
            [
                'distribution_key' => ['Id', 'Name'],
            ],
            'Only one of "distribution_key" or "schema[].distribution_key" can be defined.',
        ];

        yield 'conflict-metadata' => [
            [
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
            ],
            'Only one of "schema" or "metadata" can be defined.',
        ];

        yield 'conflict-column_metadata' => [
            [
                'column_metadata' => [
                    'Id' => [
                        [
                            'key' => 'KBC.dataType',
                            'value' => 'VARCHAR',
                        ],
                    ],
                ],
            ],
            'Only one of "schema" or "column_metadata" can be defined.',
        ];
    }

    protected function getMetadataValues(array $metadata): array
    {
        $result = [];
        foreach ($metadata as $item) {
            $result[$item['key']] = $item['value'];
        }
        return $result;
    }
}
