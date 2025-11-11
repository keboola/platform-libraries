<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\OutputMappingSettings;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyInputBucket;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\OutputMapping\Tests\Needs\NeedsTestTables;
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

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage($expectedErrorMessage);

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => [array_merge($baseConfig, $config)]],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
        $tableQueue->waitForAll();
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
                            'type' => 'INTEGER',
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

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => [$config]],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => [$config]],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'authoritative',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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

    #[NeedsEmptyOutputBucket]
    public function testAddMissingColumnTableDefinition(): void
    {
        $this->clientWrapper->getTableAndFileStorageClient()->createTableDefinition($this->emptyOutputBucketId, [
            'name' => 'tableDefinition',
            'primaryKeysNames' => [],
            'columns' => [
                [
                    'name' => 'Id',
                    'basetype' => BaseType::NUMERIC,
                ],
                [
                    'name' => 'Name',
                    'basetype' => BaseType::STRING,
                ],
            ],
        ]);

        $config = [
            'source' => 'tableDefinition.csv',
            'destination' => $this->emptyOutputBucketId . '.tableDefinition',
            'schema' => [
                [
                    'name' => 'Id',
                    'data_type' => [
                        'base' => [
                            'type' => BaseType::NUMERIC,
                        ],
                    ],
                ],
                [
                    'name' => 'Name',
                    'data_type' => [
                        'base' => [
                            'type' => BaseType::STRING,
                        ],
                    ],
                ],
                [
                    'name' => 'newColumn',
                    'data_type' => [
                        'base' => [
                            'type' => BaseType::STRING,
                        ],
                    ],
                ],
            ],
        ];

        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/tableDefinition.csv',
            <<< EOT
            "1","bob","abcdef"
            EOT,
        );

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => [$config]],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'authoritative',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);
        $tableDetails = $this->clientWrapper->getTableAndFileStorageClient()->getTable($config['destination']);
        self::assertTrue($tableDetails['isTyped']);

        self::assertDataTypeDefinition(
            array_filter($tableDetails['definition']['columns'], fn($v) => $v['name'] === 'newColumn'),
            [
                'type' => Snowflake::TYPE_VARCHAR,
                'length' => '16777216', // default varchar length
                'nullable' => true,
            ],
        );
    }

    public function configProvider(): iterable
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

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => [$config]],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => [$config]],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: OutputMappingSettings::DATA_TYPES_SUPPORT_HINTS,
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => [$config]],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: OutputMappingSettings::DATA_TYPES_SUPPORT_HINTS,
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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

    #[NeedsEmptyOutputBucket]
    public function testDropTypedTableFailedImportData(): void
    {
        $tableId = $this->emptyOutputBucketId . '.table';
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

        $tableQueue = $this->getTableLoader(
            logger: $this->testLogger,
            strategyFactory: $this->getLocalStagingFactory(
                logger: $this->testLogger,
            ),
        )->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => [$config]],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: OutputMappingSettings::DATA_TYPES_SUPPORT_HINTS,
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );

        try {
            $tableQueue->waitForAll();
            $this->fail('Job should fail');
        } catch (InvalidOutputException $e) {
            self::assertStringContainsString('Failed to load table', $e->getMessage());
        }

        self::assertTrue(
            $this->testHandler->hasWarning(sprintf('Failed to load table "%s". Dropping table.', $tableId)),
        );

        self::assertFalse($this->clientWrapper->getBasicClient()->tableExists($tableId));
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingWithPkUpdateAndLegacyManifest(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/table14.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [
                    'mapping' => [
                        [
                            'source' => 'table14.csv',
                            'destination' => $this->emptyOutputBucketId . '.table14',
                            'primary_key' => ['Id', 'Name'],
                        ],
                    ],
                ],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $tableInfo = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId . '.table14',
        );
        $this->assertEquals(['Id', 'Name'], $tableInfo['primaryKey']);

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [
                    'mapping' => [
                        [
                            'source' => 'table14.csv',
                            'destination' => $this->emptyOutputBucketId . '.table14',
                            'primary_key' => ['Id'],
                        ],
                    ],
                ],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );

        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $tableInfo = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId . '.table14',
        );
        $this->assertEquals(['Id'], $tableInfo['primaryKey']);
    }

    #[NeedsEmptyOutputBucket]
    public function testAuthoritativeManifestAndLegacyPrimaryKeysInConfiguration(): void
    {
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
                ],
            ]),
        );

        $baseConfig = [
            'source' => 'tableDefinition.csv',
            'destination' => $this->emptyOutputBucketId . '.tableDefinition',
            'primary_key' => ['Id'],
        ];

        $tableQueue = $this->getTableLoader(
            logger: $this->testLogger,
            strategyFactory: $this->getLocalStagingFactory(
                logger: $this->testLogger,
            ),
        )->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => [$baseConfig]],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );

        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $tableInfo = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId . '.tableDefinition',
        );
        $this->assertEquals(['Id'], $tableInfo['primaryKey']);
    }

    #[NeedsEmptyOutputBucket]
    public function testLongColumnName(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/test1.csv',
            'newName 1',
        );

        file_put_contents(
            $root . DIRECTORY_SEPARATOR . 'upload/test1.csv.manifest',
            json_encode([
                'destination' => $this->emptyOutputBucketId . '.test1',
                'primary_key' => [],
                'columns' => ['LongLongLongLongLongLongLongLongLongLongLongLongLongLongLongLongName'],
                'incremental' => true,
            ]),
        );

        try {
            $tableQueue = $this->getTableLoader()->uploadTables(
                configuration: new OutputMappingSettings(
                    configuration: [],
                    sourcePathPrefix: 'upload',
                    storageApiToken: $this->clientWrapper->getToken(),
                    isFailedJob: false,
                    dataTypeSupport: 'none',
                ),
                systemMetadata: new SystemMetadata(['componentId' => 'foo']),
            );
            $tableQueue->waitForAll();
            $this->fail('Must throw exception');
        } catch (InvalidOutputException $e) {
            $this->assertStringContainsString(
                '\'LongLongLongLongLongLongLongLongLongLongLongLongLongLongLongLongName\' '.
                'is more than 64 characters long',
                $e->getMessage(),
            );
        }
    }

    public function conflictsConfigurationWithManifestProvider(): iterable
    {
        yield 'conflict-columns' => [
            [
                'columns' => ['Id', 'Name'],
            ],
            'Only one of "schema" or "columns" can be defined.',
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

    #[NeedsEmptyOutputBucket]
    public function testDirectGrantUnloadStrategySkipsTableUpload(): void
    {
        $this->initWorkspace();
        $tableId = $this->emptyOutputBucketId . '.tableDefinition';

        $config = [
            'destination' => $tableId,
            'unload_strategy' => 'direct-grant',
        ];

        $strategyFactory = $this->getWorkspaceStagingFactory(
            clientWrapper: $this->clientWrapper,
            logger: $this->testLogger,
        );

        $tableLoader = $this->getTableLoader(
            clientWrapper: $this->clientWrapper,
            logger: $this->testLogger,
            strategyFactory: $strategyFactory,
        );

        $tableQueue = $tableLoader->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => [$config]],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(0, $jobIds, 'No jobs should be created when unload_strategy is direct-grant');

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(0, $tables, 'No tables should be imported when unload_strategy is direct-grant');
    }
}
