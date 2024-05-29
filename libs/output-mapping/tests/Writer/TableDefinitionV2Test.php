<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer;

use Generator;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyInputBucket;
use Keboola\OutputMapping\Tests\Needs\NeedsTestTables;
use Keboola\OutputMapping\Writer\TableWriter;

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
            "Id","Name"
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
}
