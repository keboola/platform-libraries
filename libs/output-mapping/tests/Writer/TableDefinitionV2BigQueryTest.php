<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer;

use Generator;
use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyBigqueryOutputBucket;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Util\Test;

class TableDefinitionV2BigQueryTest extends AbstractTestCase
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
     * @dataProvider configProvider
     */
    #[NeedsEmptyBigqueryOutputBucket]
    public function testWriterCreateTableDefinition(
        array $configTemplate,
        array $expectedTypes,
    ): void {
        array_walk_recursive($configTemplate, function (&$value) {
            $value = is_string($value) ? sprintf($value, $this->emptyBigqueryOutputBucketId) : $value;
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

    #[NeedsEmptyBigqueryOutputBucket]
    public function testWriteTableOutputMappingExistingTable(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/tableDefinition.csv',
            <<< EOT
            "1","bob","10.11","2021-12-12 16:45:21"
            "2","alice","5.63","2020-12-12 15:45:21"
            EOT,
        );

        $configs = [
            [
                'source' => 'tableDefinition.csv',
                'destination' => $this->emptyBigqueryOutputBucketId . '.tableDefinitionBackendType',
                'schema' => [
                    [
                        'name' => 'Id',
                        'data_type' => [
                            'base' => [
                                'type' => BaseType::NUMERIC,
                            ],
                            'bigquery' => [
                                'type' => Bigquery::TYPE_INT64,
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
                            'bigquery' => [
                                'type' => Bigquery::TYPE_STRING,
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
                            'bigquery' => [
                                'type' => Bigquery::TYPE_DECIMAL,
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
                            'bigquery' => [
                                'type' => Bigquery::TYPE_TIMESTAMP,
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $writer = new TableWriter($this->getLocalStagingFactory());
        $tableQueue =  $writer->uploadTables(
            '/upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'local',
            false,
            'authoritative',
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        // And again
        $tableQueue =  $writer->uploadTables(
            '/upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'local',
            false,
            'authoritative',
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyBigqueryOutputBucketId);
        self::assertCount(1, $tables);
        self::assertEquals($this->emptyBigqueryOutputBucketId . '.tableDefinitionBackendType', $tables[0]['id']);
        self::assertNotEmpty($jobIds[0]);
    }

    public function configProvider(): Generator
    {
        yield 'base types' => [
            [
                'source' => 'tableDefinition.csv',
                'destination' => '%s.tableDefinitionBaseType',
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
                    'type' => Bigquery::TYPE_NUMERIC,
                    'nullable' => false,
                ],
                'Name' => [
                    'type' => Bigquery::TYPE_STRING,
                    'nullable' => false,
                ],
                'birthweight' => [
                    'type' => Bigquery::TYPE_NUMERIC,
                    'length' => '38,9',
                    'nullable' => true,
                ],
                'created' => [
                    'type' => Bigquery::TYPE_TIMESTAMP,
                    'nullable' => true,
                ],
            ],
        ];

        yield 'native bigquery types' => [
            [
                'source' => 'tableDefinition.csv',
                'destination' => '%s.tableDefinitionBackendType',
                'schema' => [
                    [
                        'name' => 'Id',
                        'data_type' => [
                            'base' => [
                                'type' => BaseType::NUMERIC,
                            ],
                            'bigquery' => [
                                'type' => Bigquery::TYPE_INTEGER,
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
                            'bigquery' => [
                                'type' => Bigquery::TYPE_STRING,
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
                            'bigquery' => [
                                'type' => Bigquery::TYPE_DECIMAL,
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
                            'bigquery' => [
                                'type' => Bigquery::TYPE_TIMESTAMP,
                            ],
                        ],
                    ],
                ],
            ],
            [
                'Id' => [
                    'type' => Bigquery::TYPE_INTEGER,
                    'nullable' => false,
                ],
                'Name' => [
                    'type' => Bigquery::TYPE_STRING,
                    'length' => '17',
                    'nullable' => false,
                ],
                'birthweight' => [
                    'type' => Bigquery::TYPE_NUMERIC,
                    'length' => '10,2',
                    'nullable' => true,
                ],
                // this one stays the same because it is not an alias
                'created' => [
                    'type' => Bigquery::TYPE_TIMESTAMP,
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
        self::assertSame($expectedType['nullable'], $definitionColumn['definition']['nullable']);
        if (!isset($expectedType['length'])) {
            self::assertArrayNotHasKey('length', $definitionColumn['definition']);
        } else {
            self::assertSame($expectedType['length'], $definitionColumn['definition']['length']);
        }
    }

    protected function initClient(?string $branchId = null): void
    {
        $clientOptions = (new ClientOptions())
            ->setUrl((string) getenv('BIGQUERY_STORAGE_API_URL'))
            ->setToken((string) getenv('BIGQUERY_STORAGE_API_TOKEN'))
            ->setBranchId($branchId)
            ->setBackoffMaxTries(1)
            ->setJobPollRetryDelay(function () {
                return 1;
            })
            ->setUserAgent(implode('::', Test::describe($this)));
        $this->clientWrapper = new ClientWrapper($clientOptions);
        $tokenInfo = $this->clientWrapper->getBranchClient()->verifyToken();
        print(sprintf(
            'Authorized as "%s (%s)" to project "%s (%s)" at "%s" stack.',
            $tokenInfo['description'],
            $tokenInfo['id'],
            $tokenInfo['owner']['name'],
            $tokenInfo['owner']['id'],
            $this->clientWrapper->getBranchClient()->getApiUrl(),
        ));
    }
}
