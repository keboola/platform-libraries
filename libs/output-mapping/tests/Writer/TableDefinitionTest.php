<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer;

use Generator;
use Keboola\Datatype\Definition\Common;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyInputBucket;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\OutputMapping\Writer\TableWriter;

class TableDefinitionTest extends AbstractTestCase
{
    public function setUp(): void
    {
        parent::setUp();

        $requiredFeatures = [
            'native-types',
            'tables-definition',
        ];

        $tokenData = $this->clientWrapper->getBasicClient()->verifyToken();
        foreach ($requiredFeatures as $requiredFeature) {
            if (!in_array($requiredFeature, $tokenData['owner']['features'])) {
                self::fail(sprintf(
                    '%s is not enabled for project "%s".',
                    ucfirst(str_replace('-', ' ', $requiredFeature)),
                    $tokenData['owner']['id']
                ));
            }
        }
    }

    #[NeedsEmptyOutputBucket]
    public function testNotCreateTableDefinition(): void
    {
        $tableId = $this->emptyOutputBucketId . '.tableDefinition';
        $config = [
            'source' => 'tableDefinition.csv',
            'destination' => $tableId,
            'columns' => ['Id', 'Name', 'birthweight', 'created'],
            'column_metadata' => [],
            'primary_key' => ['Id', 'Name'],
        ];

        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/tableDefinition.csv',
            <<< EOT
            "1","bob","10.11","2021-12-12 16:45:21"
            "2","alice","5.63","2020-12-12 15:45:21"
            EOT
        );
        $writer = new TableWriter($this->getLocalStagingFactory());

        $tableQueue =  $writer->uploadTables(
            'upload',
            [
                'typedTableEnabled' => true,
                'mapping' => [$config],
            ],
            ['componentId' => 'foo'],
            'local',
            true,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);
        $tableDetails = $this->clientWrapper->getBasicClient()->getTable($tableId);
        self::assertFalse($tableDetails['isTyped']);
    }

    #[NeedsEmptyOutputBucket]
    public function testCreateTableDefinitionErrorHandling(): void
    {
        $tableId = $this->emptyOutputBucketId . '.tableDefinitionWithInvalidDataTypes';
        $config = [
            'source' => 'tableDefinition.csv',
            'destination' => $tableId,
            'columns' => ['Id', 'Name'],
            'column_metadata' => [
                'Id' => (new GenericStorage('int', ['nullable' => false]))->toMetadata(),
            ],
            'primary_key' => ['Id', 'Name'],
        ];

        touch(sprintf('%s/upload/tableDefinition.csv', $this->temp->getTmpFolder()));
        $writer = new TableWriter($this->getLocalStagingFactory());

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionCode(400);
        $this->expectExceptionMessageMatches(
            '/^Cannot create table \"tableDefinitionWithInvalidDataTypes\" definition in Storage API: {.+}$/u'
        );
        $this->expectExceptionMessage('Selected columns are not included in table definition');

        $writer->uploadTables(
            'upload',
            [
                'typedTableEnabled' => true,
                'mapping' => [$config],
            ],
            ['componentId' => 'foo'],
            'local',
            true,
            false
        );
    }

    /**
     * @dataProvider configProvider
     */
    #[NeedsEmptyOutputBucket]
    public function testWriterCreateTableDefinition(
        array $configTemplate,
        array $expectedTypes
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
            EOT
        );
        $writer = new TableWriter($this->getLocalStagingFactory());

        $tableQueue =  $writer->uploadTables(
            'upload',
            [
                'typedTableEnabled' => true,
                'mapping' => [$config],
            ],
            ['componentId' => 'foo'],
            'local',
            true,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);
        $tableDetails = $this->clientWrapper->getBasicClient()->getTable($config['destination']);
        self::assertTrue($tableDetails['isTyped']);

        self::assertDataTypeDefinition($tableDetails['columnMetadata']['Id'], $expectedTypes['Id']);
        self::assertDataTypeDefinition($tableDetails['columnMetadata']['Name'], $expectedTypes['Name']);
        self::assertDataTypeDefinition($tableDetails['columnMetadata']['birthweight'], $expectedTypes['birthweight']);
        self::assertDataTypeDefinition($tableDetails['columnMetadata']['created'], $expectedTypes['created']);
    }

    public function configProvider(): Generator
    {
        yield 'base types' => [
            [
                'source' => 'tableDefinition.csv',
                'destination' => '%s.tableDefinition',
                'columns' => ['Id', 'Name', 'birthweight', 'created'],
                'column_metadata' => [
                    'Id' => (new GenericStorage('int', ['nullable' => false]))->toMetadata(),
                    'Name' => (new GenericStorage('varchar', ['length' => '17', 'nullable' => false]))->toMetadata(),
                    'birthweight' => (new GenericStorage('decimal', ['length' => '5,2']))->toMetadata(),
                    'created' => (new GenericStorage('timestamp'))->toMetadata(),
                ],
                'primary_key' => ['Id', 'Name'],
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
                    'length' => '38,0',
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
                'columns' => ['Id', 'Name', 'birthweight', 'created'],
                'metadata' => [
                    [
                        'key' => 'KBC.datatype.backend',
                        'value' => 'snowflake',
                    ],
                ],
                'column_metadata' => [
                    'Id' => (new Snowflake(
                        Snowflake::TYPE_INTEGER,
                        ['nullable' => false]
                    ))->toMetadata(),
                    'Name' => (new Snowflake(
                        Snowflake::TYPE_TEXT,
                        ['length' => '17', 'nullable' => false]
                    ))->toMetadata(),
                    'birthweight' => (new Snowflake(Snowflake::TYPE_DECIMAL, ['length' => '10,2']))->toMetadata(),
                    'created' => (new Snowflake(Snowflake::TYPE_TIMESTAMP_TZ))->toMetadata(),
                ],
                'primary_key' => ['Id', 'Name'],
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

    /**
     * @dataProvider incrementalFlagProvider
     */
    #[NeedsEmptyInputBucket, NeedsEmptyOutputBucket]
    public function testWriterUpdateTableDefinitionWithBaseTypes(bool $incrementalFlag): void
    {
        $tableId = $this->emptyInputBucketId . '.tableDefinition';

        $idDatatype = new GenericStorage('int', ['nullable' => false]);
        $nameDatatype = new GenericStorage('varchar', ['length' => '17', 'nullable' => false]);
        $birthweightDatatype = new GenericStorage('decimal', ['length' => '5,2']);
        $created = new GenericStorage('timestamp');

        $config = [
            'source' => 'tableDefinition.csv',
            'destination' => $tableId,
            'incremental' => $incrementalFlag,
            'columns' => ['Id', 'Name', 'birthweight', 'created'],
            'primary_key' => ['Id', 'Name'],
            'column_metadata' => [
                'Id' => $idDatatype->toMetadata(),
                'Name' => $nameDatatype->toMetadata(),
                'birthweight' => $birthweightDatatype->toMetadata(),
                'created' => $created->toMetadata(),
            ],
        ];

        $this->clientWrapper->getBasicClient()->createTableDefinition($this->emptyInputBucketId, [
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

        $runId = $this->clientWrapper->getBasicClient()->generateRunId();
        $this->clientWrapper->getBasicClient()->setRunId($runId);

        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/tableDefinition.csv',
            <<< EOT
            "1","bob","10.11","2021-12-12 16:45:21"
            "2","alice","5.63","2020-12-12 15:45:21"
            EOT
        );
        $writer = new TableWriter($this->getLocalStagingFactory());

        $tableQueue =  $writer->uploadTables(
            'upload',
            [
                'typedTableEnabled' => true,
                'mapping' => [$config],
            ],
            ['componentId' => 'foo'],
            'local',
            true,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $writerJobs = array_filter(
            $this->clientWrapper->getBasicClient()->listJobs(),
            function (array $job) use ($runId) {
                return $runId === $job['runId'];
            }
        );

        self::assertCount(4, $writerJobs);

        // tableColumnAdd jobs
        $job = array_pop($writerJobs);
        self::assertSame('tableColumnAdd', $job['operationName']);
        self::assertSame('birthweight', $job['operationParams']['name']);
        self::assertSame('NUMERIC', $job['operationParams']['basetype']);
        self::assertNull($job['operationParams']['definition']);

        $job = array_pop($writerJobs);
        self::assertSame('tableColumnAdd', $job['operationName']);
        self::assertSame('created', $job['operationParams']['name']);
        self::assertSame('TIMESTAMP', $job['operationParams']['basetype']);
        self::assertNull($job['operationParams']['definition']);

        // modify PK job
        $job = array_pop($writerJobs);
        self::assertSame('tablePrimaryKeyAdd', $job['operationName']);
        self::assertSame(['Id', 'Name'], $job['operationParams']['columns']);

        // incremental import
        $job = array_pop($writerJobs);
        self::assertSame('tableImport', $job['operationName']);
        self::assertSame($incrementalFlag, $job['operationParams']['params']['incremental']);
        self::assertSame([], $job['results']['newColumns']);

        $tableDetails = $this->clientWrapper->getBasicClient()->getTable($tableId);
        self::assertTrue($tableDetails['isTyped']);

        self::assertDataTypeDefinition($tableDetails['columnMetadata']['Id'], [
            'type' => Snowflake::TYPE_NUMBER,
            'length' => '38,0', // default integer length
            'nullable' => true,
        ]);
        self::assertDataTypeDefinition($tableDetails['columnMetadata']['Name'], [
            'type' => Snowflake::TYPE_VARCHAR,
            'length' => '16777216', // default varchar length
            'nullable' => true,
        ]);
        self::assertDataTypeDefinition($tableDetails['columnMetadata']['birthweight'], [
            'type' => Snowflake::TYPE_NUMBER,
            'length' => '38,0',
            'nullable' => true,
        ]);
        self::assertDataTypeDefinition($tableDetails['columnMetadata']['created'], [
            'type' => Snowflake::TYPE_TIMESTAMP_LTZ,
            'length' => '9',
            'nullable' => true,
        ]);
    }

    /**
     * @dataProvider incrementalFlagProvider
     */
    #[NeedsEmptyOutputBucket]
    public function testWriterUpdateTableDefinitionWithNativeTypes(bool $incrementalFlag): void
    {
        $tableId = $this->emptyOutputBucketId . '.tableDefinition';

        $idDatatype = new Snowflake(Snowflake::TYPE_INTEGER, ['nullable' => false]);
        $nameDatatype = new Snowflake(Snowflake::TYPE_TEXT, ['length' => '17', 'nullable' => false]);
        $birthweightDatatype = new Snowflake(Snowflake::TYPE_DECIMAL, ['length' => '10,2']);
        $created = new Snowflake(Snowflake::TYPE_TIMESTAMP_TZ);

        $config = [
            'source' => 'tableDefinition.csv',
            'destination' => $tableId,
            'incremental' => $incrementalFlag,
            'columns' => ['Id', 'Name', 'birthweight', 'created'],
            'primary_key' => ['Id', 'Name'],
            'metadata' => [
                [
                    'key' => 'KBC.datatype.backend',
                    'value' => 'snowflake',
                ],
            ],
            'column_metadata' => [
                'Id' => $idDatatype->toMetadata(),
                'Name' => $nameDatatype->toMetadata(),
                'birthweight' => $birthweightDatatype->toMetadata(),
                'created' => $created->toMetadata(),
            ],
        ];

        $this->clientWrapper->getBasicClient()->createTableDefinition($this->emptyOutputBucketId, [
            'name' => 'tableDefinition',
            'primaryKeysNames' => [],
            'columns' => [
                [
                    'name' => 'Id',
                    'definition' => $idDatatype->toArray(),
                ],
                [
                    'name' => 'Name',
                    'definition' => $nameDatatype->toArray(),
                ],
            ],
        ]);

        $runId = $this->clientWrapper->getBasicClient()->generateRunId();
        $this->clientWrapper->getBasicClient()->setRunId($runId);

        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/tableDefinition.csv',
            <<< EOT
            "1","bob","10.11","2021-12-12 16:45:21"
            "2","alice","5.63","2020-12-12 15:45:21"
            EOT
        );
        $writer = new TableWriter($this->getLocalStagingFactory());

        $tableQueue =  $writer->uploadTables(
            'upload',
            [
                'typedTableEnabled' => true,
                'mapping' => [$config],
            ],
            ['componentId' => 'foo'],
            'local',
            true,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $writerJobs = array_filter(
            $this->clientWrapper->getBasicClient()->listJobs(),
            function (array $job) use ($runId) {
                return $runId === $job['runId'];
            }
        );

        self::assertCount(4, $writerJobs);
        self::assertTableTypedColumnAddJob(
            array_pop($writerJobs),
            'birthweight',
            null,
            [
                'type' => 'DECIMAL',
                'length' => '10,2',
                'nullable' => true,
            ]
        );
        self::assertTableTypedColumnAddJob(
            array_pop($writerJobs),
            'created',
            null,
            [
                'type' => 'TIMESTAMP_TZ',
                'length' => null,
                'nullable' => true,
            ]
        );
        self::assertTablePrimaryKeyAddJob(array_pop($writerJobs), ['Id', 'Name']);
        self::assertTableImportJob(array_pop($writerJobs), $incrementalFlag);

        $tableDetails = $this->clientWrapper->getBasicClient()->getTable($tableId);
        self::assertTrue($tableDetails['isTyped']);

        self::assertDataTypeDefinition($tableDetails['columnMetadata']['Id'], [
            'type' => Snowflake::TYPE_NUMBER,
            'length' => '38,0', // default integer length
            'nullable' => false,
        ]);
        self::assertDataTypeDefinition($tableDetails['columnMetadata']['Name'], [
            'type' => Snowflake::TYPE_VARCHAR,
            'length' => '17',
            'nullable' => false,
        ]);
        self::assertDataTypeDefinition($tableDetails['columnMetadata']['birthweight'], [
            'type' => Snowflake::TYPE_NUMBER,
            'length' => '10,2',
            'nullable' => true,
        ]);
        self::assertDataTypeDefinition($tableDetails['columnMetadata']['created'], [
            'type' => Snowflake::TYPE_TIMESTAMP_TZ,
            'length' => '9', // timestamp_tz has length 9 apparently
            'nullable' => true,
        ]);
    }

    public function writerUpdateTableDefinitionWithUnknownDataTypesProvider(): Generator
    {
        yield 'incremental load with any column metadata' => [true, null];
        yield 'full load with any column metadata' => [false, null];
        yield 'incremental load with empty column metadata' => [true, []];
        yield 'full load with empty column metadata' => [false, []];

        $dummyColumnMetadata = [
            [
                'key' => 'foo',
                'value' => 'bar',
            ],
        ];

        yield 'incremental load with dummy column metadata' => [
            true,
            [
                'Id' => $dummyColumnMetadata,
                'Name' => $dummyColumnMetadata,
                'birthweight' => $dummyColumnMetadata,
                'created' => $dummyColumnMetadata,
            ]
        ];
        yield 'full load with dummy column metadata' => [
            false,
            [
                'Id' => $dummyColumnMetadata,
                'Name' => $dummyColumnMetadata,
                'birthweight' => $dummyColumnMetadata,
                'created' => $dummyColumnMetadata,
            ]
        ];
    }

    /**
     * @dataProvider writerUpdateTableDefinitionWithUnknownDataTypesProvider
     */
    #[NeedsEmptyOutputBucket]
    public function testWriterUpdateTableDefinitionWithUnknownDataTypes(
        bool $incrementalFlag,
        ?array $columnMetadata
    ): void {
        $tableId = $this->emptyOutputBucketId . '.tableDefinition';

        $idDatatype = new Snowflake(Snowflake::TYPE_INTEGER, ['nullable' => false]);
        $nameDatatype = new Snowflake(Snowflake::TYPE_TEXT, ['length' => '17', 'nullable' => false]);

        $config = [
            'source' => 'tableDefinition.csv',
            'destination' => $tableId,
            'incremental' => $incrementalFlag,
            'columns' => ['Id', 'Name', 'birthweight', 'created'],
            'primary_key' => ['Id', 'Name'],
            'metadata' => [
                [
                    'key' => 'KBC.datatype.backend',
                    'value' => 'snowflake',
                ],
            ],
        ];

        if ($columnMetadata !== null) {
            $config['column_metadata'] = $columnMetadata;
        }

        $this->clientWrapper->getBasicClient()->createTableDefinition($this->emptyOutputBucketId, [
            'name' => 'tableDefinition',
            'primaryKeysNames' => [],
            'columns' => [
                [
                    'name' => 'Id',
                    'definition' => $idDatatype->toArray(),
                ],
                [
                    'name' => 'Name',
                    'definition' => $nameDatatype->toArray(),
                ],
            ],
        ]);

        $runId = $this->clientWrapper->getBasicClient()->generateRunId();
        $this->clientWrapper->getBasicClient()->setRunId($runId);

        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/tableDefinition.csv',
            <<< EOT
            "1","bob","10.11","2021-12-12 16:45:21"
            "2","alice","5.63","2020-12-12 15:45:21"
            EOT
        );
        $writer = new TableWriter($this->getLocalStagingFactory());

        $tableQueue =  $writer->uploadTables(
            'upload',
            [
                'typedTableEnabled' => true,
                'mapping' => [$config],
            ],
            ['componentId' => 'foo'],
            'local',
            true,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $writerJobs = array_filter(
            $this->clientWrapper->getBasicClient()->listJobs(),
            function (array $job) use ($runId) {
                return $runId === $job['runId'];
            }
        );

        self::assertCount(4, $writerJobs);
        self::assertTableTypedColumnAddJob(
            array_pop($writerJobs),
            'birthweight',
            'STRING',
            null
        );
        self::assertTableTypedColumnAddJob(
            array_pop($writerJobs),
            'created',
            'STRING',
            null
        );
        self::assertTablePrimaryKeyAddJob(array_pop($writerJobs), ['Id', 'Name']);
        self::assertTableImportJob(array_pop($writerJobs), $incrementalFlag);

        $tableDetails = $this->clientWrapper->getBasicClient()->getTable($tableId);
        self::assertTrue($tableDetails['isTyped']);

        self::assertDataTypeDefinition($tableDetails['columnMetadata']['Id'], [
            'type' => Snowflake::TYPE_NUMBER,
            'length' => '38,0', // default integer length
            'nullable' => false,
        ]);
        self::assertDataTypeDefinition($tableDetails['columnMetadata']['Name'], [
            'type' => Snowflake::TYPE_VARCHAR,
            'length' => '17',
            'nullable' => false,
        ]);
        self::assertDataTypeDefinition($tableDetails['columnMetadata']['birthweight'], [
            'type' => Snowflake::TYPE_VARCHAR,
            'length' => '16777216',
            'nullable' => true,
        ]);
        self::assertDataTypeDefinition($tableDetails['columnMetadata']['created'], [
            'type' => Snowflake::TYPE_VARCHAR,
            'length' => '16777216',
            'nullable' => true,
        ]);
    }

    private static function assertDataTypeDefinition(array $metadata, array $expectedType): void
    {
        $typeMetadata = array_values(array_filter($metadata, function ($metadatum) {
            return $metadatum['key'] === Common::KBC_METADATA_KEY_TYPE
                && $metadatum['provider'] === 'storage';
        }));
        self::assertCount(1, $typeMetadata);
        self::assertSame($expectedType['type'], $typeMetadata[0]['value']);

        $lengthMetadata = array_values(array_filter($metadata, function ($metadatum) {
            return $metadatum['key'] === Common::KBC_METADATA_KEY_LENGTH
                && $metadatum['provider'] === 'storage';
        }));
        self::assertCount(1, $lengthMetadata);
        self::assertSame($expectedType['length'], $lengthMetadata[0]['value']);

        $nullableMetadata = array_values(array_filter($metadata, function ($metadatum) {
            return $metadatum['key'] === Common::KBC_METADATA_KEY_NULLABLE
            && $metadatum['provider'] === 'storage';
        }));
        self::assertCount(1, $nullableMetadata);
        self::assertEquals($expectedType['nullable'], $nullableMetadata[0]['value']);
    }

    private static function assertTableTypedColumnAddJob(
        array $jobData,
        string $expectedColumnName,
        ?string $expectedBaseType,
        ?array $expectedDefinition
    ): void {
        self::assertSame('tableColumnAdd', $jobData['operationName']);
        self::assertSame('success', $jobData['status']);
        self::assertSame($expectedColumnName, $jobData['operationParams']['name']);
        self::assertSame($expectedBaseType, $jobData['operationParams']['basetype']);
        self::assertSame($expectedDefinition, $jobData['operationParams']['definition']);
    }

    private static function assertTablePrimaryKeyAddJob(array $jobData, array $expectedPk): void
    {
        self::assertSame('tablePrimaryKeyAdd', $jobData['operationName']);
        self::assertSame('success', $jobData['status']);
        self::assertSame($expectedPk, $jobData['operationParams']['columns']);
    }
}
