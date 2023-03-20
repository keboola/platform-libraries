<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer;

use Generator;
use Keboola\Datatype\Definition\Common;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\TableWriter;
use Throwable;

class TableDefinitionTest extends BaseWriterTest
{
    private const OUTPUT_BUCKET = 'out.c-TableDefinitionTest';

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

    public function testNotCreateTableDefinition(): void
    {
        $tableId = self::OUTPUT_BUCKET . '.tableDefinition';
        $config = [
            'source' => 'tableDefinition.csv',
            'destination' => $tableId,
            'columns' => ['Id', 'Name', 'birthweight', 'created'],
            'column_metadata' => [],
            'primary_key' => ['Id', 'Name'],
        ];

        $this->dropTableIfExists($tableId);

        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . '/upload/tableDefinition.csv',
            <<< EOT
            "1","bob","10.11","2021-12-12 16:45:21"
            "2","alice","5.63","2020-12-12 15:45:21"
            EOT
        );
        $writer = new TableWriter($this->getStagingFactory());

        $tableQueue =  $writer->uploadTables(
            'upload',
            [
                'typedTableEnabled' => true,
                'mapping' => [$config],
            ],
            ['componentId' => 'foo'],
            'local',
            true
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);
        $tableDetails = $this->clientWrapper->getBasicClient()->getTable($tableId);
        self::assertFalse($tableDetails['isTyped']);
    }

    public function testCreateTableDefinitionErrorHandling(): void
    {
        $tableId = self::OUTPUT_BUCKET . '.tableDefinitionWithInvalidDataTypes';
        $config = [
            'source' => 'tableDefinition.csv',
            'destination' => $tableId,
            'columns' => ['Id', 'Name'],
            'column_metadata' => [
                'Id' => (new GenericStorage('int', ['nullable' => false]))->toMetadata(),
            ],
            'primary_key' => ['Id', 'Name'],
        ];

        $this->dropTableIfExists($tableId);

        touch(sprintf('%s/upload/tableDefinition.csv', $this->tmp->getTmpFolder()));
        $writer = new TableWriter($this->getStagingFactory());

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
            true
        );
    }

    /**
     * @dataProvider configProvider
     */
    public function testWriterCreateTableDefinition(
        array $config,
        array $expectedTypes
    ): void {
        $this->dropTableIfExists($config['destination']);

        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . '/upload/tableDefinition.csv',
            <<< EOT
            "1","bob","10.11","2021-12-12 16:45:21"
            "2","alice","5.63","2020-12-12 15:45:21"
            EOT
        );
        $writer = new TableWriter($this->getStagingFactory());

        $tableQueue =  $writer->uploadTables(
            'upload',
            [
                'typedTableEnabled' => true,
                'mapping' => [$config],
            ],
            ['componentId' => 'foo'],
            'local',
            true
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
                'destination' => self::OUTPUT_BUCKET . '.tableDefinition',
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
                'destination' => self::OUTPUT_BUCKET . '.tableDefinition',
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

    public function incrementalFlagProvider(): Generator
    {
        yield 'incremental load' => [true];
        yield 'full load' => [false];
    }

    /**
     * @dataProvider incrementalFlagProvider
     */
    public function testWriterUpdateTableDefinitionWithBaseTypes(bool $incrementalFlag): void
    {
        $tableId = self::OUTPUT_BUCKET . '.tableDefinition';
        $this->dropTableIfExists($tableId);

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

        $this->clientWrapper->getBasicClient()->createTableDefinition(self::OUTPUT_BUCKET, [
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

        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . '/upload/tableDefinition.csv',
            <<< EOT
            "1","bob","10.11","2021-12-12 16:45:21"
            "2","alice","5.63","2020-12-12 15:45:21"
            EOT
        );
        $writer = new TableWriter($this->getStagingFactory());

        $tableQueue =  $writer->uploadTables(
            'upload',
            [
                'typedTableEnabled' => true,
                'mapping' => [$config],
            ],
            ['componentId' => 'foo'],
            'local',
            true
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
    public function testWriterUpdateTableDefinitionWithNativeTypes(bool $incrementalFlag): void
    {
        $tableId = self::OUTPUT_BUCKET . '.tableDefinition';
        $this->dropTableIfExists($tableId);

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

        $this->clientWrapper->getBasicClient()->createTableDefinition(self::OUTPUT_BUCKET, [
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

        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . '/upload/tableDefinition.csv',
            <<< EOT
            "1","bob","10.11","2021-12-12 16:45:21"
            "2","alice","5.63","2020-12-12 15:45:21"
            EOT
        );
        $writer = new TableWriter($this->getStagingFactory());

        $tableQueue =  $writer->uploadTables(
            'upload',
            [
                'typedTableEnabled' => true,
                'mapping' => [$config],
            ],
            ['componentId' => 'foo'],
            'local',
            true
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
        self::assertTableColumnAddJob(
            array_pop($writerJobs),
            'birthweight',
            null,
            [
                'type' => 'DECIMAL',
                'length' => '10,2',
                'nullable' => true,
            ]
        );
        self::assertTableColumnAddJob(
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

    private static function assertTableColumnAddJob(
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

    private static function assertTableImportJob(array $jobData, bool $expectedIncrementalFlag): void
    {
        self::assertSame('tableImport', $jobData['operationName']);
        self::assertSame('success', $jobData['status']);
        self::assertSame($expectedIncrementalFlag, $jobData['operationParams']['params']['incremental']);
        self::assertSame([], $jobData['results']['newColumns']);
    }

    private function dropTableIfExists(string $tableId): void
    {
        try {
            $this->clientWrapper->getBasicClient()->dropTable($tableId);
        } catch (Throwable $exception) {
            if ($exception->getCode() !== 404) {
                throw $exception;
            }
        }
    }
}
