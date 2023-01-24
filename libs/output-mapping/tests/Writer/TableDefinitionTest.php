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
        $config = [
            'source' => 'tableDefinition.csv',
            'destination' => self::OUTPUT_BUCKET . '.tableDefinition',
            'columns' => ['Id', 'Name', 'birthweight', 'created'],
            'column_metadata' => [],
            'primary_key' => ['Id', 'Name'],
        ];
        try {
            $this->clientWrapper->getBasicClient()->dropTable($config['destination']);
        } catch (Throwable $exception) {
            if ($exception->getCode() !== 404) {
                throw $exception;
            }
        }
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
        self::assertFalse($tableDetails['isTyped']);
    }

    public function testCreateTableDefinitionErrorHandling(): void
    {
        $config = [
            'source' => 'tableDefinition.csv',
            'destination' => self::OUTPUT_BUCKET . '.tableDefinitionWithInvalidDataTypes',
            'columns' => ['Id', 'Name'],
            'column_metadata' => [
                'Id' => (new GenericStorage('int', ['nullable' => false]))->toMetadata(),
            ],
            'primary_key' => ['Id', 'Name'],
        ];

        try {
            $this->clientWrapper->getBasicClient()->dropTable($config['destination']);
        } catch (Throwable $exception) {
            if ($exception->getCode() !== 404) {
                throw $exception;
            }
        }

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
        try {
            $this->clientWrapper->getBasicClient()->dropTable($config['destination']);
        } catch (Throwable $exception) {
            if ($exception->getCode() !== 404) {
                throw $exception;
            }
        }
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
}
