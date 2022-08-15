<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\Datatype\Definition\Common;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionColumnFactory;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionFactory;
use Keboola\OutputMapping\Writer\TableWriter;

class TableDefinitionTest extends BaseWriterTest
{
    /**
     * @dataProvider configProvider
     */
    public function testWriterCreateTableDefinition(
        array $config,
        bool $shouldBeTypedTable,
        array $expectedTypes
    ): void {
        try {
            $this->clientWrapper->getBasicClient()->dropTable($config['destination']);
        } catch (\Throwable $exception) {
            if ($exception->getCode() !== 404) {
                throw $exception;
            }
        }
        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . "/upload/tableDefinition.csv",
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
                'mapping' => [$config]
            ],
            ['componentId' => 'foo'],
            'local',
            true
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $tableDetails = $this->clientWrapper->getBasicClient()->getTable($config['destination']);
        $this->assertEquals($shouldBeTypedTable, $tableDetails['isTyped']);
        if (!empty($expectedTypes)) {
            self::assertDataType($tableDetails['columnMetadata']['Id'], $expectedTypes['Id']);
            self::assertDataType($tableDetails['columnMetadata']['Name'], $expectedTypes['Name']);
            self::assertDataType($tableDetails['columnMetadata']['birthweight'], $expectedTypes['birthweight']);
            self::assertDataType($tableDetails['columnMetadata']['created'], $expectedTypes['created']);
        }
    }

    public function configProvider(): \Generator
    {
        yield [
            [
                'source' => 'tableDefinition.csv',
                'destination' => 'out.c-output-mapping.tableDefinition',
                'columns' => ['Id', 'Name', 'birthweight', 'created'],
                'column_metadata' => [
                    'Id' => (new GenericStorage('int', ['nullable' => false]))->toMetadata(),
                    'Name' => (new GenericStorage('varchar', ['length' => '17', 'nullable' => false]))->toMetadata(),
                    'birthweight' => (new GenericStorage('decimal', ['length' => '5,2']))->toMetadata(),
                    'created' => (new GenericStorage('timestamp'))->toMetadata(),
                ],
                'primary_key' => ['Id', 'Name'],
            ],
            true,
            [
                'Id' => ['type' => Snowflake::TYPE_NUMBER],
                'Name' => ['type' => Snowflake::TYPE_VARCHAR],
                'birthweight' => ['type' => Snowflake::TYPE_NUMBER],
                'created' => ['type' => Snowflake::TYPE_TIMESTAMP_LTZ],
            ],
        ];

        // test native snowflake types
        yield [
            [
                'source' => 'tableDefinition.csv',
                'destination' => 'out.c-output-mapping.tableDefinition',
                'columns' => ['Id', 'Name', 'birthweight', 'created'],
                'metadata' => [
                    [
                        'key' => TableDefinitionColumnFactory::NATIVE_TYPE_METADATA_KEY,
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
            true,
            [
                // INTEGER is an alias of NUMBER in snflk and describe returns the root type
                'Id' => [
                    'type' => Snowflake::TYPE_NUMBER,
                    'length' => '38,0', // default integer length
                    'nullable' => false,
                ],
                // LIKEWISE TEXT is an alias of VARCHAR
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
        yield [
            [
                'source' => 'tableDefinition.csv',
                'destination' => 'out.c-output-mapping.tableDefinition',
                'columns' => ['Id', 'Name', 'birthweight', 'created'],
                'column_metadata' => [],
                'primary_key' => ['Id', 'Name'],
            ],
            false,
            [],
        ];
    }

    private static function assertDataType($metadata, $expectedType): void
    {
        $typeFound = false;
        foreach ($metadata as $metadatum) {
            if ($metadatum['key'] === Common::KBC_METADATA_KEY_TYPE
                && $metadatum['provider'] === 'storage'
            ) {
                self::assertSame($expectedType['type'], $metadatum['value']);
                $typeFound = true;
            }
            if ($metadatum['key'] === Common::KBC_METADATA_KEY_LENGTH
                && array_key_exists('length', $expectedType)
                && $metadatum['provider'] === 'storage'
            ) {
                self::assertSame($expectedType['length'], $metadatum['value']);
            }
            if ($metadatum['key'] === Common::KBC_METADATA_KEY_NULLABLE
                && array_key_exists('nullable', $expectedType)
                && $metadatum['provider'] === 'storage'
            ) {
                self::assertEquals($expectedType['nullable'], $metadatum['value']);
            }
        }
        if (!$typeFound) {
            self::fail('Metadata key ' . Common::KBC_METADATA_KEY_TYPE . ' not found');
        }
    }
}
