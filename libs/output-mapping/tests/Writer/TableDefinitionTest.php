<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\Datatype\Definition\Common;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\Datatype\Definition\Snowflake;
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
            "1","bob","10:10:10","2021-12-12 16:45:21"
            "2","alice","05:5:5","2020-12-12 15:45:21"
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
            self::assertDataType($tableDetails['columnMetadata']['birthday'], $expectedTypes['birthday']);
            self::assertDataType($tableDetails['columnMetadata']['created'], $expectedTypes['created']);
        }
    }

    public function configProvider(): \Generator
    {
        yield [
            [
                'source' => 'tableDefinition.csv',
                'destination' => 'out.c-output-mapping.tableDefinition',
                'columns' => ['Id', 'Name', 'birthday', 'created'],
                'column_metadata' => [
                    'Id' => (new GenericStorage('int', ['nullable' => false]))->toMetadata(),
                    'Name' => (new GenericStorage('varchar', ['length' => '17', 'nullable' => false]))->toMetadata(),
                    'birthday' => (new GenericStorage('date'))->toMetadata(),
                    'created' => (new GenericStorage('timestamp'))->toMetadata(),
                ],
                'primary_key' => ['Id', 'Name'],
            ],
            true,
            [
                'Id' => Snowflake::TYPE_NUMBER,
                'Name' => Snowflake::TYPE_VARCHAR,
                'birthday' => Snowflake::TYPE_DATE,
                'created' => Snowflake::TYPE_TIMESTAMP_LTZ,
            ],
        ];
        // test native snowflake types
        yield [
            [
                'source' => 'tableDefinition.csv',
                'destination' => 'out.c-output-mapping.tableDefinition',
                'columns' => ['Id', 'Name', 'birthday', 'created'],
                'metadata' => [
                    [
                        'key' => TableDefinitionFactory::NATIVE_TYPE_METADATA_KEY,
                        'value' => 'snowflake',
                    ],
                ],
                'column_metadata' => [
                    'Id' => (new Snowflake(
                        Snowflake::TYPE_NUMBER,
                        ['nullable' => false, 'length' => '10,0']
                    ))->toMetadata(),
                    'Name' => (new Snowflake(
                        Snowflake::TYPE_TEXT,
                        ['length' => '17', 'nullable' => false]
                    ))->toMetadata(),
                    'birthday' => (new Snowflake(Snowflake::TYPE_TIME))->toMetadata(),
                    'created' => (new Snowflake(Snowflake::TYPE_TIMESTAMP_TZ))->toMetadata(),
                ],
                'primary_key' => ['Id', 'Name'],
            ],
            true,
            [
                'Id' => Snowflake::TYPE_NUMBER,
                'Name' => Snowflake::TYPE_VARCHAR,
                'birthday' => Snowflake::TYPE_TIME,
                'created' => Snowflake::TYPE_TIMESTAMP_TZ,
            ],
        ];

        yield [
            [
                'source' => 'tableDefinition.csv',
                'destination' => 'out.c-output-mapping.tableDefinition',
                'columns' => ['Id', 'Name', 'birthday', 'created'],
                'column_metadata' => [],
                'primary_key' => ['Id', 'Name'],
            ],
            false,
            [],
        ];
    }

    private static function assertDataType($metadata, $expectedType): void
    {
        foreach ($metadata as $metadatum) {
            if ($metadatum['key'] === Common::KBC_METADATA_KEY_TYPE) {
                self::assertSame($expectedType, $metadatum['value']);
                return;
            }
        }
        self::fail('Metadata key ' . Common::KBC_METADATA_KEY_TYPE . ' not found');
    }
}
