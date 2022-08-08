<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\Datatype\Definition\GenericStorage;
use Keboola\OutputMapping\Writer\TableWriter;

class TableDefinitionTest extends BaseWriterTest
{
    /**
     * @dataProvider configProvider
     */
    public function testWriterCreateTableDefinition(array $config, bool $shouldBeTypedTable): void
    {
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
            "1","bob","2001-1-1","2021-12-12 16:45:21"
            "2","alice","2002-2-2","2020-12-12 15:45:21"
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
                // FIXME: Enable this once https://keboola.atlassian.net/browse/KBC-2850 is fixed
                // 'primary_key' => ['Id', 'Name'],
            ],
            true,
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
        ];
    }
}
