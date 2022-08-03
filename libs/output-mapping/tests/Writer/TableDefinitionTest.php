<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\Datatype\Definition\GenericStorage;
use Keboola\OutputMapping\Writer\TableWriter;

class TableDefinitionTest extends BaseWriterTest
{
    public function setUp()
    {
        parent::setUp();
        try {
            $this->clientWrapper->getBasicClient()->dropTable('out.c-output-mapping.tableDefinition');
        } catch (\Throwable $exception) {
            if ($exception->getCode() !== 404) {
                throw $exception;
            }
        }
    }

    /**
     * @dataProvider configProvider
     */
    public function testCreateTableDefinition(array $config): void
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . "/upload/tableDefinition.csv",
            <<< EOT
            "Id","Name", "birthday", "created"
            "1","bob","2001-1-1","2021-12-12 16:45:21"
            "2","alice","2002-2-2","2020-12-12 15:45:21"
            EOT
        );
        $writer = new TableWriter($this->getStagingFactory());

        $tableQueue =  $writer->uploadTables(
            'upload',
            [
                'typedTableEnabled' => true,
                'mapping' => $config
            ],
            ['componentId' => 'foo'],
            'local',
            true
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
    }

    public function configProvider(): \Generator
    {
        yield [
            [
                [
                    'source' => 'tableDefinition.csv',
                    'destination' => 'out.c-output-mapping.tableDefinition',
                    'column_metadata' => [
                        'Id' => (new GenericStorage('int'))->toMetadata(),
                        'Name' => (new GenericStorage('varchar', ['length' => '17']))->toMetadata(),
                        'birthday' => (new GenericStorage('date'))->toMetadata(),
                        'created' => (new GenericStorage('timestamp'))->toMetadata(),
                    ],
                ],
            ],
        ];
    }
}
