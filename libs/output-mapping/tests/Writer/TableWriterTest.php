<?php

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\OutputMapping\Writer\TableWriter;
use Psr\Log\Test\TestLogger;

class TableWriterTest extends BaseWriterTest
{
    /**
     * @group tableWriterV1
     */
    public function testV1WriterIsUsed()
    {
        $root = $this->tmp->getTmpFolder() . '/upload/';
        file_put_contents($root . 'table.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $logger = new TestLogger();

        $tableWriter = new TableWriter($this->getStagingFactory(null, 'json', $logger));
        $tableWriter->uploadTables('upload', [
            'mapping' => [
                ['source' => 'table.csv', 'destination' => 'out.c-output-mapping-test.table']
            ]
        ], ['componentId' => 'foo'], 'local');

        self::assertTrue($logger->hasInfo([
            'message' => 'Using TableWriter V1 to upload tables'
        ]));
    }

    /**
     * @group tableWriterV2
     */
    public function testV2WriterIsUsed()
    {
        $root = $this->tmp->getTmpFolder() . '/upload/';
        file_put_contents($root . 'table.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $logger = new TestLogger();

        $tableWriter = new TableWriter($this->getStagingFactory(null, 'json', $logger));
        $tableWriter->uploadTables('upload', [
            'mapping' => [
                ['source' => 'table.csv', 'destination' => 'out.c-output-mapping-test.table']
            ]
        ], ['componentId' => 'foo'], 'local');

        self::assertTrue($logger->hasInfo([
            'message' => 'Using TableWriter V2 to upload tables'
        ]));
    }
}
