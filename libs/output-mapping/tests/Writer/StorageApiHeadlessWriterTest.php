<?php

namespace Keboola\OutputMapping\Tests;

use Keboola\Csv\CsvFile;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Tests\Writer\BaseWriterTest;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\TableExporter;

class StorageApiHeadlessWriterTest extends BaseWriterTest
{
    public function setUp()
    {
        parent::setUp();
        $this->clearBuckets(['out.c-output-mapping-test']);
        $this->clearFileUploads(['output-mapping-test']);
        $this->clientWrapper->getBasicClient()->createBucket('output-mapping-test', 'out');
    }

    public function testWriteTableOutputMapping()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table.csv", "\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                "source" => "table.csv",
                "destination" => "out.c-output-mapping-test.table",
                "columns" => ["Id","Name"]
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables('upload', ["mapping" => $configs], ['componentId' => 'foo'], StrategyFactory::LOCAL);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $this->assertEquals(1, $tableQueue->getTaskCount());

        $tables = $this->clientWrapper->getBasicClient()->listTables("out.c-output-mapping-test");
        $this->assertCount(1, $tables);
        $table = $this->clientWrapper->getBasicClient()->getTable("out.c-output-mapping-test.table");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $root . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-output-mapping-test.table', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv(file_get_contents($downloadedFile));
        $this->assertCount(2, $table);
        $this->assertContains(["Id" => "test", "Name" => "test"], $table);
        $this->assertContains(["Id" => "aabb", "Name" => "ccdd"], $table);
    }

    public function testWriteTableOutputMappingEmptyFile()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table", "");

        $configs = [
            [
                "source" => "table",
                "destination" => "out.c-output-mapping-test.table",
                "columns" => ["Id","Name"]
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables('upload', ["mapping" => $configs], ['componentId' => 'foo'], StrategyFactory::LOCAL);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $table = $this->clientWrapper->getBasicClient()->getTable("out.c-output-mapping-test.table");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-output-mapping-test.table', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv(file_get_contents($downloadedFile));
        $this->assertCount(0, $table);
    }

    public function testWriteTableOutputMappingAndManifest()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/table2.csv",
            "\"test\",\"test\"\n"
        );
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/table2.csv.manifest",
            "{\"destination\": \"out.c-output-mapping-test.table2\",\"primary_key\":[\"Id\"],\"columns\":[\"a\",\"b\"]}"
        );

        $configs = [
            [
                "source" => "table2.csv",
                "destination" => "out.c-output-mapping-test.table",
                "columns" => ["Id", "Name"]
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables('upload', ["mapping" => $configs], ['componentId' => 'foo'], StrategyFactory::LOCAL);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables("out.c-output-mapping-test");
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-output-mapping-test.table', $tables[0]["id"]);
        $table = $this->clientWrapper->getBasicClient()->getTable("out.c-output-mapping-test.table");
        $this->assertEquals([], $table["primaryKey"]);
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-output-mapping-test.table', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv(file_get_contents($downloadedFile));
        $this->assertCount(1, $table);
        $this->assertEquals([["Id" => "test", "Name" => "test"]], $table);
    }

    public function testWriteTableManifest()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/out.c-output-mapping-test.table.csv",
            "\"test\",\"test\"\n"
        );
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/out.c-output-mapping-test.table.csv.manifest",
            "{\"destination\": \"out.c-output-mapping-test.table\",\"primary_key\":[\"Id\",\"Name\"],\"columns\":[\"Id\",\"Name\"]}"
        );

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables('upload', [], ['componentId' => 'foo'], StrategyFactory::LOCAL);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables("out.c-output-mapping-test");
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-output-mapping-test.table', $tables[0]["id"]);
        $table = $this->clientWrapper->getBasicClient()->getTable("out.c-output-mapping-test.table");
        $this->assertEquals(["Id", "Name"], $table["primaryKey"]);
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-output-mapping-test.table', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv(file_get_contents($downloadedFile));
        $this->assertCount(1, $table);
        $this->assertEquals([["Id" => "test", "Name" => "test"]], $table);
    }

    public function testWriteTableOutputMappingExistingTable()
    {
        $csvFile = new CsvFile($this->tmp->createFile('header')->getPathname());
        $csvFile->writeRow(["Id", "Name"]);
        $this->clientWrapper->getBasicClient()->createTable("out.c-output-mapping-test", "table", $csvFile);
        $tables = $this->clientWrapper->getBasicClient()->listTables("out.c-output-mapping-test");
        $this->assertCount(1, $tables);
        $table = $this->clientWrapper->getBasicClient()->getTable("out.c-output-mapping-test.table");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table.csv", "\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                "source" => "table.csv",
                "destination" => "out.c-output-mapping-test.table",
                "columns" => ["Id","Name"]
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables('upload', ["mapping" => $configs], ['componentId' => 'foo'], StrategyFactory::LOCAL);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables("out.c-output-mapping-test");
        $this->assertCount(1, $tables);
        $table = $this->clientWrapper->getBasicClient()->getTable("out.c-output-mapping-test.table");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-output-mapping-test.table', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv(file_get_contents($downloadedFile));
        $this->assertCount(2, $table);
        $this->assertContains(["Id" => "test", "Name" => "test"], $table);
        $this->assertContains(["Id" => "aabb", "Name" => "ccdd"], $table);
    }
}
