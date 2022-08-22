<?php

namespace Keboola\OutputMapping\Tests;

use Keboola\Csv\CsvFile;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Tests\Writer\BaseWriterTest;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\TableExporter;

class StorageApiHeadlessWriterTest extends BaseWriterTest
{
    private const FILE_TAG = 'StorageApiHeadlessWriterTest';
    private const OUTPUT_BUCKET = 'out.c-StorageApiHeadlessWriterTest';

    public function setUp()
    {
        parent::setUp();
        $this->clearBuckets([self::OUTPUT_BUCKET]);
        $this->clearFileUploads([self::FILE_TAG]);
        $this->clientWrapper->getBasicClient()->createBucket('StorageApiHeadlessWriterTest', 'out');
    }

    public function testWriteTableOutputMapping()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table.csv", "\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                "source" => "table.csv",
                "destination" => self::OUTPUT_BUCKET . ".table",
                "columns" => ["Id","Name"]
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            StrategyFactory::LOCAL
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $this->assertEquals(1, $tableQueue->getTaskCount());

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        $this->assertCount(1, $tables);
        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . '.table');
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $root . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table', $downloadedFile, []);
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
                "destination" => self::OUTPUT_BUCKET . ".table",
                "columns" => ["Id","Name"]
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            StrategyFactory::LOCAL
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . '.table');
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table', $downloadedFile, []);
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
                "destination" => self::OUTPUT_BUCKET . ".table",
                "columns" => ["Id", "Name"]
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables('upload', ["mapping" => $configs], ['componentId' => 'foo'], StrategyFactory::LOCAL);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-output-mapping-test.table', $tables[0]["id"]);
        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . ".table");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table', $downloadedFile, []);
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
            $root . DIRECTORY_SEPARATOR . 'upload/out.c-output-mapping-test.table.csv.manifest',
            '{"destination": "' . self::OUTPUT_BUCKET . '.table","primary_key":["Id","Name"],"columns":["Id","Name"]}'
        );

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables('upload', [], ['componentId' => 'foo'], StrategyFactory::LOCAL);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-output-mapping-test.table', $tables[0]["id"]);
        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . ".table");
        $this->assertEquals(["Id", "Name"], $table["primaryKey"]);
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv(file_get_contents($downloadedFile));
        $this->assertCount(1, $table);
        $this->assertEquals([["Id" => "test", "Name" => "test"]], $table);
    }

    public function testWriteTableOutputMappingExistingTable()
    {
        $csvFile = new CsvFile($this->tmp->createFile('header')->getPathname());
        $csvFile->writeRow(["Id", "Name"]);
        $this->clientWrapper->getBasicClient()->createTable(self::OUTPUT_BUCKET, "table", $csvFile);
        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        $this->assertCount(1, $tables);
        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . ".table");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table.csv", "\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                "source" => "table.csv",
                "destination" => self::OUTPUT_BUCKET . ".table",
                "columns" => ["Id","Name"]
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables('upload', ["mapping" => $configs], ['componentId' => 'foo'], StrategyFactory::LOCAL);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        $this->assertCount(1, $tables);
        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . ".table");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv(file_get_contents($downloadedFile));
        $this->assertCount(2, $table);
        $this->assertContains(["Id" => "test", "Name" => "test"], $table);
        $this->assertContains(["Id" => "aabb", "Name" => "ccdd"], $table);
    }
}
