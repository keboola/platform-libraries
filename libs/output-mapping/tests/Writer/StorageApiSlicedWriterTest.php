<?php

namespace Keboola\OutputMapping\Tests;

use Keboola\Csv\CsvFile;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Tests\Writer\BaseWriterTest;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\TableExporter;

class StorageApiSlicedWriterTest extends BaseWriterTest
{
    public function setUp()
    {
        parent::setUp();
        $this->clearBuckets(['out.c-output-mapping-test']);
        $this->clearFileUploads(['output-mapping-test']);
    }

    public function initBucket($backendType)
    {
        $this->clientWrapper->getBasicClient()->createBucket('output-mapping-test', 'out', null, $backendType);
    }

    /**
     * @dataProvider backendTypeProvider
     */
    public function testWriteTableOutputMapping($backendType)
    {
        $this->initBucket($backendType);
        $root = $this->tmp->getTmpFolder();
        mkdir($root . "/upload/table.csv");
        file_put_contents($root . "/upload/table.csv/part1", "\"test\",\"test\"\n");
        file_put_contents($root . "/upload/table.csv/part2", "\"aabb\",\"ccdd\"\n");

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

    /**
     * @dataProvider backendTypeProvider
     */
    public function testWriteTableOutputMappingEmptySlice($backendType)
    {
        $this->initBucket($backendType);
        $root = $this->tmp->getTmpFolder();
        mkdir($root . "/upload/table");
        file_put_contents($root . "/upload/table/part1", "");
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

    /**
     * @dataProvider backendTypeProvider
     */
    public function testWriteTableOutputMappingEmptySliceExistingTable($backendType)
    {
        $this->initBucket($backendType);
        $fileName = $this->tmp->getTmpFolder() . uniqid('csv-');
        file_put_contents($fileName, "\"Id\",\"Name\"\n\"ab\",\"cd\"\n");
        $csv = new CsvFile($fileName);
        $this->clientWrapper->getBasicClient()->createTable('out.c-output-mapping-test', 'table16', $csv);

        $root = $this->tmp->getTmpFolder();
        mkdir($root . "/upload/table16");
        file_put_contents($root . "/upload/table16/part1", "");
        $configs = [
            [
                "source" => "table16",
                "destination" => "out.c-output-mapping-test.table16",
                "columns" => ["Id","Name"]
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables('upload', ["mapping" => $configs], ['componentId' => 'foo'], StrategyFactory::LOCAL);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $table = $this->clientWrapper->getBasicClient()->getTable("out.c-output-mapping-test.table16");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-output-mapping-test.table16', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv(file_get_contents($downloadedFile));
        $this->assertCount(0, $table);
    }

    /**
     * @dataProvider backendTypeProvider
     */
    public function testWriteTableOutputMappingEmptyDir($backendType)
    {
        $this->initBucket($backendType);
        $root = $this->tmp->getTmpFolder();
        mkdir($root . "/upload/table15");

        $configs = [
            [
                "source" => "table15",
                "destination" => "out.c-output-mapping-test.table15",
                "columns" => ["Id","Name"]
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables('upload', ["mapping" => $configs], ['componentId' => 'foo'], StrategyFactory::LOCAL);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $table = $this->clientWrapper->getBasicClient()->getTable("out.c-output-mapping-test.table15");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-output-mapping-test.table15', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv(file_get_contents($downloadedFile));
        $this->assertCount(0, $table);
    }

    /**
     * @dataProvider backendTypeProvider
     */
    public function testWriteTableOutputMappingEmptyDirExistingTable($backendType)
    {
        $this->initBucket($backendType);
        $fileName = $this->tmp->getTmpFolder() . uniqid('csv-');
        file_put_contents($fileName, "\"Id\",\"Name\"\n\"ab\",\"cd\"\n");
        $csv = new CsvFile($fileName);
        $this->clientWrapper->getBasicClient()->createTable('out.c-output-mapping-test', 'table17', $csv);

        $root = $this->tmp->getTmpFolder();
        mkdir($root . "/upload/table17");

        $configs = [
            [
                "source" => "table17",
                "destination" => "out.c-output-mapping-test.table17",
                "columns" => ["Id","Name"]
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables('upload', ["mapping" => $configs], ['componentId' => 'foo'], StrategyFactory::LOCAL);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $table = $this->clientWrapper->getBasicClient()->getTable("out.c-output-mapping-test.table17");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-output-mapping-test.table17', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv(file_get_contents($downloadedFile));
        $this->assertCount(0, $table);
    }

    /**
     * @dataProvider backendTypeProvider
     */
    public function testWriteTableOutputMappingMissingHeaders($backendType)
    {
        $this->initBucket($backendType);
        $root = $this->tmp->getTmpFolder();
        mkdir($root . "/upload/table");

        $configs = [
            [
                "source" => "table",
                "destination" => "out.c-output-mapping-test.table"
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());
        try {
            $writer->uploadTables('upload', ["mapping" => $configs], ['componentId' => 'foo'], StrategyFactory::LOCAL);
            $this->fail("Exception not caught");
        } catch (InvalidOutputException $e) {
            $this->assertEquals('Sliced file "table" columns specification missing.', $e->getMessage());
        }
    }

    /**
     * @dataProvider backendTypeProvider
     */
    public function testWriteTableOutputMappingExistingTable($backendType)
    {
        $this->initBucket($backendType);
        $csvFile = new CsvFile($this->tmp->createFile('header')->getPathname());
        $csvFile->writeRow(["Id", "Name"]);
        $this->clientWrapper->getBasicClient()->createTable("out.c-output-mapping-test", "table", $csvFile);
        $tables = $this->clientWrapper->getBasicClient()->listTables("out.c-output-mapping-test");
        $this->assertCount(1, $tables);
        $table = $this->clientWrapper->getBasicClient()->getTable("out.c-output-mapping-test.table");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $root = $this->tmp->getTmpFolder();
        mkdir($root . "/upload/table.csv");
        file_put_contents($root . "/upload/table.csv/part1", "\"test\",\"test\"\n");
        file_put_contents($root . "/upload/table.csv/part2", "\"aabb\",\"ccdd\"\n");

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

    /**
     * @dataProvider backendTypeProvider
     */
    public function testWriteTableOutputMappingDifferentDelimiterEnclosure($backendType)
    {
        $this->initBucket($backendType);
        $root = $this->tmp->getTmpFolder();
        mkdir($root . "/upload/table.csv");
        file_put_contents($root . "/upload/table.csv/part1", "'test'|'test'\n");
        file_put_contents($root . "/upload/table.csv/part2", "'aabb'|'ccdd'\n");

        $configs = [
            [
                "source" => "table.csv",
                "destination" => "out.c-output-mapping-test.table",
                "columns" => ["Id","Name"],
                "delimiter" => "|",
                "enclosure" => "'"
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

    /**
     * @dataProvider backendTypeProvider
     */
    public function testWriteTableOutputMappingCombination($backendType)
    {
        $this->initBucket($backendType);
        $root = $this->tmp->getTmpFolder();
        mkdir($root . "/upload/table.csv");
        file_put_contents($root . "/upload/table.csv/part1", "\"test\",\"test\"\n");
        file_put_contents($root . "/upload/table.csv/part2", "\"aabb\",\"ccdd\"\n");
        file_put_contents($root . "/upload/table2.csv", "\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                "source" => "table.csv",
                "destination" => "out.c-output-mapping-test.table",
                "columns" => ["Id","Name"]
            ],
            [
                "source" => "table2.csv",
                "destination" => "out.c-output-mapping-test.table2",
                "columns" => ["Id","Name"]
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables('upload', ["mapping" => $configs], ['componentId' => 'foo'], StrategyFactory::LOCAL);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(2, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables("out.c-output-mapping-test");
        $this->assertCount(2, $tables);
        $table = $this->clientWrapper->getBasicClient()->getTable("out.c-output-mapping-test.table");
        $this->assertEquals(["Id", "Name"], $table["columns"]);
        $table = $this->clientWrapper->getBasicClient()->getTable("out.c-output-mapping-test.table2");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-output-mapping-test.table', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv(file_get_contents($downloadedFile));
        $this->assertCount(2, $table);
        $this->assertContains(["Id" => "test", "Name" => "test"], $table);
        $this->assertContains(["Id" => "aabb", "Name" => "ccdd"], $table);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-output-mapping-test.table2', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv(file_get_contents($downloadedFile));
        $this->assertCount(2, $table);
        $this->assertContains(["Id" => "test", "Name" => "test"], $table);
        $this->assertContains(["Id" => "aabb", "Name" => "ccdd"], $table);
    }

    /**
     * @dataProvider backendTypeProvider
     */
    public function testWriteTableOutputMappingCompression($backendType)
    {
        $this->initBucket($backendType);
        $root = $this->tmp->getTmpFolder();
        mkdir($root . "/upload/table18.csv");
        file_put_contents($root . "/upload/table18.csv/part1", "\"test\",\"test\"\n");
        file_put_contents($root . "/upload/table18.csv/part2", "\"aabb\",\"ccdd\"\n");
        exec("gzip " . $root . "/upload/table18.csv/part1");
        exec("gzip " . $root . "/upload/table18.csv/part2");

        $configs = [
            [
                "source" => "table18.csv",
                "destination" => "out.c-output-mapping-test.table18",
                "columns" => ["Id","Name"]
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());

        $tableQueue =  $writer->uploadTables('upload', ["mapping" => $configs], ['componentId' => 'foo'], StrategyFactory::LOCAL);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables("out.c-output-mapping-test");
        $this->assertCount(1, $tables);
        $table = $this->clientWrapper->getBasicClient()->getTable("out.c-output-mapping-test.table18");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-output-mapping-test.table18', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv(file_get_contents($downloadedFile));
        $this->assertCount(2, $table);
        $this->assertContains(["Id" => "test", "Name" => "test"], $table);
        $this->assertContains(["Id" => "aabb", "Name" => "ccdd"], $table);
    }

    public function backendTypeProvider()
    {
        return [
            ["snowflake"],
            ["redshift"]
        ];
    }
}
