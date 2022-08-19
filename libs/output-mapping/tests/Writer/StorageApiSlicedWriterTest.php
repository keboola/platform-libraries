<?php

namespace Keboola\OutputMapping\Tests;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Staging\NullProvider;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Tests\Writer\BaseWriterTest;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\TableExporter;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\NullLogger;

class StorageApiSlicedWriterTest extends BaseWriterTest
{
    private const OUTPUT_BUCKET = 'out.c-' . self::class;
    private const FILE_TAG = self::class;

    public function setUp()
    {
        parent::setUp();
        $this->clearBuckets([self::OUTPUT_BUCKET]);
        $this->clearFileUploads([self::FILE_TAG]);
    }

    public function initBucket($backendType)
    {
        $this->clientWrapper->getBasicClient()->createBucket(self::class, 'out', null, $backendType);
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

        $job = $this->clientWrapper->getBasicClient()->getJob($jobIds[0]);
        $fileId = $job['operationParams']['source']['fileId'];
        $file = $this->clientWrapper->getBasicClient()->getFile($fileId);
        self::assertEquals([], $file['tags']);
    }

    public function testWriteTableTagStagingFile()
    {
        $this->initBucket('snowflake');
        $root = $this->tmp->getTmpFolder();
        mkdir($root . "/upload/table.csv");
        file_put_contents($root . "/upload/table.csv/part1", "\"test\",\"test\"\n");
        file_put_contents($root . "/upload/table.csv/part2", "\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                "source" => "table.csv",
                "destination" => self::OUTPUT_BUCKET . ".table",
                "columns" => ["Id","Name"]
            ]
        ];

        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        $client = $this->getMockBuilder(Client::class)
            ->setConstructorArgs([[
                'url' => STORAGE_API_URL,
                'token' => STORAGE_API_TOKEN,
            ]])
            ->setMethods(['verifyToken'])
            ->getMock();
        $tokenInfo['owner']['features'][] = 'tag-staging-files';
        $client->method('verifyToken')->willReturn($tokenInfo);
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getBasicClient')->willReturn($client);
        $writer = new TableWriter($this->getStagingFactory($clientWrapper));

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

        $job = $this->clientWrapper->getBasicClient()->getJob($jobIds[0]);
        $fileId = $job['operationParams']['source']['fileId'];
        $file = $this->clientWrapper->getBasicClient()->getFile($fileId);
        self::assertEquals(
            ['componentId: foo'],
            $file['tags']
        );
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
                "destination" => self::OUTPUT_BUCKET . ".table",
                "columns" => ["Id","Name"]
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables('upload', ["mapping" => $configs], ['componentId' => 'foo'], StrategyFactory::LOCAL);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . ".table");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table', $downloadedFile, []);
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
        $this->clientWrapper->getBasicClient()->createTable(self::OUTPUT_BUCKET, 'table16', $csv);

        $root = $this->tmp->getTmpFolder();
        mkdir($root . "/upload/table16");
        file_put_contents($root . "/upload/table16/part1", "");
        $configs = [
            [
                "source" => "table16",
                "destination" => self::OUTPUT_BUCKET . ".table16",
                "columns" => ["Id","Name"]
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables('upload', ["mapping" => $configs], ['componentId' => 'foo'], StrategyFactory::LOCAL);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . ".table16");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table16', $downloadedFile, []);
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
                "destination" => self::OUTPUT_BUCKET . ".table15",
                "columns" => ["Id","Name"]
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables('upload', ["mapping" => $configs], ['componentId' => 'foo'], StrategyFactory::LOCAL);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . ".table15");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table15', $downloadedFile, []);
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
        $this->clientWrapper->getBasicClient()->createTable(self::OUTPUT_BUCKET, 'table17', $csv);

        $root = $this->tmp->getTmpFolder();
        mkdir($root . "/upload/table17");

        $configs = [
            [
                "source" => "table17",
                "destination" => self::OUTPUT_BUCKET . ".table17",
                "columns" => ["Id","Name"]
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables('upload', ["mapping" => $configs], ['componentId' => 'foo'], StrategyFactory::LOCAL);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . ".table17");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table17', $downloadedFile, []);
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
                "destination" => self::OUTPUT_BUCKET . ".table"
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
        $this->clientWrapper->getBasicClient()->createTable(self::OUTPUT_BUCKET, "table", $csvFile);
        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        $this->assertCount(1, $tables);
        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . ".table");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $root = $this->tmp->getTmpFolder();
        mkdir($root . "/upload/table.csv");
        file_put_contents($root . "/upload/table.csv/part1", "\"test\",\"test\"\n");
        file_put_contents($root . "/upload/table.csv/part2", "\"aabb\",\"ccdd\"\n");

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
                "destination" => self::OUTPUT_BUCKET . ".table",
                "columns" => ["Id","Name"],
                "delimiter" => "|",
                "enclosure" => "'"
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
                "destination" => self::OUTPUT_BUCKET . ".table",
                "columns" => ["Id","Name"]
            ],
            [
                "source" => "table2.csv",
                "destination" => self::OUTPUT_BUCKET . ".table2",
                "columns" => ["Id","Name"]
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables('upload', ["mapping" => $configs], ['componentId' => 'foo'], StrategyFactory::LOCAL);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(2, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        $this->assertCount(2, $tables);
        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . ".table");
        $this->assertEquals(["Id", "Name"], $table["columns"]);
        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . ".table2");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv(file_get_contents($downloadedFile));
        $this->assertCount(2, $table);
        $this->assertContains(["Id" => "test", "Name" => "test"], $table);
        $this->assertContains(["Id" => "aabb", "Name" => "ccdd"], $table);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table2', $downloadedFile, []);
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
                "destination" => self::OUTPUT_BUCKET . ".table18",
                "columns" => ["Id","Name"]
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());

        $tableQueue =  $writer->uploadTables('upload', ["mapping" => $configs], ['componentId' => 'foo'], StrategyFactory::LOCAL);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        $this->assertCount(1, $tables);
        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . ".table18");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table18', $downloadedFile, []);
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
