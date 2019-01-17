<?php

namespace Keboola\OutputMapping\Tests;

use Keboola\Csv\CsvFile;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Writer;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApi\TableExporter;
use Keboola\Temp\Temp;
use Psr\Log\NullLogger;
use Symfony\Component\Filesystem\Filesystem;

class StorageApiSlicedWriterTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var Client
     */
    protected $client;

    /**
     * @var Temp
     */
    private $tmp;

    protected function clearBucket()
    {
        foreach (['out.c-docker-test'] as $bucket) {
            try {
                $this->client->dropBucket($bucket, ['force' => true]);
            } catch (ClientException $e) {
                if ($e->getCode() != 404) {
                    throw $e;
                }
            }
        }
    }

    protected function clearFileUploads()
    {
        // Delete file uploads
        $options = new ListFilesOptions();
        $options->setTags(['docker-bundle-test']);
        $files = $this->client->listFiles($options);
        foreach ($files as $file) {
            $this->client->deleteFile($file['id']);
        }
    }

    public function setUp()
    {
        // Create folders
        $this->tmp = new Temp();
        $this->tmp->initRunFolder();
        $root = $this->tmp->getTmpFolder();
        $fs = new Filesystem();
        $fs->mkdir($root . DIRECTORY_SEPARATOR . 'upload');
        $fs->mkdir($root . DIRECTORY_SEPARATOR . 'download');

        $this->client = new Client([
            'url' => STORAGE_API_URL,
            'token' => STORAGE_API_TOKEN,
        ]);
        $this->clearBucket();
        $this->clearFileUploads();
    }

    public function initBucket($backendType)
    {
        $this->client->createBucket('docker-test', 'out', null, $backendType);
    }

    public function tearDown()
    {
        $this->clearBucket();
        $this->clearFileUploads();
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
                "destination" => "out.c-docker-test.table",
                "columns" => ["Id","Name"]
            ]
        ];

        $writer = new Writer($this->client, new NullLogger());
        $job = $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);
        $jobIds = $job->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->client->listTables("out.c-docker-test");
        $this->assertCount(1, $tables);
        $table = $this->client->getTable("out.c-docker-test.table");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->client);
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-docker-test.table', $downloadedFile, []);
        $table = $this->client->parseCsv(file_get_contents($downloadedFile));
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
                "destination" => "out.c-docker-test.table",
                "columns" => ["Id","Name"]
            ]
        ];

        $writer = new Writer($this->client, new NullLogger());
        $job = $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);
        $jobIds = $job->waitForAll();
        $this->assertCount(1, $jobIds);

        $table = $this->client->getTable("out.c-docker-test.table");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->client);
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-docker-test.table', $downloadedFile, []);
        $table = $this->client->parseCsv(file_get_contents($downloadedFile));
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
        $this->client->createTable('out.c-docker-test', 'table16', $csv);

        $root = $this->tmp->getTmpFolder();
        mkdir($root . "/upload/table16");
        file_put_contents($root . "/upload/table16/part1", "");
        $configs = [
            [
                "source" => "table16",
                "destination" => "out.c-docker-test.table16",
                "columns" => ["Id","Name"]
            ]
        ];

        $writer = new Writer($this->client, new NullLogger());
        $job = $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);
        $jobIds = $job->waitForAll();
        $this->assertCount(1, $jobIds);

        $table = $this->client->getTable("out.c-docker-test.table16");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->client);
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-docker-test.table16', $downloadedFile, []);
        $table = $this->client->parseCsv(file_get_contents($downloadedFile));
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
                "destination" => "out.c-docker-test.table15",
                "columns" => ["Id","Name"]
            ]
        ];

        $writer = new Writer($this->client, new NullLogger());
        $job = $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);
        $jobIds = $job->waitForAll();
        $this->assertCount(1, $jobIds);

        $table = $this->client->getTable("out.c-docker-test.table15");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->client);
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-docker-test.table15', $downloadedFile, []);
        $table = $this->client->parseCsv(file_get_contents($downloadedFile));
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
        $this->client->createTable('out.c-docker-test', 'table17', $csv);

        $root = $this->tmp->getTmpFolder();
        mkdir($root . "/upload/table17");

        $configs = [
            [
                "source" => "table17",
                "destination" => "out.c-docker-test.table17",
                "columns" => ["Id","Name"]
            ]
        ];

        $writer = new Writer($this->client, new NullLogger());
        $job = $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);
        $jobIds = $job->waitForAll();
        $this->assertCount(1, $jobIds);

        $table = $this->client->getTable("out.c-docker-test.table17");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->client);
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-docker-test.table17', $downloadedFile, []);
        $table = $this->client->parseCsv(file_get_contents($downloadedFile));
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
                "destination" => "out.c-docker-test.table"
            ]
        ];

        $writer = new Writer($this->client, new NullLogger());
        try {
            $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);
            $this->fail("Exception not caught");
        } catch (InvalidOutputException $e) {
            $this->assertEquals("Sliced file 'table': columns specification missing.", $e->getMessage());
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
        $this->client->createTable("out.c-docker-test", "table", $csvFile);
        $tables = $this->client->listTables("out.c-docker-test");
        $this->assertCount(1, $tables);
        $table = $this->client->getTable("out.c-docker-test.table");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $root = $this->tmp->getTmpFolder();
        mkdir($root . "/upload/table.csv");
        file_put_contents($root . "/upload/table.csv/part1", "\"test\",\"test\"\n");
        file_put_contents($root . "/upload/table.csv/part2", "\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                "source" => "table.csv",
                "destination" => "out.c-docker-test.table",
                "columns" => ["Id","Name"]
            ]
        ];

        $writer = new Writer($this->client, new NullLogger());

        $job = $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);
        $jobIds = $job->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->client->listTables("out.c-docker-test");
        $this->assertCount(1, $tables);
        $table = $this->client->getTable("out.c-docker-test.table");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->client);
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-docker-test.table', $downloadedFile, []);
        $table = $this->client->parseCsv(file_get_contents($downloadedFile));
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
                "destination" => "out.c-docker-test.table",
                "columns" => ["Id","Name"],
                "delimiter" => "|",
                "enclosure" => "'"
            ]
        ];

        $writer = new Writer($this->client, new NullLogger());

        $job = $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);
        $jobIds = $job->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->client->listTables("out.c-docker-test");
        $this->assertCount(1, $tables);
        $table = $this->client->getTable("out.c-docker-test.table");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->client);
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-docker-test.table', $downloadedFile, []);
        $table = $this->client->parseCsv(file_get_contents($downloadedFile));
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
                "destination" => "out.c-docker-test.table",
                "columns" => ["Id","Name"]
            ],
            [
                "source" => "table2.csv",
                "destination" => "out.c-docker-test.table2",
                "columns" => ["Id","Name"]
            ]
        ];

        $writer = new Writer($this->client, new NullLogger());
        $job = $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);
        $jobIds = $job->waitForAll();
        $this->assertCount(2, $jobIds);

        $tables = $this->client->listTables("out.c-docker-test");
        $this->assertCount(2, $tables);
        $table = $this->client->getTable("out.c-docker-test.table");
        $this->assertEquals(["Id", "Name"], $table["columns"]);
        $table = $this->client->getTable("out.c-docker-test.table2");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->client);
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-docker-test.table', $downloadedFile, []);
        $table = $this->client->parseCsv(file_get_contents($downloadedFile));
        $this->assertCount(2, $table);
        $this->assertContains(["Id" => "test", "Name" => "test"], $table);
        $this->assertContains(["Id" => "aabb", "Name" => "ccdd"], $table);

        $exporter = new TableExporter($this->client);
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-docker-test.table2', $downloadedFile, []);
        $table = $this->client->parseCsv(file_get_contents($downloadedFile));
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
                "destination" => "out.c-docker-test.table18",
                "columns" => ["Id","Name"]
            ]
        ];

        $writer = new Writer($this->client, new NullLogger());

        $job = $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);
        $jobIds = $job->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->client->listTables("out.c-docker-test");
        $this->assertCount(1, $tables);
        $table = $this->client->getTable("out.c-docker-test.table18");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->client);
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-docker-test.table18', $downloadedFile, []);
        $table = $this->client->parseCsv(file_get_contents($downloadedFile));
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
