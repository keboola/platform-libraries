<?php

namespace Keboola\OutputMapping\Tests;

use Keboola\Csv\CsvFile;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApi\TableExporter;
use Keboola\Temp\Temp;
use Psr\Log\NullLogger;
use Symfony\Component\Filesystem\Filesystem;

class StorageApiHeadlessWriterTest extends \PHPUnit_Framework_TestCase
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
        parent::setUp();
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
        $this->client->createBucket('docker-test', 'out');
    }

    public function tearDown()
    {
        $this->clearBucket();
        $this->clearFileUploads();
    }

    public function testWriteTableOutputMapping()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table.csv", "\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                "source" => "table.csv",
                "destination" => "out.c-docker-test.table",
                "columns" => ["Id","Name"]
            ]
        ];

        $writer = new TableWriter($this->client, new NullLogger());

        $tableQueue =  $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->client->listTables("out.c-docker-test");
        $this->assertCount(1, $tables);
        $table = $this->client->getTable("out.c-docker-test.table");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->client);
        $downloadedFile = $root . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-docker-test.table', $downloadedFile, []);
        $table = $this->client->parseCsv(file_get_contents($downloadedFile));
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
                "destination" => "out.c-docker-test.table",
                "columns" => ["Id","Name"]
            ]
        ];

        $writer = new TableWriter($this->client, new NullLogger());
        $tableQueue =  $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $table = $this->client->getTable("out.c-docker-test.table");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->client);
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-docker-test.table', $downloadedFile, []);
        $table = $this->client->parseCsv(file_get_contents($downloadedFile));
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
            "{\"destination\": \"out.c-docker-test.table2\",\"primary_key\":[\"Id\"],\"columns\":[\"a\",\"b\"]}"
        );

        $configs = [
            [
                "source" => "table2.csv",
                "destination" => "out.c-docker-test.table",
                "columns" => ["Id", "Name"]
            ]
        ];

        $writer = new TableWriter($this->client, new NullLogger());

        $tableQueue =  $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->client->listTables("out.c-docker-test");
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-docker-test.table', $tables[0]["id"]);
        $table = $this->client->getTable("out.c-docker-test.table");
        $this->assertEquals([], $table["primaryKey"]);
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->client);
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-docker-test.table', $downloadedFile, []);
        $table = $this->client->parseCsv(file_get_contents($downloadedFile));
        $this->assertCount(1, $table);
        $this->assertEquals([["Id" => "test", "Name" => "test"]], $table);
    }

    public function testWriteTableManifest()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/out.c-docker-test.table.csv",
            "\"test\",\"test\"\n"
        );
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/out.c-docker-test.table.csv.manifest",
            "{\"destination\": \"out.c-docker-test.table\",\"primary_key\":[\"Id\",\"Name\"],\"columns\":[\"Id\",\"Name\"]}"
        );

        $writer = new TableWriter($this->client, new NullLogger());

        $tableQueue =  $writer->uploadTables($root . "/upload", [], ['componentId' => 'foo']);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->client->listTables("out.c-docker-test");
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-docker-test.table', $tables[0]["id"]);
        $table = $this->client->getTable("out.c-docker-test.table");
        $this->assertEquals(["Id", "Name"], $table["primaryKey"]);
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->client);
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-docker-test.table', $downloadedFile, []);
        $table = $this->client->parseCsv(file_get_contents($downloadedFile));
        $this->assertCount(1, $table);
        $this->assertEquals([["Id" => "test", "Name" => "test"]], $table);
    }

    public function testWriteTableOutputMappingExistingTable()
    {
        $csvFile = new CsvFile($this->tmp->createFile('header')->getPathname());
        $csvFile->writeRow(["Id", "Name"]);
        $this->client->createTable("out.c-docker-test", "table", $csvFile);
        $tables = $this->client->listTables("out.c-docker-test");
        $this->assertCount(1, $tables);
        $table = $this->client->getTable("out.c-docker-test.table");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table.csv", "\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                "source" => "table.csv",
                "destination" => "out.c-docker-test.table",
                "columns" => ["Id","Name"]
            ]
        ];

        $writer = new TableWriter($this->client, new NullLogger());

        $tableQueue =  $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);
        $jobIds = $tableQueue->waitForAll();
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
}
