<?php

namespace Keboola\OutputMapping\Tests;

use Keboola\Csv\CsvFile;
use Keboola\OutputMapping\Writer;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApi\TableExporter;
use Keboola\Temp\Temp;
use Keboola\Syrup\Exception\UserException;
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
        $this->client->createBucket('docker-test', 'out');
    }

    public function tearDown()
    {
        // Delete local files
        $this->tmp = null;

        $this->clearBucket();
        $this->clearFileUploads();
    }

    public function testWriteTableOutputMapping()
    {
        $root = $this->tmp->getTmpFolder();
        mkdir($root . "/upload/table.csv");
        file_put_contents($root . "/upload/table.csv/part1", "\"test\",\"test\"\n");
        file_put_contents($root . "/upload/table.csv/part2", "\"aabb\",\"ccdd\"\n");

        $configs = array(
            array(
                "source" => "table.csv",
                "destination" => "out.c-docker-test.table",
                "columns" => ["Id","Name"]
            )
        );

        $writer = new Writer($this->client, new NullLogger());

        $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);

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

    public function testWriteTableOutputMappingEmptyFile()
    {
        $root = $this->tmp->getTmpFolder();
        mkdir($root . "/upload/table");

        $configs = array(
            array(
                "source" => "table",
                "destination" => "out.c-docker-test.table",
                "columns" => ["Id","Name"]
            )
        );

        $writer = new Writer($this->client, new NullLogger());
        $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);

        $table = $this->client->getTable("out.c-docker-test.table");
        $this->assertEquals(["Id", "Name"], $table["columns"]);

        $exporter = new TableExporter($this->client);
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-docker-test.table', $downloadedFile, []);
        $table = $this->client->parseCsv(file_get_contents($downloadedFile));
        $this->assertCount(0, $table);
    }

    public function testWriteTableOutputMappingMissingHeaders()
    {
        $root = $this->tmp->getTmpFolder();
        mkdir($root . "/upload/table");

        $configs = array(
            array(
                "source" => "table",
                "destination" => "out.c-docker-test.table"
            )
        );

        $writer = new Writer($this->client, new NullLogger());
        try {
            $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);
            $this->fail("Exception not caught");
        } catch (UserException $e) {
            $this->assertEquals("Sliced file 'table': columns specification missing.", $e->getMessage());
        }
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
        mkdir($root . "/upload/table.csv");
        file_put_contents($root . "/upload/table.csv/part1", "\"test\",\"test\"\n");
        file_put_contents($root . "/upload/table.csv/part2", "\"aabb\",\"ccdd\"\n");

        $configs = array(
            array(
                "source" => "table.csv",
                "destination" => "out.c-docker-test.table",
                "columns" => ["Id","Name"]
            )
        );

        $writer = new Writer($this->client, new NullLogger());

        $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);

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


    public function testWriteTableOutputMappingDifferentDelimiterEnclosure()
    {
        $root = $this->tmp->getTmpFolder();
        mkdir($root . "/upload/table.csv");
        file_put_contents($root . "/upload/table.csv/part1", "'test'|'test'\n");
        file_put_contents($root . "/upload/table.csv/part2", "'aabb'|'ccdd'\n");

        $configs = array(
            array(
                "source" => "table.csv",
                "destination" => "out.c-docker-test.table",
                "columns" => ["Id","Name"],
                "delimiter" => "|",
                "enclosure" => "'"
            )
        );

        $writer = new Writer($this->client, new NullLogger());

        $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);

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


    public function testWriteTableOutputMappingCombination()
    {
        $root = $this->tmp->getTmpFolder();
        mkdir($root . "/upload/table.csv");
        file_put_contents($root . "/upload/table.csv/part1", "\"test\",\"test\"\n");
        file_put_contents($root . "/upload/table.csv/part2", "\"aabb\",\"ccdd\"\n");
        file_put_contents($root . "/upload/table2.csv", "\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = array(
            array(
                "source" => "table.csv",
                "destination" => "out.c-docker-test.table",
                "columns" => ["Id","Name"]
            ),
            array(
                "source" => "table2.csv",
                "destination" => "out.c-docker-test.table2",
                "columns" => ["Id","Name"]
            )
        );

        $writer = new Writer($this->client, new NullLogger());
        $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);

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

    public function testWriteTableOutputMappingCompression()
    {
        $root = $this->tmp->getTmpFolder();
        mkdir($root . "/upload/table.csv");
        file_put_contents($root . "/upload/table.csv/part1", "\"test\",\"test\"\n");
        file_put_contents($root . "/upload/table.csv/part2", "\"aabb\",\"ccdd\"\n");
        exec("gzip " . $root . "/upload/table.csv/part1");
        exec("gzip " . $root . "/upload/table.csv/part2");

        $configs = array(
            array(
                "source" => "table.csv",
                "destination" => "out.c-docker-test.table",
                "columns" => ["Id","Name"]
            )
        );

        $writer = new Writer($this->client, new NullLogger());

        $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);

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
