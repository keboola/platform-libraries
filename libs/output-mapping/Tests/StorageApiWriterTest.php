<?php

namespace Keboola\OutputMapping\Tests;

use Keboola\OutputMapping\Writer;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApi\TableExporter;
use Keboola\Syrup\Exception\ApplicationException;
use Keboola\Temp\Temp;
use Keboola\Syrup\Exception\UserException;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use Psr\Log\NullLogger;
use Symfony\Component\Filesystem\Filesystem;

class StorageApiWriterTest extends \PHPUnit_Framework_TestCase
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
        foreach (['out.c-docker-test', 'out.c-docker-default-test', 'out.c-docker-redshift-test', 'in.c-docker-test'] as $bucket) {
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
        $this->client->createBucket('docker-redshift-test', 'out', '', 'redshift');

        $this->client->createBucket('docker-default-test', 'out');
    }

    public function tearDown()
    {
        // Delete local files
        $this->tmp = null;

        $this->clearBucket();
        $this->clearFileUploads();
    }

    public function testWriteFiles()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/file1", "test");
        file_put_contents($root . "/upload/file2", "test");
        file_put_contents(
            $root . "/upload/file2.manifest",
            "{\"tags\": [\"docker-bundle-test\", \"xxx\"],\"is_public\": false}"
        );
        file_put_contents($root . "/upload/file3", "test");
        file_put_contents($root . "/upload/file3.manifest", "{\"tags\": [\"docker-bundle-test\"],\"is_public\": true}");

        $configs = array(
            array(
                "source" => "file1",
                "tags" => array("docker-bundle-test")
            ),
            array(
                "source" => "file2",
                "tags" => array("docker-bundle-test", "another-tag"),
                "is_public" => true
            )
        );

        $writer = new Writer($this->client, new NullLogger());

        $writer->uploadFiles($root . "/upload", ["mapping" => $configs]);

        $options = new ListFilesOptions();
        $options->setTags(array("docker-bundle-test"));
        $files = $this->client->listFiles($options);
        $this->assertCount(3, $files);

        $file1 = $file2 = $file3 = null;
        foreach ($files as $file) {
            if ($file["name"] == 'file1') {
                $file1 = $file;
            }
            if ($file["name"] == 'file2') {
                $file2 = $file;
            }
            if ($file["name"] == 'file3') {
                $file3 = $file;
            }
        }

        $this->assertNotNull($file1);
        $this->assertNotNull($file2);
        $this->assertNotNull($file3);
        $this->assertEquals(4, $file1["sizeBytes"]);
        $this->assertEquals(array("docker-bundle-test"), $file1["tags"]);
        $this->assertEquals(array("docker-bundle-test", "another-tag"), $file2["tags"]);
        $this->assertEquals(array("docker-bundle-test"), $file3["tags"]);
        $this->assertFalse($file1["isPublic"]);
        $this->assertTrue($file2["isPublic"]);
        $this->assertTrue($file3["isPublic"]);
    }

    public function testWriteFilesOutputMapping()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/file1", "test");

        $configs = array(
            array(
                "source" => "file1",
                "tags" => array("docker-bundle-test")
            )
        );

        $writer = new Writer($this->client, new NullLogger());

        $writer->uploadFiles($root . "/upload", ["mapping" => $configs]);

        $options = new ListFilesOptions();
        $options->setTags(array("docker-bundle-test"));
        $files = $this->client->listFiles($options);
        $this->assertCount(1, $files);

        $file1 = null;
        foreach ($files as $file) {
            if ($file["name"] == 'file1') {
                $file1 = $file;
            }
        }

        $this->assertNotNull($file1);
        $this->assertEquals(4, $file1["sizeBytes"]);
        $this->assertEquals(array("docker-bundle-test"), $file1["tags"]);
    }

    public function testWriteFilesOutputMappingAndManifest()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/file1", "test");
        file_put_contents($root . "/upload/file1.manifest", "{\"tags\": [\"docker-bundle-test\", \"xxx\"],\"is_public\": true}");

        $configs = array(
            array(
                "source" => "file1",
                "tags" => array("docker-bundle-test", "yyy"),
                "is_public" => false
            )
        );

        $writer = new Writer($this->client, new NullLogger());
        $writer->uploadFiles($root . "/upload", ["mapping" => $configs]);

        $options = new ListFilesOptions();
        $options->setTags(array("docker-bundle-test"));
        $files = $this->client->listFiles($options);
        $this->assertCount(1, $files);

        $file1 = null;
        foreach ($files as $file) {
            if ($file["name"] == 'file1') {
                $file1 = $file;
            }
        }

        $this->assertNotNull($file1);
        $this->assertEquals(4, $file1["sizeBytes"]);
        $this->assertEquals(array("docker-bundle-test", "yyy"), $file1["tags"]);
        $this->assertFalse($file1["isPublic"]);
    }

    public function testWriteFilesInvalidJson()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/file1", "test");
        file_put_contents($root . "/upload/file1.manifest", "this is not at all a {valid} json");

        $configs = array(
            array(
                "source" => "file1",
                "tags" => array("docker-bundle-test", "yyy"),
                "is_public" => false
            )
        );

        $writer = new Writer($this->client, new NullLogger());
        $writer->setFormat('json');
        try {
            $writer->uploadFiles($root . "/upload", ["mapping" => $configs]);
            $this->fail("Invalid manifest must raise exception.");
        } catch (UserException $e) {
            $this->assertContains('json', $e->getMessage());
        }
    }

    public function testWriteFilesInvalidYaml()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/file1", "test");
        file_put_contents($root . "/upload/file1.manifest", "\tthis is not \n\t \tat all a {valid} json");

        $configs = array(
            array(
                "source" => "file1",
                "tags" => array("docker-bundle-test", "yyy"),
                "is_public" => false
            )
        );

        $writer = new Writer($this->client, new NullLogger());
        $writer->setFormat('json');
        try {
            $writer->uploadFiles($root . "/upload", ["mapping" => $configs]);
            $this->fail("Invalid manifest must raise exception.");
        } catch (UserException $e) {
            $this->assertContains('json', $e->getMessage());
        }
    }

    /**
     * @expectedException \Keboola\DockerBundle\Exception\MissingFileException
     * @expectedExceptionMessage File 'file2' not found
     */
    public function testWriteFilesOutputMappingMissing()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/file1", "test");
        file_put_contents($root . "/upload/file1.manifest", "{\"tags\": [\"docker-bundle-test-xxx\"],\"is_public\": true}");

        $configs = array(
            array(
                "source" => "file2",
                "tags" => array("docker-bundle-test"),
                "is_public" => false
            )
        );
        $writer = new Writer($this->client, new NullLogger());
        $writer->uploadFiles($root . "/upload", ["mapping" => $configs]);
    }

    /**
     * @expectedException \Keboola\DockerBundle\Exception\ManifestMismatchException
     * @expectedExceptionMessage Found orphaned file manifest: 'file1.manifest'
     */
    public function testWriteFilesOrphanedManifest()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/file1.manifest", "{\"tags\": [\"docker-bundle-test-xxx\"],\"is_public\": true}");
        $writer = new Writer($this->client, new NullLogger());
        $writer->uploadFiles($root . "/upload");
    }

    public function testWriteTableOutputMapping()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table1.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = array(
            array(
                "source" => "table1.csv",
                "destination" => "out.c-docker-test.table1"
            )
        );

        $writer = new Writer($this->client, new NullLogger());

        $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);

        $tables = $this->client->listTables("out.c-docker-test");
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-docker-test.table1', $tables[0]["id"]);
    }

    public function testWriteTableOutputMappingWithoutCsv()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table1", "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = array(
            array(
                "source" => "table1",
                "destination" => "out.c-docker-test.table1"
            )
        );

        $writer = new Writer($this->client, new NullLogger());

        $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);

        $tables = $this->client->listTables("out.c-docker-test");
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-docker-test.table1', $tables[0]["id"]);
    }

    public function testWriteTableOutputMappingEmptyFile()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table1", "");

        $configs = array(
            array(
                "source" => "table1",
                "destination" => "out.c-docker-test.table1"
            )
        );

        $writer = new Writer($this->client, new NullLogger());

        try {
            $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);
            $this->fail("Empty CSV file must fail");
        } catch (UserException $e) {
            $this->assertContains('no data in import file', $e->getMessage());
        }
    }

    public function testWriteTableOutputMappingAndManifest()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/table2.csv",
            "\"Id\",\"Name\"\n\"test\",\"test\"\n"
        );
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/table2.csv.manifest",
            "{\"destination\": \"out.c-docker-test.table2\",\"primary_key\": [\"Id\"]}"
        );

        $configs = array(
            array(
                "source" => "table2.csv",
                "destination" => "out.c-docker-test.table"
            )
        );

        $writer = new Writer($this->client, new NullLogger());

        $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);

        $tables = $this->client->listTables("out.c-docker-test");
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-docker-test.table', $tables[0]["id"]);
        $this->assertEquals(array(), $tables[0]["primaryKey"]);
    }

    public function testWriteTableManifest()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/out.c-docker-test.table3.csv",
            "\"Id\",\"Name\"\n\"test\",\"test\"\n"
        );
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/out.c-docker-test.table3.csv.manifest",
            "{\"destination\": \"out.c-docker-test.table3\",\"primary_key\": [\"Id\",\"Name\"]}"
        );

        $writer = new Writer($this->client, new NullLogger());

        $writer->uploadTables($root . "/upload", [], ['componentId' => 'foo']);

        $tables = $this->client->listTables("out.c-docker-test");
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-docker-test.table3', $tables[0]["id"]);
        $this->assertEquals(array("Id", "Name"), $tables[0]["primaryKey"]);
    }

    public function testWriteTableZipManifest()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/out.c-docker-test.table3.csv",
            "\"Id\",\"Name\"\n\"test\",\"test\"\n"
        );
        exec('gzip ' . $root . DIRECTORY_SEPARATOR . "upload/out.c-docker-test.table3.csv");
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/out.c-docker-test.table3.csv.gz.manifest",
            "{\"destination\": \"out.c-docker-test.table3\",\"primary_key\": [\"Id\",\"Name\"]}"
        );

        $writer = new Writer($this->client, new NullLogger());

        $writer->uploadTables($root . "/upload", [], ['componentId' => 'foo']);

        $tables = $this->client->listTables("out.c-docker-test");
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-docker-test.table3', $tables[0]["id"]);
        $this->assertEquals(array("Id", "Name"), $tables[0]["primaryKey"]);
    }

    public function testWriteTableInvalidManifest()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/out.c-docker-test.table3.csv",
            "\"Id\",\"Name\"\n\"test\",\"test\"\n"
        );
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/out.c-docker-test.table3.csv.manifest",
            "{\"destination\": \"out.c-docker-test.table3\",\"primary_key\": \"Id\"}"
        );

        $writer = new Writer($this->client, new NullLogger());
        try {
            $writer->uploadTables($root . "/upload", [], ['componentId' => 'foo']);
            $this->fail('Invalid table manifest must cause exception');
        } catch (UserException $e) {
            $this->assertContains('Invalid type for path', $e->getMessage());
        }
    }

    public function testWriteTableManifestCsvDefaultBackend()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/out.c-docker-default-test.table3.csv",
            "'Id'\t'Name'\n'test'\t'test''s'\n"
        );
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/out.c-docker-default-test.table3.csv.manifest",
            "{\"destination\": \"out.c-docker-default-test.table3\",\"delimiter\": \"\\t\",\"enclosure\": \"'\"}"
        );

        $writer = new Writer($this->client, new NullLogger());

        $writer->uploadTables($root . "/upload", [], ['componentId' => 'foo']);

        $tables = $this->client->listTables("out.c-docker-default-test");
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-docker-default-test.table3', $tables[0]["id"]);
        $exporter = new TableExporter($this->client);
        $downloadedFile = $root . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-docker-default-test.table3', $downloadedFile, []);
        $table = $this->client->parseCsv(file_get_contents($downloadedFile));
        $this->assertEquals(1, count($table));
        $this->assertEquals(2, count($table[0]));
        $this->assertArrayHasKey('Id', $table[0]);
        $this->assertArrayHasKey('Name', $table[0]);
        $this->assertEquals('test', $table[0]['Id']);
        $this->assertEquals('test\'s', $table[0]['Name']);
    }

    public function testWriteTableManifestCsvRedshift()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/out.c-docker-redshift-test.table3.csv",
            "'Id'\t'Name'\n'test'\t'test''s'\n"
        );
        // TODO: remove the escaped_by parameter as soon as it is removed from manifest
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/out.c-docker-redshift-test.table3.csv.manifest",
            "{\"destination\": \"out.c-docker-redshift-test.table3\",\"delimiter\": \"\\t\",\"enclosure\": \"'\",\"escaped_by\": \"\\\\\"}"
        );

        $writer = new Writer($this->client, new NullLogger());

        $writer->uploadTables($root . "/upload", [], ['componentId' => 'foo']);

        $tables = $this->client->listTables("out.c-docker-redshift-test");
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-docker-redshift-test.table3', $tables[0]["id"]);
        $exporter = new TableExporter($this->client);
        $downloadedFile = $root . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-docker-redshift-test.table3', $downloadedFile, []);
        $table = $this->client->parseCsv(file_get_contents($downloadedFile));
        $this->assertEquals(1, count($table));
        $this->assertEquals(2, count($table[0]));
        $this->assertArrayHasKey('Id', $table[0]);
        $this->assertArrayHasKey('Name', $table[0]);
        $this->assertEquals('test', $table[0]['Id']);
        $this->assertEquals('test\'s', $table[0]['Name']);
    }

    /**
     * @expectedException \Keboola\DockerBundle\Exception\ManifestMismatchException
     * @expectedExceptionMessage Found orphaned table manifest: 'table.csv.manifest'
     */
    public function testWriteTableOrphanedManifest()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/table.csv.manifest",
            "{\"destination\": \"out.c-docker-test.table3\",\"primary_key\": [\"Id\", \"Name\"]}"
        );
        $writer = new Writer($this->client, new NullLogger());
        $writer->uploadTables($root . "/upload", [], ['componentId' => 'foo']);
    }


    /**
     * @expectedException \Keboola\DockerBundle\Exception\MissingFileException
     * @expectedExceptionMessage Table source 'table1.csv' not found
     */
    public function testWriteTableOutputMappingMissing()
    {
        $root = $this->tmp->getTmpFolder();

        $configs = array(
            array(
                "source" => "table1.csv",
                "destination" => "out.c-docker-test.table1"
            )
        );
        $writer = new Writer($this->client, new NullLogger());
        $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);
    }

    public function testWriteTableMetadataMissing()
    {
        $root = $this->tmp->getTmpFolder();

        $writer = new Writer($this->client, new NullLogger());
        try {
            $writer->uploadTables($root . "/upload", [], []);
            self::fail("Missing metadata must fail.");
        } catch (ApplicationException $e) {
            self::assertContains('Component Id must be set', $e->getMessage());
        }
    }

    public function testWriteTableBare()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/out.c-docker-test.table4.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $writer = new Writer($this->client, new NullLogger());

        $writer->uploadTables($root . "/upload", [], ['componentId' => 'foo']);

        $tables = $this->client->listTables("out.c-docker-test");
        $this->assertCount(1, $tables);

        $this->assertEquals('out.c-docker-test.table4', $tables[0]["id"]);
        $tableInfo = $this->client->getTable('out.c-docker-test.table4');
        $this->assertEquals(array("Id", "Name"), $tableInfo["columns"]);
    }

    public function testWriteTableBareWithoutSuffix()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/out.c-docker-test.table4", "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $writer = new Writer($this->client, new NullLogger());

        $writer->uploadTables($root . "/upload", [], ['componentId' => 'foo']);

        $tables = $this->client->listTables("out.c-docker-test");
        $this->assertCount(1, $tables);

        $this->assertEquals('out.c-docker-test.table4', $tables[0]["id"]);
        $tableInfo = $this->client->getTable('out.c-docker-test.table4');
        $this->assertEquals(array("Id", "Name"), $tableInfo["columns"]);
    }

    public function testWriteTableIncrementalWithDeleteDefault()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table1.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = array(
            array(
                "source" => "table1.csv",
                "destination" => "out.c-docker-default-test.table1",
                "delete_where_column" => "Id",
                "delete_where_values" => array("aabb"),
                "delete_where_operator" => "eq",
                "incremental" => true
            )
        );

        $writer = new Writer($this->client, new NullLogger());

        $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);

        // And again, check first incremental table
        $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);
        $this->client->exportTable("out.c-docker-default-test.table1", $root . DIRECTORY_SEPARATOR . "download.csv");
        $table = $this->client->parseCsv(file_get_contents($root . DIRECTORY_SEPARATOR . "download.csv"));
        usort($table, function ($a, $b) {
            return strcasecmp($a['Id'], $b['Id']);
        });
        $this->assertEquals(3, count($table));
        $this->assertEquals(2, count($table[0]));
        $this->assertArrayHasKey('Id', $table[0]);
        $this->assertArrayHasKey('Name', $table[0]);
        $this->assertEquals('aabb', $table[0]['Id']);
        $this->assertEquals('ccdd', $table[0]['Name']);
        $this->assertEquals('test', $table[1]['Id']);
        $this->assertEquals('test', $table[1]['Name']);
        $this->assertEquals('test', $table[2]['Id']);
        $this->assertEquals('test', $table[2]['Name']);
    }

    public function testWriteTableIncrementalWithDeleteRedshift()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table1.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = array(
            array(
                "source" => "table1.csv",
                "destination" => "out.c-docker-redshift-test.table1",
                "delete_where_column" => "Id",
                "delete_where_values" => array("aabb"),
                "delete_where_operator" => "eq",
                "incremental" => true
            )
        );

        $writer = new Writer($this->client, new NullLogger());

        $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);

        // And again, check first incremental table
        $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo']);
        $this->client->exportTable("out.c-docker-redshift-test.table1", $root . DIRECTORY_SEPARATOR . "download.csv");
        $table = $this->client->parseCsv(file_get_contents($root . DIRECTORY_SEPARATOR . "download.csv"));
        usort($table, function ($a, $b) {
            return strcasecmp($a['Id'], $b['Id']);
        });
        $this->assertEquals(3, count($table));
        $this->assertEquals(2, count($table[0]));
        $this->assertArrayHasKey('Id', $table[0]);
        $this->assertArrayHasKey('Name', $table[0]);
        $this->assertEquals('aabb', $table[0]['Id']);
        $this->assertEquals('ccdd', $table[0]['Name']);
        $this->assertEquals('test', $table[1]['Id']);
        $this->assertEquals('test', $table[1]['Name']);
        $this->assertEquals('test', $table[2]['Id']);
        $this->assertEquals('test', $table[2]['Name']);
    }

    public function testTagFiles()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/test", "test");

        $id1 = $this->client->uploadFile(
            $root . "/upload/test",
            (new FileUploadOptions())->setTags(array("docker-bundle-test"))
        );
        $id2 = $this->client->uploadFile(
            $root . "/upload/test",
            (new FileUploadOptions())->setTags(array("docker-bundle-test"))
        );

        $writer = new Writer($this->client, new NullLogger());
        $configuration = [["tags" => ["docker-bundle-test"], "processed_tags" => ['downloaded']]];
        $writer->tagFiles($configuration);

        $file = $this->client->getFile($id1);
        $this->assertTrue(in_array('downloaded', $file['tags']));
        $file = $this->client->getFile($id2);
        $this->assertTrue(in_array('downloaded', $file['tags']));
    }

    public function testWriteTableToDefaultBucket()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table1.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");
        file_put_contents($root . "/upload/table2.csv", "\"Id\",\"Name2\"\n\"test2\",\"test2\"\n");

        $writer = new Writer($this->client, new NullLogger());

        $writer->uploadTables($root . "/upload", ["bucket" => "in.c-docker-test"], ['componentId' => 'foo']);

        $tables = $this->client->listTables("in.c-docker-test");
        $this->assertCount(2, $tables);

        $this->assertEquals('in.c-docker-test.table1', $tables[0]["id"]);
        $tableInfo = $this->client->getTable('in.c-docker-test.table1');
        $this->assertEquals(array("Id", "Name"), $tableInfo["columns"]);

        $this->assertEquals('in.c-docker-test.table2', $tables[1]["id"]);
        $tableInfo = $this->client->getTable('in.c-docker-test.table2');
        $this->assertEquals(array("Id", "Name2"), $tableInfo["columns"]);
    }

    public function testWriteTableBareWithDefaultBucket()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table5.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $writer = new Writer($this->client, new NullLogger());

        $writer->uploadTables($root . "/upload", ['bucket' => 'out.c-docker-test'], ['componentId' => 'foo']);

        $tables = $this->client->listTables("out.c-docker-test");
        $this->assertCount(1, $tables);

        $this->assertEquals('out.c-docker-test.table5', $tables[0]["id"]);
        $tableInfo = $this->client->getTable('out.c-docker-test.table5');
        $this->assertEquals(array("Id", "Name"), $tableInfo["columns"]);
    }

    public function testWriteTableManifestWithDefaultBucket()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . "/upload/table6.csv",
            "\"Id\",\"Name\"\n\"test\",\"test\"\n"
        );
        file_put_contents(
            $root . "/upload/table6.csv.manifest",
            "{\"primary_key\": [\"Id\", \"Name\"]}"
        );

        $writer = new Writer($this->client, new NullLogger());

        $writer->uploadTables($root . "/upload", ['bucket' => 'out.c-docker-test'], ['componentId' => 'foo']);

        $tables = $this->client->listTables("out.c-docker-test");
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-docker-test.table6', $tables[0]["id"]);
        $this->assertEquals(array("Id", "Name"), $tables[0]["primaryKey"]);
    }

    public function testWriteTableOutputMappingWithDefaultBucket()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table7.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = array(
            array(
                "source" => "table7.csv",
                "destination" => "table7"
            )
        );

        $writer = new Writer($this->client, new NullLogger());

        $writer->uploadTables($root . "/upload", ["mapping" => $configs, 'bucket' => 'out.c-docker-test'], ['componentId' => 'foo']);

        $tables = $this->client->listTables("out.c-docker-test");
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-docker-test.table7', $tables[0]["id"]);
    }

    public function testWriteManifestWithoutDestination()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table8.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");
        file_put_contents($root . "/upload/table8.csv.manifest", "{\"primary_key\": [\"Id\", \"Name\"]}");

        $writer = new Writer($this->client, new NullLogger());
        try {
            $writer->uploadTables($root . "/upload", ["mapping" => []], ['componentId' => 'foo']);
            $this->fail("Empty destination with invalid table name must cause exception.");
        } catch (UserException $e) {
            $this->assertContains('valid table identifier', $e->getMessage());
        }
    }

    public function testValidateAgainstTable()
    {
        $tableInfo = [
            "primaryKey" => ["Id"]
        ];

        $writer = new Writer($this->client, new NullLogger());
        $writer->validateAgainstTable(
            $tableInfo,
            [
                "source" => "table9.csv",
                "destination" => "out.c-docker-test.table9",
                "primary_key" => ["Id"]
            ]
        );
    }

    public function testValidateAgainstTableEmptyPK()
    {
        $tableInfo = [
            "primaryKey" => []
        ];

        $writer = new Writer($this->client, new NullLogger());
        $writer->validateAgainstTable(
            $tableInfo,
            [
                "source" => "table9.csv",
                "destination" => "out.c-docker-test.table9",
                "primary_key" => []
            ]
        );
    }

    public function testValidateAgainstTableMismatch()
    {
        $tableInfo = [
            "primaryKey" => ["Id"]
        ];

        $writer = new Writer($this->client, new NullLogger());
        try {
            $writer->validateAgainstTable(
                $tableInfo,
                [
                    "source" => "table9.csv",
                    "destination" => "out.c-docker-test.table9",
                    "primary_key" => ["Id", "Name"]
                ]
            );
            $this->fail("Exception not caught");
        } catch (UserException $e) {
            $message = "Output mapping does not match destination table: primary key 'Id, Name' does not match 'Id' in 'out.c-docker-test.table9'.";
            $this->assertEquals($message, $e->getMessage());
        }
    }

    public function testWriteTableOutputMappingWithPk()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table9.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $writer = new Writer($this->client, new NullLogger());
        $writer->uploadTables(
            $root . "/upload",
            [
                "mapping" => [
                    [
                        "source" => "table9.csv",
                        "destination" => "out.c-docker-test.table9",
                        "primary_key" => ["Id"]
                    ]
                ]
            ],
            ['componentId' => 'foo']
        );
        $tableInfo = $this->client->getTable("out.c-docker-test.table9");
        $this->assertEquals(["Id"], $tableInfo["primaryKey"]);
    }

    public function testWriteTableOutputMappingWithPkOverwrite()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table9.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $writer = new Writer($this->client, new NullLogger());
        $writer->uploadTables(
            $root . "/upload",
            [
                "mapping" => [
                    [
                        "source" => "table9.csv",
                        "destination" => "out.c-docker-test.table9",
                        "primary_key" => ["Id"]
                    ]
                ]
            ],
            ['componentId' => 'foo']
        );

        $writer = new Writer($this->client, new NullLogger());
        $writer->uploadTables(
            $root . "/upload",
            [
                "mapping" => [
                    [
                        "source" => "table9.csv",
                        "destination" => "out.c-docker-test.table9",
                        "primary_key" => ["Id"]
                    ]
                ]
            ],
            ['componentId' => 'foo']
        );
        $tableInfo = $this->client->getTable("out.c-docker-test.table9");

        $this->assertEquals(["Id"], $tableInfo["primaryKey"]);
    }

    public function testWriteTableOutputMappingWithPkMismatch()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table9.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $handler = new TestHandler();

        $writer = new Writer($this->client, (new Logger("null"))->pushHandler($handler));
        $writer->uploadTables(
            $root . "/upload",
            [
                "mapping" => [
                    [
                        "source" => "table9.csv",
                        "destination" => "out.c-docker-test.table9",
                        "primary_key" => ["Id"]
                    ]
                ]
            ],
            ['componentId' => 'foo']
        );
        $tableInfo = $this->client->getTable("out.c-docker-test.table9");
        $this->assertEquals(["Id"], $tableInfo["primaryKey"]);

        $writer = new Writer($this->client, (new Logger("null"))->pushHandler($handler));
        try {
            $writer->uploadTables(
                $root . "/upload",
                [
                    "mapping" => [
                        [
                            "source" => "table9.csv",
                            "destination" => "out.c-docker-test.table9",
                            "primary_key" => ["Id", "Name"]
                        ]
                    ]
                ],
                ['componentId' => 'foo']
            );
            $this->fail("Exception not caught");
        } catch (\Exception $e) {
            $this->assertEquals("Keboola\\Syrup\\Exception\\UserException", get_class($e));
            $this->assertEquals("Output mapping does not match destination table: primary key 'Id, Name' does not match 'Id' in 'out.c-docker-test.table9'.", $e->getMessage());
        }
    }

    public function testWriteTableOutputMappingWithPkMismatchWhitespace()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table9.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $handler = new TestHandler();

        $writer = new Writer($this->client, (new Logger("null"))->pushHandler($handler));
        $writer->uploadTables(
            $root . "/upload",
            [
                "mapping" => [
                    [
                        "source" => "table9.csv",
                        "destination" => "out.c-docker-test.table9",
                        "primary_key" => ["Id "]
                    ]
                ]
            ],
            ['componentId' => 'foo']
        );
        $tableInfo = $this->client->getTable("out.c-docker-test.table9");
        $this->assertEquals(["Id"], $tableInfo["primaryKey"]);

        $writer = new Writer($this->client, (new Logger("null"))->pushHandler($handler));
        try {
            $writer->uploadTables(
                $root . "/upload",
                [
                    "mapping" => [
                        [
                            "source" => "table9.csv",
                            "destination" => "out.c-docker-test.table9",
                            "primary_key" => ["Id ", "Name "]
                        ]
                    ]
                ],
                ['componentId' => 'foo']
            );
            $this->fail("Exception not caught");
        } catch (\Exception $e) {
            $this->assertEquals("Keboola\\Syrup\\Exception\\UserException", get_class($e));
            $this->assertEquals("Output mapping does not match destination table: primary key 'Id, Name' does not match 'Id' in 'out.c-docker-test.table9'.", $e->getMessage());
        }
    }



    public function testWriteTableOutputMappingWithEmptyStringPk()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table9.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $handler = new TestHandler();

        $writer = new Writer($this->client, (new Logger("null"))->pushHandler($handler));
        $writer->uploadTables(
            $root . "/upload",
            [
                "mapping" => [
                    [
                        "source" => "table9.csv",
                        "destination" => "out.c-docker-test.table9",
                        "primary_key" => []
                    ]
                ]
            ],
            ['componentId' => 'foo']
        );
        $tableInfo = $this->client->getTable("out.c-docker-test.table9");
        $this->assertEquals([], $tableInfo["primaryKey"]);


        $writer = new Writer($this->client, (new Logger("null"))->pushHandler($handler));
        $writer->uploadTables(
            $root . "/upload",
            [
                "mapping" => [
                    [
                        "source" => "table9.csv",
                        "destination" => "out.c-docker-test.table9",
                        "primary_key" => [""]
                    ]
                ]
            ],
            ['componentId' => 'foo']
        );
        $this->assertFalse($handler->hasWarningThatContains("Output mapping does not match destination table"));
        $tableInfo = $this->client->getTable("out.c-docker-test.table9");
        $this->assertEquals([], $tableInfo["primaryKey"]);
    }

    public function testWriteTableOutputMappingWithEmptyStringPkInManifest()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table9.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $handler = new TestHandler();

        $writer = new Writer($this->client, (new Logger("null"))->pushHandler($handler));
        $writer->uploadTables(
            $root . "/upload",
            [
                "mapping" => [
                    [
                        "source" => "table9.csv",
                        "destination" => "out.c-docker-test.table9",
                        "primary_key" => []
                    ]
                ]
            ],
            ['componentId' => 'foo']
        );

        $writer = new Writer($this->client, (new Logger("null"))->pushHandler($handler));
        file_put_contents($root . "/upload/table9.csv.manifest", '{"destination": "out.c-docker-test.table9","primary_key": [""]}');
        $writer->uploadTables($root . "/upload", [], ['componentId' => 'foo']);
        $this->assertFalse($handler->hasWarningThatContains("Output mapping does not match destination table: primary key '' does not match '' in 'out.c-docker-test.table9'."));
        $tableInfo = $this->client->getTable("out.c-docker-test.table9");
        $this->assertEquals([], $tableInfo["primaryKey"]);
    }
}
