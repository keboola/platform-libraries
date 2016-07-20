<?php

namespace Keboola\InputMapping\Tests;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Configuration\File\Manifest\Adapter as FileManifestAdapter;
use Keboola\InputMapping\Configuration\Table\Manifest\Adapter as TableManifestAdapter;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader\Reader;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\Temp\Temp;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;

class StorageApiReaderTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var Client
     */
    protected $client;

    /**
     * @var string
     */
    protected $tmpDir;

    /**
     * @var Temp
     */
    private $temp;

    public function setUp()
    {
        // Create folders
        $temp = new Temp('docker');
        $temp->initRunFolder();
        $this->temp = $temp;
        $this->tmpDir = $temp->getTmpFolder();
        $fs = new Filesystem();
        $fs->mkdir($this->tmpDir . "/download");

        $this->client = new Client(["token" => STORAGE_API_TOKEN]);
    }

    public function tearDown()
    {
        // Delete local files
        $finder = new Finder();
        $fs = new Filesystem();
        $fs->remove($finder->files()->in($this->tmpDir . "/download"));
        $fs->remove($finder->files()->in($this->tmpDir));
        $fs->remove($this->tmpDir . "/download");
        $fs->remove($this->tmpDir);

        // Delete file uploads
        $options = new ListFilesOptions();
        $options->setTags(["docker-bundle-test"]);
        $files = $this->client->listFiles($options);
        foreach ($files as $file) {
            $this->client->deleteFile($file["id"]);
        }

        if ($this->client->bucketExists("in.c-docker-test")) {
            // Delete tables
            foreach ($this->client->listTables("in.c-docker-test") as $table) {
                $this->client->dropTable($table["id"]);
            }

            // Delete bucket
            $this->client->dropBucket("in.c-docker-test");
        }

        if ($this->client->bucketExists("in.c-docker-test-redshift")) {
            // Delete tables
            foreach ($this->client->listTables("in.c-docker-test-redshift") as $table) {
                $this->client->dropTable($table["id"]);
            }

            // Delete bucket
            $this->client->dropBucket("in.c-docker-test-redshift");
        }
    }

    public function testReadFiles()
    {
        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");

        $id1 = $this->client->uploadFile($root . "/upload", (new FileUploadOptions())->setTags(["docker-bundle-test"]));
        $id2 = $this->client->uploadFile($root . "/upload", (new FileUploadOptions())->setTags(["docker-bundle-test"]));

        $reader = new Reader($this->client);
        $configuration = [["tags" => ["docker-bundle-test"]]];
        $reader->downloadFiles($configuration, $root . "/download");

        $this->assertEquals("test", file_get_contents($root . "/download/" . $id1 . '_upload'));
        $this->assertEquals("test", file_get_contents($root . "/download/" . $id2 . '_upload'));

        $adapter = new FileManifestAdapter();
        $manifest1 = $adapter->readFromFile($root . "/download/" . $id1 . "_upload.manifest");
        $manifest2 = $adapter->readFromFile($root . "/download/" . $id2 . "_upload.manifest");

        $this->assertEquals($id1, $manifest1["id"]);
        $this->assertEquals($id2, $manifest2["id"]);
    }


    public function testParentId()
    {
        $reader = new Reader($this->client);
        $this->client->setRunId('123456789');
        $this->assertEquals('123456789', $reader->getParentRunId());
        $this->client->setRunId('123456789.98765432');
        $this->assertEquals('123456789', $reader->getParentRunId());
        $this->client->setRunId('123456789.98765432.4563456');
        $this->assertEquals('123456789.98765432', $reader->getParentRunId());
        $this->client->setRunId(null);
        $this->assertEquals('', $reader->getParentRunId());
    }


    public function testReadFilesTagsFilterRunId()
    {
        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");
        $reader = new Reader($this->client);
        $fo = new FileUploadOptions();
        $fo->setTags(["docker-bundle-test"]);

        $this->client->setRunId('xyz');
        $id1 = $this->client->uploadFile($root . "/upload", $fo);
        $id2 = $this->client->uploadFile($root . "/upload", $fo);
        $this->client->setRunId('1234567');
        $id3 = $this->client->uploadFile($root . "/upload", $fo);
        $id4 = $this->client->uploadFile($root . "/upload", $fo);
        $this->client->setRunId('1234567.8901234');
        $id5 = $this->client->uploadFile($root . "/upload", $fo);
        $id6 = $this->client->uploadFile($root . "/upload", $fo);

        $configuration = [["tags" => ["docker-bundle-test"], "filter_by_run_id" => true]];
        $reader->downloadFiles($configuration, $root . "/download");

        $this->assertFalse(file_exists($root . "/download/" . $id1 . '_upload'));
        $this->assertFalse(file_exists($root . "/download/" . $id2 . '_upload'));
        $this->assertTrue(file_exists($root . "/download/" . $id3 . '_upload'));
        $this->assertTrue(file_exists($root . "/download/" . $id4 . '_upload'));
        $this->assertTrue(file_exists($root . "/download/" . $id5 . '_upload'));
        $this->assertTrue(file_exists($root . "/download/" . $id6 . '_upload'));
    }


    public function testReadFilesEsQueryFilterRunId()
    {
        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");
        $reader = new Reader($this->client);
        $fo = new FileUploadOptions();
        $fo->setTags(["docker-bundle-test"]);

        $this->client->setRunId('xyz');
        $id1 = $this->client->uploadFile($root . "/upload", $fo);
        $id2 = $this->client->uploadFile($root . "/upload", $fo);
        $this->client->setRunId('1234567');
        $id3 = $this->client->uploadFile($root . "/upload", $fo);
        $id4 = $this->client->uploadFile($root . "/upload", $fo);
        $this->client->setRunId('1234567.8901234');
        $id5 = $this->client->uploadFile($root . "/upload", $fo);
        $id6 = $this->client->uploadFile($root . "/upload", $fo);

        $configuration = [["query" => "tags: docker-bundle-test", "filter_by_run_id" => true]];
        $reader->downloadFiles($configuration, $root . "/download");

        $this->assertFalse(file_exists($root . "/download/" . $id1 . '_upload'));
        $this->assertFalse(file_exists($root . "/download/" . $id2 . '_upload'));
        $this->assertTrue(file_exists($root . "/download/" . $id3 . '_upload'));
        $this->assertTrue(file_exists($root . "/download/" . $id4 . '_upload'));
        $this->assertTrue(file_exists($root . "/download/" . $id5 . '_upload'));
        $this->assertTrue(file_exists($root . "/download/" . $id6 . '_upload'));
    }

    public function testReadSlicedFile()
    {
        // Create bucket
        if (!$this->client->bucketExists("in.c-docker-test-redshift")) {
            $this->client->createBucket("docker-test-redshift", Client::STAGE_IN, "Docker Testsuite", "redshift");
        }

        // Create redshift table and export it to produce a sliced file
        if (!$this->client->tableExists("in.c-docker-test-redshift.test_file")) {
            $csv = new CsvFile($this->tmpDir . "/upload.csv");
            $csv->writeRow(["Id", "Name"]);
            $csv->writeRow(["test", "test"]);
            $this->client->createTableAsync("in.c-docker-test-redshift", "test_file", $csv);
        }
        $table = $this->client->exportTableAsync('in.c-docker-test-redshift.test_file');
        $fileId = $table['file']['id'];

        $reader = new Reader($this->client);
        $configuration = [['query' => 'id: ' . $fileId]];
        try {
            $reader->downloadFiles($configuration, $this->tmpDir . "/download");
            $this->fail("Downloading a sliced files should fail.");
        } catch (InvalidInputException $e) {
        }
    }


    public function testReadFilesErrors()
    {
        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");

        // make at least 10 files in the project
        for ($i = 0; $i < 12; $i++) {
            $this->client->uploadFile($root . "/upload", (new FileUploadOptions())->setTags(["docker-bundle-test"]));
        }

        // valid configuration, but does nothing
        $reader = new Reader($this->client);
        $configuration = [];
        $reader->downloadFiles($configuration, $root . "/download");

        // invalid configuration
        $reader = new Reader($this->client);
        $configuration = [[]];
        try {
            $reader->downloadFiles($configuration, $root . "/download");
            $this->fail("Invalid configuration should fail.");
        } catch (InvalidInputException $e) {
        }

        $reader = new Reader($this->client);
        $configuration = [['query' => 'id:>0 AND (NOT tags:table-export)']];
        try {
            $reader->downloadFiles($configuration, $root . "/download");
            $this->fail("Too broad query should fail.");
        } catch (InvalidInputException $e) {
            $this->assertContains('File input mapping downloads more than', $e->getMessage());
        }

        $reader = new Reader($this->client);
        $configuration = [['tags' => ['docker-bundle-test'], 'limit' => 12]];
        $reader->downloadFiles($configuration, $root . "/download");
    }


    /**
     *
     */
    public function testReadTablesDefaultBackend()
    {
        // Create bucket
        if (!$this->client->bucketExists("in.c-docker-test")) {
            $this->client->createBucket("docker-test", Client::STAGE_IN, "Docker Testsuite");
        }

        // Create table
        if (!$this->client->tableExists("in.c-docker-test.test")) {
            $csv = new CsvFile($this->tmpDir . "/upload.csv");
            $csv->writeRow(["Id", "Name"]);
            $csv->writeRow(["test", "test"]);
            $this->client->createTableAsync("in.c-docker-test", "test", $csv);
            $this->client->setTableAttribute("in.c-docker-test.test", "attr1", "val1");
        }

        $root = $this->tmpDir;

        $reader = new Reader($this->client);
        $configuration = [
            [
                "source" => "in.c-docker-test.test",
                "destination" => "test.csv"
            ]
        ];

        $reader->downloadTables($configuration, $root . "/download");

        $this->assertEquals("\"Id\",\"Name\"\n\"test\",\"test\"\n", file_get_contents($root . "/download/test.csv"));

        $adapter = new TableManifestAdapter();

        $manifest = $adapter->readFromFile($root . "/download/test.csv.manifest");
        $this->assertEquals("in.c-docker-test.test", $manifest["id"]);
        $this->assertEquals("val1", $manifest["attributes"][0]["value"]);
    }

    /**
     *
     */
    public function testReadTablesRedshift()
    {
        // Create bucket
        if (!$this->client->bucketExists("in.c-docker-test-redshift")) {
            $this->client->createBucket("docker-test-redshift", Client::STAGE_IN, "Docker Testsuite", "redshift");
        }

        // Create table
        if (!$this->client->tableExists("in.c-docker-test-redshift.test")) {
            $csv = new CsvFile($this->tmpDir . "/upload.csv");
            $csv->writeRow(["Id", "Name"]);
            $csv->writeRow(["test", "test"]);
            $this->client->createTableAsync("in.c-docker-test-redshift", "test", $csv);
            $this->client->setTableAttribute("in.c-docker-test-redshift.test", "attr1", "val2");
        }

        $root = $this->tmpDir;

        $reader = new Reader($this->client);
        $configuration = [
            [
                "source" => "in.c-docker-test-redshift.test",
                "destination" => "test-redshift.csv"
            ]
        ];

        $reader->downloadTables($configuration, $root . "/download");

        $this->assertEquals(
            "\"Id\",\"Name\"\n\"test\",\"test\"\n",
            file_get_contents($root . "/download/test-redshift.csv")
        );

        $adapter = new TableManifestAdapter();

        $manifest = $adapter->readFromFile($root . "/download/test-redshift.csv.manifest");
        $this->assertEquals("in.c-docker-test-redshift.test", $manifest["id"]);
        $this->assertEquals("val2", $manifest["attributes"][0]["value"]);
    }
}
