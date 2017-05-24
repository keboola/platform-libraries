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
use Psr\Log\NullLogger;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;

class ReaderTest extends \PHPUnit_Framework_TestCase
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

    public function testParentId()
    {
        $reader = new Reader($this->client, new NullLogger());
        $this->client->setRunId('123456789');
        self::assertEquals('123456789', $reader->getParentRunId());
        $this->client->setRunId('123456789.98765432');
        self::assertEquals('123456789', $reader->getParentRunId());
        $this->client->setRunId('123456789.98765432.4563456');
        self::assertEquals('123456789.98765432', $reader->getParentRunId());
        $this->client->setRunId(null);
        self::assertEquals('', $reader->getParentRunId());
    }

    public function testReadInvalidConfigurations()
    {
        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");

        // empty configuration, ignored
        $reader = new Reader($this->client, new NullLogger());
        $configuration = null;
        $reader->downloadFiles($configuration, $root . "/download");

        // empty configuration, ignored
        $reader = new Reader($this->client, new NullLogger());
        $configuration = 'foobar';
        try {
            /** @noinspection PhpParamsInspection */
            $reader->downloadFiles($configuration, $root . "/download");
            self::fail("Invalid configuration should fail.");
        } catch (InvalidInputException $e) {
        }

        // empty configuration, ignored
        $reader = new Reader($this->client, new NullLogger());
        $configuration = null;
        $reader->downloadTables($configuration, $root . "/download");
    }
}
