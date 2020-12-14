<?php

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\NullCapability;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Psr\Log\Test\TestLogger;

class DownloadTablesABSDefaultTest extends DownloadTablesTestAbstract
{
    public function setUp()
    {
        parent::setUp();
        try {
            $this->clientWrapper->getBasicClient()->dropBucket("in.c-docker-test", ["force" => true]);
        } catch (ClientException $e) {
            if ($e->getCode() != 404) {
                throw $e;
            }
        }
        $this->clientWrapper->getBasicClient()->createBucket("docker-test", Client::STAGE_IN, "Docker Testsuite");

        // Create table
        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "upload.csv");
        $csv->writeRow(["Id", "Name", "foo", "bar"]);
        $csv->writeRow(["id1", "name1", "foo1", "bar1"]);
        $csv->writeRow(["id2", "name2", "foo2", "bar2"]);
        $csv->writeRow(["id3", "name3", "foo3", "bar3"]);
        $this->clientWrapper->getBasicClient()->createTableAsync("in.c-docker-test", "test", $csv);
        $this->clientWrapper->getBasicClient()->createTableAsync("in.c-docker-test", "test2", $csv);
    }

    public function testReadTablesABSDefaultBackend()
    {
        $logger = new TestLogger();
        $reader = new Reader($this->clientWrapper, $logger, new NullCapability());
        $configuration = new InputTableOptionsList([
            [
                "source" => "in.c-docker-test.test",
                "destination" => "test.csv",
            ],
            [
                "source" => "in.c-docker-test.test2",
                "destination" => "test2.csv",
            ]
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download",
            "abs"
        );

        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . "/download/test.csv.manifest");
        self::assertEquals("in.c-docker-test.test", $manifest["id"]);
        $this->assertABSinfo($manifest);

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . "/download/test2.csv.manifest");
        self::assertEquals("in.c-docker-test.test2", $manifest["id"]);
        $this->assertABSinfo($manifest);
        self::assertTrue($logger->hasInfoThatContains('Processing 2 ABS table exports.'));
    }

    public function testReadTablesS3UnsupportedBackend()
    {
        $logger = new TestLogger();
        $reader = new Reader($this->clientWrapper, $logger, new NullCapability());
        $configuration = new InputTableOptionsList([
            [
                "source" => "in.c-docker-test.test",
                "destination" => "test.csv",
            ],
            [
                "source" => "in.c-docker-test.test2",
                "destination" => "test2.csv",
            ]
        ]);

        self::expectException(InvalidInputException::class);
        self::expectExceptionMessage('This project does not have S3 backend.');
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download",
            "s3"
        );
    }
}
