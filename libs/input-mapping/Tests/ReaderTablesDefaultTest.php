<?php

namespace Keboola\InputMapping\Tests;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Reader\Reader;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Psr\Log\NullLogger;

class ReaderTablesDefaultTest extends ReaderTablesTestAbstract
{
    public function setUp()
    {
        parent::setUp();
        try {
            $this->client->dropBucket("in.c-docker-test", ["force" => true]);
        } catch (ClientException $e) {
            if ($e->getCode() != 404) {
                throw $e;
            }
        }
        $this->client->createBucket("docker-test", Client::STAGE_IN, "Docker Testsuite");

        // Create table
        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "upload.csv");
        $csv->writeRow(["Id", "Name"]);
        $csv->writeRow(["test", "test"]);
        $this->client->createTableAsync("in.c-docker-test", "test", $csv);
        $this->client->setTableAttribute("in.c-docker-test.test", "attr1", "val1");
    }

    public function testReadTablesDefaultBackend()
    {
        $reader = new Reader($this->client, new NullLogger());
        $configuration = [
            [
                "source" => "in.c-docker-test.test",
                "destination" => "test.csv"
            ]
        ];

        $reader->downloadTables($configuration, $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download");

        self::assertEquals(
            "\"Id\",\"Name\"\n\"test\",\"test\"\n",
            file_get_contents($this->temp->getTmpFolder() . "/download/test.csv")
        );

        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . "/download/test.csv.manifest");
        self::assertEquals("in.c-docker-test.test", $manifest["id"]);
        self::assertEquals("val1", $manifest["attributes"][0]["value"]);
    }

    public function testReadTablesEmptyDaysFilter()
    {
        $reader = new Reader($this->client, new NullLogger());
        $configuration = [
            [
                "source" => "in.c-docker-test.test",
                "destination" => "test.csv",
                "days" => 0
            ]
        ];

        $reader->downloadTables($configuration, $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download");
        self::assertEquals(
            "\"Id\",\"Name\"\n\"test\",\"test\"\n",
            file_get_contents($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download/test.csv")
        );
    }

    public function testReadTablesS3DefaultBackend()
    {
        $reader = new Reader($this->client, new NullLogger());
        $configuration = [
            [
                "source" => "in.c-docker-test.test",
                "destination" => "test.csv",
            ]
        ];

        $reader->downloadTables($configuration, $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download", "s3");

        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . "/download/test.csv.manifest");
        self::assertEquals("in.c-docker-test.test", $manifest["id"]);
        self::assertEquals("val1", $manifest["attributes"][0]["value"]);
        $this->assertS3info($manifest);
    }
}
