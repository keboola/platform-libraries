<?php

namespace Keboola\InputMapping\Tests\Reader;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Reader\Options\InputTableOptionsList;
use Keboola\InputMapping\Reader\Reader;
use Keboola\InputMapping\Reader\State\InputTableStateList;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Psr\Log\NullLogger;

class DownloadTablesRedshiftTest extends DownloadTablesTestAbstract
{
    public function setUp()
    {
        parent::setUp();
        try {
            $this->client->dropBucket("in.c-docker-test-redshift", ["force" => true]);
        } catch (ClientException $e) {
            if ($e->getCode() != 404) {
                throw $e;
            }
        }
        $this->client->createBucket("docker-test-redshift", Client::STAGE_IN, "Docker Testsuite", "redshift");

        // Create table
        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "upload.csv");
        $csv->writeRow(["Id", "Name"]);
        $csv->writeRow(["test", "test"]);
        $this->client->createTableAsync("in.c-docker-test-redshift", "test", $csv);
    }

    public function testReadTablesRedshift()
    {
        $reader = new Reader($this->client, new NullLogger());
        $configuration = new InputTableOptionsList([
            [
                "source" => "in.c-docker-test-redshift.test",
                "destination" => "test-redshift.csv"
            ]
        ]);

        $reader->downloadTables($configuration, new InputTableStateList([]), $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download");

        self::assertEquals(
            "\"Id\",\"Name\"\n\"test\",\"test\"\n",
            file_get_contents($this->temp->getTmpFolder(). "/download/test-redshift.csv")
        );

        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . "/download/test-redshift.csv.manifest");
        self::assertEquals("in.c-docker-test-redshift.test", $manifest["id"]);
    }

    public function testReadTablesS3Redshift()
    {
        $reader = new Reader($this->client, new NullLogger());
        $configuration = new InputTableOptionsList([
            [
                "source" => "in.c-docker-test-redshift.test",
                "destination" => "test-redshift.csv"
            ]
        ]);

        $reader->downloadTables($configuration, new InputTableStateList([]), $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download", "s3");
        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . "/download/test-redshift.csv.manifest");
        self::assertEquals("in.c-docker-test-redshift.test", $manifest["id"]);
        $this->assertS3info($manifest);
    }
}
