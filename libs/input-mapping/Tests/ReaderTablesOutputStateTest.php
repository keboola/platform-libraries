<?php

namespace Keboola\InputMapping\Tests;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Reader\Options\InputTablesOptions;
use Keboola\InputMapping\Reader\Reader;
use Keboola\InputMapping\Reader\State\InputTableStateList;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Psr\Log\NullLogger;

class ReaderTablesOutputStateTest extends ReaderTablesTestAbstract
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
        $csv->writeRow(["Id", "Name", "foo", "bar"]);
        $csv->writeRow(["id1", "name1", "foo1", "bar1"]);
        $csv->writeRow(["id2", "name2", "foo2", "bar2"]);
        $csv->writeRow(["id3", "name3", "foo3", "bar3"]);
        $this->client->createTableAsync("in.c-docker-test", "test", $csv);
        $this->client->createTableAsync("in.c-docker-test", "test2", $csv);
    }

    public function testDownloadTablesReturnsAllTablesTimestamps()
    {
        $reader = new Reader($this->client, new NullLogger());
        $configuration = new InputTablesOptions([
            [
                "source" => "in.c-docker-test.test",
                "destination" => "test.csv",
            ],
            [
                "source" => "in.c-docker-test.test2",
                "destination" => "test2.csv",
            ],
        ]);

        $tablesState = $reader->downloadTables($configuration, new InputTableStateList([]), $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download");
        $testTableInfo = $this->client->getTable("in.c-docker-test.test");
        $test2TableInfo = $this->client->getTable("in.c-docker-test.test2");
        self::assertEquals($testTableInfo['lastImportDate'], $tablesState->getTable("in.c-docker-test.test")->getLastImportDate());
        self::assertEquals($test2TableInfo['lastImportDate'], $tablesState->getTable("in.c-docker-test.test2")->getLastImportDate());
        self::assertCSVEquals(
            "\"Id\",\"Name\",\"foo\",\"bar\"\n\"id1\",\"name1\",\"foo1\",\"bar1\"\n" .
            "\"id2\",\"name2\",\"foo2\",\"bar2\"\n\"id3\",\"name3\",\"foo3\",\"bar3\"\n",
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download/test.csv"
        );
        self::assertCSVEquals(
            "\"Id\",\"Name\",\"foo\",\"bar\"\n\"id1\",\"name1\",\"foo1\",\"bar1\"\n" .
            "\"id2\",\"name2\",\"foo2\",\"bar2\"\n\"id3\",\"name3\",\"foo3\",\"bar3\"\n",
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download/test2.csv"
        );
        self::assertCount(2, $tablesState->toArray());
    }


    public function testDownloadTablesReturnsASingleTimestamps()
    {
        $reader = new Reader($this->client, new NullLogger());
        $configuration = new InputTablesOptions([
            [
                "source" => "in.c-docker-test.test",
                "destination" => "test.csv",
            ]
        ]);
        $tablesState = $reader->downloadTables($configuration, new InputTableStateList([]), $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download");
        self::assertCount(1, $tablesState->toArray());
    }
}
