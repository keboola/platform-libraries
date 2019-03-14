<?php

namespace Keboola\InputMapping\Tests;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Reader\Options\InputTableOptions;
use Keboola\InputMapping\Reader\Options\InputTablesOptions;
use Keboola\InputMapping\Reader\Reader;
use Keboola\InputMapping\Reader\State\InputTablesState;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Psr\Log\NullLogger;

class ReaderTablesAdaptiveTest extends ReaderTablesTestAbstract
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
    }

    public function testDownloadTablesDownloadsTheWholeTable()
    {
        $reader = new Reader($this->client, new NullLogger());
        $configuration = new InputTablesOptions([
            [
                "source" => "in.c-docker-test.test",
                "destination" => "test.csv",
                "changed_since" => InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE,
            ]
        ]);

        $tablesState = $reader->downloadTables($configuration, new InputTablesState([]), $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download");
        $testTableInfo = $this->client->getTable("in.c-docker-test.test");
        self::assertEquals(new \DateTime($testTableInfo['lastImportDate']), $tablesState->getTable("in.c-docker-test.test")->getLastImportDate());
        self::assertCSVEquals(
            "\"Id\",\"Name\",\"foo\",\"bar\"\n\"id1\",\"name1\",\"foo1\",\"bar1\"\n" .
            "\"id2\",\"name2\",\"foo2\",\"bar2\"\n\"id3\",\"name3\",\"foo3\",\"bar3\"\n",
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download/test.csv"
        );
        self::assertCount(1, $tablesState->toArray());
    }

    public function testDownloadTablesDownloadsEmptyTable()
    {
        $reader = new Reader($this->client, new NullLogger());
        $configuration = new InputTablesOptions([
            [
                "source" => "in.c-docker-test.test",
                "destination" => "test.csv",
                "changed_since" => InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE,
            ]
        ]);
        $testTableInfo = $this->client->getTable("in.c-docker-test.test");
        $inputTablesState = new InputTablesState([
            [
                "source" => "in.c-docker-test.test",
                "lastImportDate" => $testTableInfo['lastImportDate']
            ]
        ]);
        $tablesState = $reader->downloadTables($configuration, $inputTablesState, $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download");

        self::assertEquals(new \DateTime($testTableInfo['lastImportDate']), $tablesState->getTable("in.c-docker-test.test")->getLastImportDate());
        self::assertCSVEquals(
            "\"Id\",\"Name\",\"foo\",\"bar\"\n",
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download/test.csv"
        );
        self::assertCount(1, $tablesState->toArray());
    }


    public function testDownloadTablesDownloadsOnlyNewRows()
    {
        $reader = new Reader($this->client, new NullLogger());
        $configuration = new InputTablesOptions([
            [
                "source" => "in.c-docker-test.test",
                "destination" => "test.csv",
                "changed_since" => InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE,
            ]
        ]);
        $testTableInfo = $this->client->getTable("in.c-docker-test.test");
        $inputTablesState = new InputTablesState([
            [
                "source" => "in.c-docker-test.test",
                "lastImportDate" => $testTableInfo['lastImportDate']
            ]
        ]);

        // Create table
        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "upload.csv");
        $csv->writeRow(["Id", "Name", "foo", "bar"]);
        $csv->writeRow(["id4", "name4", "foo4", "bar4"]);
        $this->client->writeTableAsync("in.c-docker-test.test", $csv);

        $updatedTestTableInfo = $this->client->getTable("in.c-docker-test.test");
        $tablesState = $reader->downloadTables($configuration, $inputTablesState, $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download");

        self::assertEquals(new \DateTime($updatedTestTableInfo['lastImportDate']), $tablesState->getTable("in.c-docker-test.test")->getLastImportDate());
        self::assertCSVEquals(
            "\"Id\",\"Name\",\"foo\",\"bar\"\n\"id4\",\"name4\",\"foo4\",\"bar4\"\n",
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download/test.csv"
        );
        self::assertCount(1, $tablesState->toArray());
    }
}
