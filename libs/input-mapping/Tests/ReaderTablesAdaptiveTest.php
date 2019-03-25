<?php

namespace Keboola\InputMapping\Tests;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Reader\Options\InputTableOptions;
use Keboola\InputMapping\Reader\Options\InputTablesOptions;
use Keboola\InputMapping\Reader\Reader;
use Keboola\InputMapping\Reader\State\InputTableStateList;
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
        $inputTablesState = new InputTableStateList([
            [
                "source" => "in.c-docker-test.test",
                "lastImportDate" => $testTableInfo['lastImportDate']
            ]
        ]);
        $tablesState = $reader->downloadTables($configuration, $inputTablesState, $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download");

        self::assertEquals($testTableInfo['lastImportDate'], $tablesState->getTable("in.c-docker-test.test")->getLastImportDate());
        self::assertCSVEquals(
            "\"Id\",\"Name\",\"foo\",\"bar\"\n",
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download/test.csv"
        );
        self::assertCount(1, $tablesState->jsonSerialize());
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
        $firstTablesState = $reader->downloadTables($configuration, new InputTableStateList([]), $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download");

        // Update table
        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "upload.csv");
        $csv->writeRow(["Id", "Name", "foo", "bar"]);
        $csv->writeRow(["id4", "name4", "foo4", "bar4"]);
        $this->client->writeTableAsync("in.c-docker-test.test", $csv, ["incremental" => true]);

        $updatedTestTableInfo = $this->client->getTable("in.c-docker-test.test");
        $secondTablesState = $reader->downloadTables($configuration, $firstTablesState, $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download");

        self::assertEquals($updatedTestTableInfo['lastImportDate'], $secondTablesState->getTable("in.c-docker-test.test")->getLastImportDate());
        self::assertCSVEquals(
            "\"Id\",\"Name\",\"foo\",\"bar\"\n\"id4\",\"name4\",\"foo4\",\"bar4\"\n",
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download/test.csv"
        );
        self::assertCount(1, $secondTablesState->jsonSerialize());
    }

    public function testDownloadTablesInvalidDate()
    {
        $reader = new Reader($this->client, new NullLogger());
        $configuration = new InputTablesOptions([
            [
                "source" => "in.c-docker-test.test",
                "destination" => "test.csv",
                "changed_since" => InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE,
            ]
        ]);
        $inputTablesState = new InputTableStateList([
            [
                "source" => "in.c-docker-test.test",
                "lastImportDate" => "nonsense"
            ]
        ]);

        self::expectException(ClientException::class);
        self::expectExceptionMessage("Invalid date format: nonsense");
        $reader->downloadTables($configuration, $inputTablesState, $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download");
    }
}
