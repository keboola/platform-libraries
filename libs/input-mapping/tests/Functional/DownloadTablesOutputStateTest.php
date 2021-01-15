<?php

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;

class DownloadTablesOutputStateTest extends DownloadTablesTestAbstract
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

    public function testDownloadTablesReturnsAllTablesTimestamps()
    {
        $reader = new Reader($this->getStagingFactory());
        $configuration = new InputTableOptionsList([
            [
                "source" => "in.c-docker-test.test",
                "destination" => "test.csv",
            ],
            [
                "source" => "in.c-docker-test.test2",
                "destination" => "test2.csv",
            ],
        ]);

        $tablesState = $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            StrategyFactory::LOCAL,
            new ReaderOptions(true)
        );
        $testTableInfo = $this->clientWrapper->getBasicClient()->getTable("in.c-docker-test.test");
        $test2TableInfo = $this->clientWrapper->getBasicClient()->getTable("in.c-docker-test.test2");
        self::assertEquals(
            $testTableInfo['lastImportDate'],
            $tablesState->getTable("in.c-docker-test.test")->getLastImportDate()
        );
        self::assertEquals(
            $test2TableInfo['lastImportDate'],
            $tablesState->getTable("in.c-docker-test.test2")->getLastImportDate()
        );
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
        self::assertCount(2, $tablesState->jsonSerialize());
    }
}
