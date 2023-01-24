<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptions;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;

class DownloadTablesAdaptiveTest extends DownloadTablesTestAbstract
{
    public function setUp(): void
    {
        parent::setUp();
        try {
            $this->clientWrapper->getBasicClient()->dropBucket('in.c-docker-test', ['force' => true]);
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }
        $this->clientWrapper->getBasicClient()->createBucket('docker-test', Client::STAGE_IN, 'Docker Testsuite');

        // Create table
        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload.csv');
        $csv->writeRow(['Id', 'Name', 'foo', 'bar']);
        $csv->writeRow(['id1', 'name1', 'foo1', 'bar1']);
        $csv->writeRow(['id2', 'name2', 'foo2', 'bar2']);
        $csv->writeRow(['id3', 'name3', 'foo3', 'bar3']);
        $this->clientWrapper->getBasicClient()->createTableAsync('in.c-docker-test', 'test', $csv);
    }

    public function testDownloadTablesDownloadsEmptyTable(): void
    {
        $reader = new Reader($this->getStagingFactory());
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-docker-test.test',
                'destination' => 'test.csv',
                'changed_since' => InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE,
            ],
        ]);
        $testTableInfo = $this->clientWrapper->getBasicClient()->getTable('in.c-docker-test.test');
        $inputTablesState = new InputTableStateList([
            [
                'source' => 'in.c-docker-test.test',
                'lastImportDate' => $testTableInfo['lastImportDate'],
            ],
        ]);
        $tablesResult = $reader->downloadTables(
            $configuration,
            $inputTablesState,
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true)
        );

        self::assertEquals(
            $testTableInfo['lastImportDate'],
            $tablesResult->getInputTableStateList()->getTable('in.c-docker-test.test')->getLastImportDate()
        );
        self::assertCSVEquals(
            "\"Id\",\"Name\",\"foo\",\"bar\"\n",
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download/test.csv'
        );
        self::assertCount(1, $tablesResult->getInputTableStateList()->jsonSerialize());
    }

    public function testDownloadTablesDownloadsOnlyNewRows(): void
    {
        $reader = new Reader($this->getStagingFactory());
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-docker-test.test',
                'destination' => 'test.csv',
                'changed_since' => InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE,
            ],
        ]);
        $firstTablesResult = $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true)
        );

        // Update table
        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload.csv');
        $csv->writeRow(['Id', 'Name', 'foo', 'bar']);
        $csv->writeRow(['id4', 'name4', 'foo4', 'bar4']);
        $this->clientWrapper->getBasicClient()->writeTableAsync(
            'in.c-docker-test.test',
            $csv,
            ['incremental' => true]
        );

        $updatedTestTableInfo = $this->clientWrapper->getBasicClient()->getTable('in.c-docker-test.test');
        $secondTablesResult = $reader->downloadTables(
            $configuration,
            $firstTablesResult->getInputTableStateList(),
            'data/in/tables/',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true)
        );

        self::assertEquals(
            $updatedTestTableInfo['lastImportDate'],
            $secondTablesResult->getInputTableStateList()->getTable('in.c-docker-test.test')->getLastImportDate()
        );
        self::assertCSVEquals(
            "\"Id\",\"Name\",\"foo\",\"bar\"\n\"id4\",\"name4\",\"foo4\",\"bar4\"\n",
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'data/in/tables/test.csv'
        );
        self::assertCount(1, $secondTablesResult->getInputTableStateList()->jsonSerialize());
    }

    public function testDownloadTablesInvalidDate(): void
    {
        $reader = new Reader($this->getStagingFactory());
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-docker-test.test',
                'destination' => 'test.csv',
                'changed_since' => InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE,
            ],
        ]);
        $inputTablesState = new InputTableStateList([
            [
                'source' => 'in.c-docker-test.test',
                'lastImportDate' => 'nonsense',
            ],
        ]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Invalid date format: nonsense');
        $reader->downloadTables(
            $configuration,
            $inputTablesState,
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true)
        );
    }
}
