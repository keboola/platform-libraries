<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptions;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\InputMapping\Tests\AbstractTestCase;
use Keboola\InputMapping\Tests\Needs\NeedsTestTables;
use Keboola\StorageApi\ClientException;

class DownloadTablesAdaptiveTest extends AbstractTestCase
{
    #[NeedsTestTables]
    public function testDownloadTablesDownloadsEmptyTable(): void
    {
        $clientWrapper = $this->initClient();
        $reader = new Reader($this->getLocalStagingFactory($clientWrapper));
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test.csv',
                'changed_since' => InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE,
            ],
        ]);
        $testTableInfo = $clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId);
        $inputTablesState = new InputTableStateList([
            [
                'source' => $this->firstTableId,
                'lastImportDate' => $testTableInfo['lastImportDate'],
            ],
        ]);
        $tablesResult = $reader->downloadTables(
            $configuration,
            $inputTablesState,
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true),
        );

        self::assertEquals(
            $testTableInfo['lastImportDate'],
            $tablesResult->getInputTableStateList()->getTable($this->firstTableId)->getLastImportDate(),
        );
        self::assertCSVEquals(
            "\"Id\",\"Name\",\"foo\",\"bar\"\n",
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download/test.csv',
        );
        self::assertCount(1, $tablesResult->getInputTableStateList()->jsonSerialize());
    }

    #[NeedsTestTables]
    public function testDownloadTablesDownloadsOnlyNewRows(): void
    {
        $clientWrapper = $this->initClient();
        $reader = new Reader($this->getLocalStagingFactory($clientWrapper));
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test.csv',
                'changed_since' => InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE,
            ],
        ]);
        $firstTablesResult = $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true),
        );

        // Update table
        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload.csv');
        $csv->writeRow(['Id', 'Name', 'foo', 'bar']);
        $csv->writeRow(['id4', 'name4', 'foo4', 'bar4']);
        $clientWrapper->getTableAndFileStorageClient()->writeTableAsync(
            $this->firstTableId,
            $csv,
            ['incremental' => true],
        );

        $updatedTestTableInfo = $clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId);
        $secondTablesResult = $reader->downloadTables(
            $configuration,
            $firstTablesResult->getInputTableStateList(),
            'data/in/tables/',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true),
        );

        self::assertEquals(
            $updatedTestTableInfo['lastImportDate'],
            $secondTablesResult->getInputTableStateList()->getTable($this->firstTableId)->getLastImportDate(),
        );
        self::assertCSVEquals(
            "\"Id\",\"Name\",\"foo\",\"bar\"\n\"id4\",\"name4\",\"foo4\",\"bar4\"\n",
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'data/in/tables/test.csv',
        );
        self::assertCount(1, $secondTablesResult->getInputTableStateList()->jsonSerialize());
    }

    #[NeedsTestTables]
    public function testDownloadTablesInvalidDate(): void
    {
        $reader = new Reader($this->getLocalStagingFactory($this->initClient()));
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test.csv',
                'changed_since' => InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE,
            ],
        ]);
        $inputTablesState = new InputTableStateList([
            [
                'source' => $this->firstTableId,
                'lastImportDate' => 'nonsense',
            ],
        ]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Invalid parameters - changedSince: changedSince has to be '
            . 'compatible with strtotime() or to be a unix timestamp.');
        $reader->downloadTables(
            $configuration,
            $inputTablesState,
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true),
        );
    }
}
