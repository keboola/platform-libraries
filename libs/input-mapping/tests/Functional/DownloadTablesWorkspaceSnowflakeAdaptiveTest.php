<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptions;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\InputMapping\Tests\AbstractTestCase;
use Keboola\InputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\InputMapping\Tests\Needs\NeedsTestTables;
use Keboola\StorageApi\ClientException;

class DownloadTablesWorkspaceSnowflakeAdaptiveTest extends AbstractTestCase
{
    #[NeedsTestTables, NeedsEmptyOutputBucket]
    public function testDownloadTablesDownloadsEmptyTable(): void
    {
        $clientWrapper = $this->initClient();
        $reader = new Reader($this->getWorkspaceStagingFactory(
            clientWrapper: $clientWrapper,
            logger: $this->testLogger,
        ));
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test1',
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
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
            new ReaderOptions(true),
        );

        self::assertEquals(
            $testTableInfo['lastImportDate'],
            $tablesResult->getInputTableStateList()->getTable($this->firstTableId)->getLastImportDate(),
        );

        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test1.manifest');
        self::assertEquals($this->firstTableId, $manifest['id']);

        // check that the table exists in the workspace
        $clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
            $this->emptyOutputBucketId,
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test1', 'name' => 'test1'],
        );

        self::assertCount(1, $tablesResult->getInputTableStateList()->jsonSerialize());
        self::assertTrue($this->testHandler->hasInfoThatContains('Using "workspace-snowflake" table input staging.'));
        self::assertTrue(
            $this->testHandler->hasInfoThatContains(sprintf('Table "%s" will be copied.', $this->firstTableId)),
        );
        self::assertTrue($this->testHandler->hasInfoThatContains('Copying 1 tables to workspace.'));
        self::assertTrue($this->testHandler->hasInfoThatContains('Processed 1 workspace exports.'));
    }

    #[NeedsTestTables, NeedsEmptyOutputBucket]
    public function testDownloadTablesDownloadsOnlyNewRows(): void
    {
        $clientWrapper = $this->initClient();
        $reader = new Reader($this->getWorkspaceStagingFactory(
            clientWrapper: $clientWrapper,
            logger: $this->testLogger,
        ));
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test1',
                'changed_since' => InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE,
            ],
        ]);
        $firstTablesResult = $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
            new ReaderOptions(true),
        );

        // Update the source table
        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload.csv');
        $csv->writeRow(['Id', 'Name', 'foo', 'bar']);
        $csv->writeRow(['id4', 'name4', 'foo4', 'bar4']);
        $clientWrapper->getTableAndFileStorageClient()->writeTableAsync(
            $this->firstTableId,
            $csv,
            ['incremental' => true],
        );
        $updatedTestTableInfo = $clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId);
        // now the source table has 4 rows (it has 3 originally)
        self::assertEquals(4, $updatedTestTableInfo['rowsCount']);

        // load the table to workspace again
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test1',
                'changed_since' => InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE,
                'overwrite' => true,
            ],
        ]);
        $secondTablesResult = $reader->downloadTables(
            $configuration,
            $firstTablesResult->getInputTableStateList(),
            'data/in/tables/',
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
            new ReaderOptions(true),
        );

        self::assertEquals(
            $updatedTestTableInfo['lastImportDate'],
            $secondTablesResult->getInputTableStateList()->getTable($this->firstTableId)->getLastImportDate(),
        );
        self::assertCount(1, $secondTablesResult->getInputTableStateList()->jsonSerialize());

        // create a Storage table from the workspace table
        $clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
            $this->emptyOutputBucketId,
            [
                'dataWorkspaceId' => $this->workspaceId,
                'dataObject' => 'test1',
                'name' => 'testWorkspace1',
            ],
        );
        // assert it has just the 1 new row
        $workspaceTableInfo = $clientWrapper->getTableAndFileStorageClient()
            ->getTable($this->emptyOutputBucketId . '.testWorkspace1');
        self::assertEquals(1, $workspaceTableInfo['rowsCount']);
    }

    #[NeedsTestTables]
    public function testDownloadTablesInvalidDate(): void
    {
        $reader = new Reader($this->getWorkspaceStagingFactory(
            clientWrapper: $this->initClient(),
            logger: $this->testLogger,
        ));
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test1',
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
        $this->expectExceptionMessage('Invalid column definition: Invalid "changedSince" parameter');
        $reader->downloadTables(
            $configuration,
            $inputTablesState,
            'download',
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
            new ReaderOptions(true),
        );
    }
}
