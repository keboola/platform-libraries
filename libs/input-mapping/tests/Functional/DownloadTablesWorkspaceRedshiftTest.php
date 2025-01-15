<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\InputMapping\Tests\AbstractTestCase;
use Keboola\InputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\InputMapping\Tests\Needs\NeedsStorageBackend;
use Keboola\InputMapping\Tests\Needs\NeedsTestTables;
use Keboola\StorageApi\ClientException;

#[NeedsStorageBackend('redshift')]
class DownloadTablesWorkspaceRedshiftTest extends AbstractTestCase
{
    #[NeedsTestTables(2), NeedsEmptyOutputBucket]
    public function testTablesRedshiftBackend(): void
    {
        $clientWrapper = $this->initClient();
        $reader = new Reader(
            $this->getWorkspaceStagingFactory(
                clientWrapper: $clientWrapper,
                logger: $this->testLogger,
                backend: [AbstractStrategyFactory::WORKSPACE_REDSHIFT, 'redshift'],
            ),
        );
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test1',
                'changed_since' => '-2 days',
                'columns' => ['Id'],
            ],
            [
                'source' => $this->secondTableId,
                'destination' => 'test2',
                'column_types' => [
                    [
                        'source' => 'Id',
                        'type' => 'VARCHAR',
                    ],
                    [
                        'source' => 'Name',
                        'type' => 'VARCHAR',
                    ],
                ],
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::WORKSPACE_REDSHIFT,
            new ReaderOptions(true),
        );

        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test1.manifest');
        self::assertEquals($this->firstTableId, $manifest['id']);
        // test that the table exists in the workspace
        $tableId = $clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
            $this->emptyOutputBucketId,
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test1', 'name' => 'test1'],
        );
        self::assertEquals($this->emptyOutputBucketId . '.test1', $tableId);
        $table = $clientWrapper->getTableAndFileStorageClient()->getTable($tableId);
        self::assertEquals(['id'], $table['columns']);

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test2.manifest');
        self::assertEquals($this->secondTableId, $manifest['id']);
        $tableId = $clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
            $this->emptyOutputBucketId,
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test2', 'name' => 'test2'],
        );
        self::assertEquals($this->emptyOutputBucketId . '.test2', $tableId);

        self::assertTrue($this->testHandler->hasInfoThatContains(
            sprintf('Table "%s" will be copied.', $this->firstTableId),
        ));
        self::assertTrue($this->testHandler->hasInfoThatContains(
            sprintf('Table "%s" will be copied.', $this->secondTableId),
        ));
        self::assertTrue($this->testHandler->hasInfoThatContains('Processed 1 workspace exports.'));
    }

    #[NeedsTestTables]
    public function testUseViewFails(): void
    {
        $reader = new Reader(
            $this->getWorkspaceStagingFactory(
                clientWrapper: $this->initClient(),
                logger: $this->testLogger,
                backend: [AbstractStrategyFactory::WORKSPACE_REDSHIFT, 'redshift'],
            ),
        );
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test1',
                'use_view' => true,
            ],
        ]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'View load for table "test1" is not supported for "redshift", only the following backends are supported: '
            . '"synapse", "snowflake", "bigquery".',
        );

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::WORKSPACE_REDSHIFT,
            new ReaderOptions(true),
        );
    }
}
