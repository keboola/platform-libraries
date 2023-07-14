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
use Keboola\InputMapping\Tests\Needs\NeedsTestTables;
use Keboola\InputMapping\Tests\Needs\TestSatisfyer;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Psr\Log\Test\TestLogger;

class DownloadTablesWorkspaceRedshiftTest extends AbstractTestCase
{
    #[NeedsTestTables(2)]
    public function testTablesRedshiftBackend(): void
    {
        self::markTestSkipped('does not work');
        $logger = new TestLogger();
        $reader = new Reader(
            $this->getWorkspaceStagingFactory(
                logger: $logger,
                backend: [AbstractStrategyFactory::WORKSPACE_REDSHIFT, 'redshift']
            )
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
            new ReaderOptions(true)
        );

        /* because of https://keboola.atlassian.net/browse/KBC-228 we have to create redshift bucket to
            unload data from redshift workspace */
        $bucketId = TestSatisfyer::getBucketIdByDisplayName(
            $this->clientWrapper,
            'input-mapping-test-rs',
            Client::STAGE_OUT
        );
        if ($bucketId !== null) {
            $this->clientWrapper->getTableAndFileStorageClient()->dropBucket(
                $bucketId,
                ['force' => true, 'async' => true]
            );
        }

        $this->emptyOutputBucketId = $this->clientWrapper->getTableAndFileStorageClient()->createBucket(
            'input-mapping-test-rs',
            Client::STAGE_OUT,
            'Docker Testsuite',
            'redshift'
        );

        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test1.manifest');
        self::assertEquals($this->firstTableId, $manifest['id']);
        // test that the table exists in the workspace
        $tableId = $this->clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
            $bucketId,
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test1', 'name' => 'test1']
        );
        self::assertEquals($bucketId . '.test1', $tableId);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($tableId);
        self::assertEquals(['id'], $table['columns']);

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test2.manifest');
        self::assertEquals($this->secondTableId, $manifest['id']);
        $tableId = $this->clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
            $bucketId,
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test2', 'name' => 'test2']
        );
        self::assertEquals($bucketId . '.test2', $tableId);

        self::assertTrue($logger->hasInfoThatContains(
            sprintf('Table "%s" will be copied.', $this->firstTableId)
        ));
        self::assertTrue($logger->hasInfoThatContains(
            sprintf('Table "%s" will be copied.', $this->secondTableId)
        ));
        self::assertTrue($logger->hasInfoThatContains('Processed 1 workspace exports.'));
    }

    #[NeedsTestTables]
    public function testUseViewFails(): void
    {
        if (time() > 1) {
            $this->markTestSkipped('TODO fix test https://keboola.atlassian.net/browse/PST-961');
        }

        $logger = new TestLogger();
        $reader = new Reader(
            $this->getWorkspaceStagingFactory(
                logger: $logger,
                backend: [AbstractStrategyFactory::WORKSPACE_REDSHIFT, 'redshift']
            )
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
            'View load for table "test1" using backend "redshift" can\'t be used, only Synapse is supported.'
        );

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::WORKSPACE_REDSHIFT,
            new ReaderOptions(true)
        );
    }
}
