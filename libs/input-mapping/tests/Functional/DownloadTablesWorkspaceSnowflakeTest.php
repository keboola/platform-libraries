<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\InputMapping\Tests\AbstractTestCase;
use Keboola\InputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\InputMapping\Tests\Needs\NeedsTestTables;
use Keboola\StorageApi\ClientException;
use Psr\Log\Test\TestLogger;

class DownloadTablesWorkspaceSnowflakeTest extends AbstractTestCase
{
    #[NeedsTestTables(3), NeedsEmptyOutputBucket]
    public function testTablesSnowflakeBackend(): void
    {
        $logger = new TestLogger();
        $reader = new Reader($this->getWorkspaceStagingFactory(logger: $logger));
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test1',
            ],
            [
                'source' => $this->secondTableId,
                'destination' => 'test2',
                'where_column' => 'Id',
                'where_values' => ['id2', 'id3'],
                'columns' => ['Id'],
            ],
            [
                'source' => $this->thirdTableId,
                'destination' => 'test3',
                'keep_internal_timestamp_column' => false,
            ],
        ]);

        $result = $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
            new ReaderOptions(true)
        );

        // there were 2 jobs, clone and copy, so should have 2 metrics entries
        $metrics = $result->getMetrics()?->getTableMetrics();
        self::assertNotNull($metrics);
        self::assertCount(2, iterator_to_array($metrics));

        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test1.manifest');
        self::assertEquals($this->firstTableId, $manifest['id']);
        /* we want to check that the table exists in the workspace, so we try to load it, which fails, because of
            the _timestamp columns, but that's okay. It means that the table is indeed in the workspace. */
        try {
            $this->clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
                $this->firstTableId,
                ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test1', 'name' => 'test1']
            );
            self::fail('Must throw exception');
        } catch (ClientException $e) {
            self::assertStringContainsString('Invalid columns: _timestamp:', $e->getMessage());
        }

        // this is copy, so it doesn't contain the _timestamp column
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test2.manifest');
        self::assertEquals($this->secondTableId, $manifest['id']);
        $this->clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
            $this->emptyOutputBucketId,
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test2', 'name' => 'test2']
        );
        self::assertTrue($this->clientWrapper->getTableAndFileStorageClient()->tableExists($this->emptyOutputBucketId . '.test2'));

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test3.manifest');
        self::assertEquals($this->thirdTableId, $manifest['id']);
        /* we want to check that the table exists in the workspace, so we try to load it. This time it
            doesn't fail because keep_internal_timestamp_column=false was provided */
        $this->clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
            $this->emptyOutputBucketId,
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test3', 'name' => 'test3']
        );
        self::assertTrue($this->clientWrapper->getTableAndFileStorageClient()->tableExists($this->emptyOutputBucketId . '.test3'));
        self::assertTrue($logger->hasInfoThatContains('Using "workspace-snowflake" table input staging.'));
        self::assertTrue($logger->hasInfoThatContains(sprintf('Table "%s" will be cloned.', $this->firstTableId)));
        self::assertTrue($logger->hasInfoThatContains(sprintf('Table "%s" will be copied.', $this->secondTableId)));
        self::assertTrue($logger->hasInfoThatContains(sprintf('Table "%s" will be cloned.', $this->thirdTableId)));
        self::assertTrue($logger->hasInfoThatContains('Cloning 2 tables to workspace.'));
        self::assertTrue($logger->hasInfoThatContains('Copying 1 tables to workspace.'));
        self::assertTrue($logger->hasInfoThatContains('Processed 2 workspace exports.'));
        // test that the clone jobs are merged into a single one
        sleep(2);
        $jobs = $this->clientWrapper->getTableAndFileStorageClient()->listJobs(['limit' => 20]);
        $params = null;
        foreach ($jobs as $job) {
            if ($job['operationName'] === 'workspaceLoadClone') {
                $params = $job['operationParams'];
                break;
            }
        }
        self::assertNotEmpty($params);
        self::assertEquals(2, count($params['input']));
        self::assertEquals('test1', $params['input'][0]['destination']);
        self::assertEquals('test3', $params['input'][1]['destination']);
    }

    #[NeedsTestTables(2)]
    public function testTablesInvalidMapping(): void
    {
        $reader = new Reader($this->getWorkspaceStagingFactory());
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test1',
                'changed_since' => 'adaptive',
            ],
            [
                'source' => $this->secondTableId,
                'destination' => 'test2',
            ],
        ]);

        $this->expectException(InvalidInputException::class);
        $this->expectExceptionMessage('Adaptive input mapping is not supported on input mapping to workspace.');
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
            new ReaderOptions(true)
        );
    }

    #[NeedsTestTables, NeedsEmptyOutputBucket]
    public function testTablesSnowflakeDataTypes(): void
    {
        $logger = new TestLogger();
        $reader = new Reader($this->getWorkspaceStagingFactory(logger: $logger));
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test2',
                'where_column' => 'Id',
                'where_values' => ['id2', 'id3'],
                'column_types' => [
                    [
                        'source' => 'Id',
                        'destination' => 'MyId',
                        'type' => 'VARCHAR',
                        'convert_empty_values_to_null' => true,
                    ],
                ],
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
            new ReaderOptions(true)
        );

        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test2.manifest');
        self::assertEquals($this->firstTableId, $manifest['id']);
        self::assertEquals(
            ['Id'],
            $manifest['columns']
        );
        // check that the table exists in the workspace
        $this->clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
            $this->emptyOutputBucketId,
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test2', 'name' => 'test2']
        );

        self::assertTrue($logger->hasInfoThatContains('Using "workspace-snowflake" table input staging.'));
        self::assertTrue($logger->hasInfoThatContains(sprintf('Table "%s" will be copied.', $this->firstTableId)));
        self::assertTrue($logger->hasInfoThatContains('Copying 1 tables to workspace.'));
        self::assertTrue($logger->hasInfoThatContains('Processed 1 workspace exports.'));
    }

    #[NeedsTestTables]
    public function testTablesSnowflakeDataTypesInvalid(): void
    {
        $reader = new Reader($this->getWorkspaceStagingFactory());
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test2',
                'where_column' => 'Id',
                'where_values' => ['id2'],
                'column_types' => [
                    [
                        'source' => 'Id',
                        'destination' => 'MyId',
                        'type' => 'NUMERIC',
                    ],
                ],
            ],
        ]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'Likely datatype conversion: odbc_execute(): SQL error: Numeric value \'id2\' is not recognized'
        );
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
            new ReaderOptions(true)
        );
    }

    #[NeedsTestTables, NeedsEmptyOutputBucket]
    public function testTablesSnowflakeOverwrite(): void
    {
        $logger = new TestLogger();
        $reader = new Reader($this->getWorkspaceStagingFactory(logger: $logger));
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test2',
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
            new ReaderOptions(true)
        );
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test2',
                'where_column' => 'Id',
                'where_values' => ['id2', 'id3'],
                'columns' => ['Id'],
                'overwrite' => true,
            ],
        ]);
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
            new ReaderOptions(true)
        );
        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test2.manifest');
        self::assertEquals($this->firstTableId, $manifest['id']);
        self::assertEquals(
            ['Id'],
            $manifest['columns']
        );
        // check that the table exists in the workspace
        $this->clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
            $this->emptyOutputBucketId,
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test2', 'name' => 'test2']
        );

        self::assertTrue($logger->hasInfoThatContains('Using "workspace-snowflake" table input staging.'));
        self::assertTrue($logger->hasInfoThatContains(sprintf('Table "%s" will be copied.', $this->firstTableId)));
        self::assertTrue($logger->hasInfoThatContains('Copying 1 tables to workspace.'));
        self::assertTrue($logger->hasInfoThatContains('Processed 1 workspace exports.'));

        // check that we can overwrite while using clone
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test2',
                'overwrite' => true,
            ],
        ]);
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
            new ReaderOptions(true)
        );
        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test2.manifest');
        self::assertEquals($this->firstTableId, $manifest['id']);
        self::assertEquals(
            ['Id', 'Name', 'foo', 'bar'],
            $manifest['columns']
        );
        /* we want to check that the table exists in the workspace, so we try to load it, which fails, because of
            the _timestamp columns, but that's okay. It means that the table is indeed in the workspace. */
        try {
            $this->clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
                $this->emptyOutputBucketId,
                ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test2', 'name' => 'test2', 'columns']
            );
            self::fail('Must throw exception');
        } catch (ClientException $e) {
            self::assertStringContainsString('Invalid columns: _timestamp:', $e->getMessage());
        }
        self::assertTrue($logger->hasInfoThatContains('Using "workspace-snowflake" table input staging.'));
        self::assertTrue($logger->hasInfoThatContains(sprintf('Table "%s" will be cloned.', $this->firstTableId)));
        self::assertTrue($logger->hasInfoThatContains('Cloning 1 tables to workspace.'));
        self::assertTrue($logger->hasInfoThatContains('Processed 1 workspace exports.'));
    }

    #[NeedsTestTables]
    public function testUseViewFails(): void
    {
        if (time() > 1) {
            $this->markTestSkipped('TODO fix test https://keboola.atlassian.net/browse/PST-961');
        }

        $logger = new TestLogger();
        $reader = new Reader($this->getWorkspaceStagingFactory(logger: $logger));
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test1',
                'limit' => 100,
                'use_view' => true,
            ],
        ]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'View load for table "test1" using backend "snowflake" can\'t be used, only Synapse is supported.'
        );

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
            new ReaderOptions(true)
        );
    }

    #[NeedsTestTables(2), NeedsEmptyOutputBucket]
    public function testDownloadTablesPreserveFalse(): void
    {
        // first we create the workspace and load there some data.
        // then we will do a new load with preserve=false to make sure that the old data was removed
        $logger = new TestLogger();
        $reader = new Reader($this->getWorkspaceStagingFactory(logger: $logger));
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'initial_table',
                'where_column' => 'Id',
                'where_values' => ['id2', 'id3'],
                'columns' => ['Id'],
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
            new ReaderOptions(true)
        );
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'new_clone_table',
            ],
            [
                'source' => $this->firstTableId,
                'destination' => 'new_copy_table',
                'where_column' => 'Id',
                'where_values' => ['id2', 'id3'],
                'columns' => ['Id'],
            ],
        ]);
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
            new ReaderOptions(true, false)
        );
        // the initial_table should not be present in the workspace anymore
        try {
            $this->clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
                $this->emptyOutputBucketId,
                [
                    'dataWorkspaceId' => $this->workspaceId,
                    'dataTableName' => 'initial_table',
                    'name' => 'initial_table',
                ]
            );
            self::fail('should throw 404 for workspace table not found');
        } catch (ClientException $exception) {
            self::assertStringContainsString(
                'Table "initial_table" not found in schema',
                $exception->getMessage()
            );
        }

        // check that the tables exist in the workspace. the cloned table will throw the _timestamp col error
        try {
            $this->clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
                $this->emptyOutputBucketId,
                [
                    'dataWorkspaceId' => $this->workspaceId,
                    'dataTableName' => 'new_clone_table',
                    'name' => 'new_clone_table',
                ]
            );
        } catch (ClientException $exception) {
            self::assertStringContainsString('Invalid columns: _timestamp:', $exception->getMessage());
        }

        $this->clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
            $this->emptyOutputBucketId,
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'new_copy_table', 'name' => 'new_clopy_table']
        );
    }
}
