<?php

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\StorageApi\ClientException;
use Psr\Log\Test\TestLogger;

class DownloadTablesWorkspaceSnowflakeTest extends DownloadTablesWorkspaceTestAbstract
{
    public function testTablesSnowflakeBackend()
    {
        $logger = new TestLogger();
        $reader = new Reader($this->getStagingFactory(null, 'json', $logger, [StrategyFactory::WORKSPACE_SNOWFLAKE, 'snowflake']));
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-input-mapping-test.test1',
                'destination' => 'test1',
            ],
            [
                'source' => 'in.c-input-mapping-test.test2',
                'destination' => 'test2',
                'where_column' => 'Id',
                'where_values' => ['id2', 'id3'],
                'columns' => ['Id'],
            ],
            [
                'source' => 'in.c-input-mapping-test.test3',
                'destination' => 'test3',
            ],
        ]);

        $result = $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            StrategyFactory::WORKSPACE_SNOWFLAKE,
            new ReaderOptions(true)
        );

        // there were 2 jobs, clone and copy, so should have 2 metrics entries
        $this->assertCount(2, $result->getMetrics()->getTableMetrics());

        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test1.manifest');
        self::assertEquals('in.c-input-mapping-test.test1', $manifest['id']);
        /* we want to check that the table exists in the workspace, so we try to load it, which fails, because of
            the _timestamp columns, but that's okay. It means that the table is indeed in the workspace. */
        try {
            $this->clientWrapper->getBasicClient()->createTableAsyncDirect(
                'out.c-input-mapping-test',
                ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test1', 'name' => 'test1']
            );
            self::fail('Must throw exception');
        } catch (ClientException $e) {
            self::assertContains('Invalid columns: _timestamp:', $e->getMessage());
        }

        // this is copy, so it doesn't contain the _timestamp column
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test2.manifest');
        self::assertEquals('in.c-input-mapping-test.test2', $manifest['id']);
        $this->clientWrapper->getBasicClient()->createTableAsyncDirect(
            'out.c-input-mapping-test',
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test2', 'name' => 'test2']
        );

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test3.manifest');
        self::assertEquals('in.c-input-mapping-test.test3', $manifest['id']);
        /* we want to check that the table exists in the workspace, so we try to load it, which fails, because of
            the _timestamp columns, but that's okay. It means that the table is indeed in the workspace. */
        try {
            $this->clientWrapper->getBasicClient()->createTableAsyncDirect(
                'out.c-input-mapping-test',
                ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test3', 'name' => 'test3']
            );
            self::fail('Must throw exception');
        } catch (ClientException $e) {
            self::assertContains('Invalid columns: _timestamp:', $e->getMessage());
        }
        self::assertTrue($logger->hasInfoThatContains('Using "workspace-snowflake" table input staging.'));
        self::assertTrue($logger->hasInfoThatContains('Table "in.c-input-mapping-test.test1" will be cloned.'));
        self::assertTrue($logger->hasInfoThatContains('Table "in.c-input-mapping-test.test2" will be copied.'));
        self::assertTrue($logger->hasInfoThatContains('Table "in.c-input-mapping-test.test3" will be cloned.'));
        self::assertTrue($logger->hasInfoThatContains('Cloning 2 tables to workspace.'));
        self::assertTrue($logger->hasInfoThatContains('Copying 1 tables to workspace.'));
        self::assertTrue($logger->hasInfoThatContains('Processed 2 workspace exports.'));
        // test that the clone jobs are merged into a single one
        sleep(2);
        $jobs = $this->clientWrapper->getBasicClient()->listJobs(['limit' => 20]);
        $params = null;
        foreach ($jobs as $job) {
            if ($job['operationName'] === 'workspaceLoadClone') {
                $params = $job['operationParams'];
            }
        }
        self::assertNotEmpty($params);
        self::assertEquals(2, count($params['input']));
        self::assertEquals('test1', $params['input'][0]['destination']);
        self::assertEquals('test3', $params['input'][1]['destination']);
    }

    public function testTablesInvalidMapping()
    {
        $reader = new Reader($this->getStagingFactory(null, 'json', null, [StrategyFactory::WORKSPACE_SNOWFLAKE, 'snowflake']));
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-input-mapping-test.test1',
                'destination' => 'test1',
                'changed_since' => 'adaptive',
            ],
            [
                'source' => 'in.c-input-mapping-test.test2',
                'destination' => 'test2',
            ]
        ]);

        self::expectException(InvalidInputException::class);
        self::expectExceptionMessage('Adaptive input mapping is not supported on input mapping to workspace.');
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            StrategyFactory::WORKSPACE_SNOWFLAKE,
            new ReaderOptions(true)
        );
    }

    public function testTablesSnowflakeDataTypes()
    {
        $logger = new TestLogger();
        $reader = new Reader($this->getStagingFactory(null, 'json', $logger, [StrategyFactory::WORKSPACE_SNOWFLAKE, 'snowflake']));
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-input-mapping-test.test2',
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
            ]
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            StrategyFactory::WORKSPACE_SNOWFLAKE,
            new ReaderOptions(true)
        );

        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test2.manifest');
        self::assertEquals('in.c-input-mapping-test.test2', $manifest['id']);
        self::assertEquals(
            ['Id'],
            $manifest['columns']
        );
        // check that the table exists in the workspace
        $this->clientWrapper->getBasicClient()->createTableAsyncDirect(
            'out.c-input-mapping-test',
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test2', 'name' => 'test2']
        );

        self::assertTrue($logger->hasInfoThatContains('Using "workspace-snowflake" table input staging.'));
        self::assertTrue($logger->hasInfoThatContains('Table "in.c-input-mapping-test.test2" will be copied.'));
        self::assertTrue($logger->hasInfoThatContains('Copying 1 tables to workspace.'));
        self::assertTrue($logger->hasInfoThatContains('Processing 1 workspace exports.'));
    }

    public function testTablesSnowflakeDataTypesInvalid()
    {
        $reader = new Reader($this->getStagingFactory(null, 'json', null, [StrategyFactory::WORKSPACE_SNOWFLAKE, 'snowflake']));
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-input-mapping-test.test2',
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
            ]
        ]);

        self::expectException(ClientException::class);
        self::expectExceptionMessage(
            'Likely datatype conversion: odbc_execute(): SQL error: Numeric value \'id2\' is not recognized'
        );
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            StrategyFactory::WORKSPACE_SNOWFLAKE,
            new ReaderOptions(true)
        );
    }

    public function testTablesSnowflakeOverwrite()
    {
        $logger = new TestLogger();
        $reader = new Reader($this->getStagingFactory(null, 'json', $logger, [StrategyFactory::WORKSPACE_SNOWFLAKE, 'snowflake']));
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-input-mapping-test.test2',
                'destination' => 'test2',
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            StrategyFactory::WORKSPACE_SNOWFLAKE,
            new ReaderOptions(true)
        );
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-input-mapping-test.test2',
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
            StrategyFactory::WORKSPACE_SNOWFLAKE,
            new ReaderOptions(true)
        );
        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test2.manifest');
        self::assertEquals('in.c-input-mapping-test.test2', $manifest['id']);
        self::assertEquals(
            ['Id'],
            $manifest['columns']
        );
        // check that the table exists in the workspace
        $this->clientWrapper->getBasicClient()->createTableAsyncDirect(
            'out.c-input-mapping-test',
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test2', 'name' => 'test2']
        );

        self::assertTrue($logger->hasInfoThatContains('Using "workspace-snowflake" table input staging.'));
        self::assertTrue($logger->hasInfoThatContains('Table "in.c-input-mapping-test.test2" will be copied.'));
        self::assertTrue($logger->hasInfoThatContains('Copying 1 tables to workspace.'));
        self::assertTrue($logger->hasInfoThatContains('Processing 1 workspace exports.'));

        // check that we can overwrite while using clone
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-input-mapping-test.test2',
                'destination' => 'test2',
                'overwrite' => true,
            ],
        ]);
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            StrategyFactory::WORKSPACE_SNOWFLAKE,
            new ReaderOptions(true)
        );
        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test2.manifest');
        self::assertEquals('in.c-input-mapping-test.test2', $manifest['id']);
        self::assertEquals(
            ['Id', 'Name', 'foo', 'bar'],
            $manifest['columns']
        );
        /* we want to check that the table exists in the workspace, so we try to load it, which fails, because of
            the _timestamp columns, but that's okay. It means that the table is indeed in the workspace. */
        try {
            $this->clientWrapper->getBasicClient()->createTableAsyncDirect(
                'out.c-input-mapping-test',
                ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test2', 'name' => 'test2', 'columns']
            );
            self::fail('Must throw exception');
        } catch (ClientException $e) {
            self::assertContains('Invalid columns: _timestamp:', $e->getMessage());
        }
        self::assertTrue($logger->hasInfoThatContains('Using "workspace-snowflake" table input staging.'));
        self::assertTrue($logger->hasInfoThatContains('Table "in.c-input-mapping-test.test2" will be cloned.'));
        self::assertTrue($logger->hasInfoThatContains('Cloning 1 tables to workspace.'));
        self::assertTrue($logger->hasInfoThatContains('Processing 1 workspace exports.'));
    }

    public function testUseViewFails()
    {
        $logger = new TestLogger();
        $reader = new Reader($this->getStagingFactory(null, 'json', $logger, [StrategyFactory::WORKSPACE_SNOWFLAKE, 'snowflake']));
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-input-mapping-test.test1',
                'destination' => 'test1',
                'limit' => 100,
                'use_view' => true,
            ]
        ]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('View load for table "test1" using backend "snowflake" can\'t be used, only Synapse is supported.');

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            StrategyFactory::WORKSPACE_SNOWFLAKE,
            new ReaderOptions(true)
        );
    }

    public function testDownloadTablesPreserveFalse()
    {
        // first we create the workspace and load there some data.
        // then we will do a new load with preserve=false to make sure that the old data was removed
        $logger = new TestLogger();
        $reader = new Reader($this->getStagingFactory(null, 'json', $logger, [StrategyFactory::WORKSPACE_SNOWFLAKE, 'snowflake']));
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-input-mapping-test.test2',
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
            StrategyFactory::WORKSPACE_SNOWFLAKE,
            new ReaderOptions(true)
        );
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-input-mapping-test.test2',
                'destination' => 'new_clone_table',
            ],
            [
                'source' => 'in.c-input-mapping-test.test2',
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
            StrategyFactory::WORKSPACE_SNOWFLAKE,
            new ReaderOptions(true, false)
        );
        // the initial_table should not be present in the workspace anymore
        try {
            $this->clientWrapper->getBasicClient()->createTableAsyncDirect(
                'out.c-input-mapping-test',
                ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'initial_table', 'name' => 'initial_table']
            );
            self::fail('should throw 404 for workspace table not found');
        } catch (ClientException $exception) {
            self::assertContains('Table "initial_table" not found in schema', $exception->getMessage());
        }

        // check that the tables exist in the workspace. the cloned table will throw the _timestamp col error
        try {
            $this->clientWrapper->getBasicClient()->createTableAsyncDirect(
                'out.c-input-mapping-test',
                ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'new_clone_table', 'name' => 'new_clone_table']
            );
        } catch (ClientException $exception) {
            self::assertContains('Invalid columns: _timestamp:', $exception->getMessage());
        }

        $this->clientWrapper->getBasicClient()->createTableAsyncDirect(
            'out.c-input-mapping-test',
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'new_copy_table', 'name' => 'new_clopy_table']
        );
    }
}
