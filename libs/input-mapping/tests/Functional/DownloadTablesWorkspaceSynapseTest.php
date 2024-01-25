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
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use Throwable;

#[NeedsStorageBackend('synapse')]
class DownloadTablesWorkspaceSynapseTest extends AbstractTestCase
{
    private bool $runSynapseTests = false;

    public function setUp(): void
    {
        $this->runSynapseTests = (bool) getenv('RUN_SYNAPSE_TESTS');
        if (!$this->runSynapseTests) {
            return;
        }
        parent::setUp();
    }

    protected function initClient(): void
    {
        $this->clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('SYNAPSE_STORAGE_API_URL'),
                (string) getenv('SYNAPSE_STORAGE_API_TOKEN'),
            ),
        );
        $tokenInfo = $this->clientWrapper->getBranchClient()->verifyToken();
        print(sprintf(
            'Authorized as "%s (%s)" to project "%s (%s)" at "%s" stack.',
            $tokenInfo['description'],
            $tokenInfo['id'],
            $tokenInfo['owner']['name'],
            $tokenInfo['owner']['id'],
            $this->clientWrapper->getBranchClient()->getApiUrl(),
        ));
    }

    #[NeedsTestTables(2), NeedsEmptyOutputBucket]
    public function testTablesSynapseBackend(): void
    {
        if (!$this->runSynapseTests) {
            self::markTestSkipped('Synapse tests disabled');
        }
        $testHandler = new TestHandler();
        $logger = new Logger('testLogger', [$testHandler]);
        $reader = new Reader(
            $this->getWorkspaceStagingFactory(
                null,
                'json',
                $logger,
                [AbstractStrategyFactory::WORKSPACE_SYNAPSE, 'synapse'],
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
            [
                'source' => $this->firstTableId,
                'destination' => 'test3',
                'use_view' => true,
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::WORKSPACE_SYNAPSE,
            new ReaderOptions(true),
        );

        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test1.manifest');

        self::assertEquals($this->firstTableId, $manifest['id']);
        // test that the table exists in the workspace
        $tableId = $this->clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
            $this->emptyOutputBucketId,
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test1', 'name' => 'test1'],
        );
        self::assertEquals($this->emptyOutputBucketId . '.test1', $tableId);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($tableId);
        self::assertEquals(['Id'], $table['columns']);

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test2.manifest');
        self::assertEquals($this->secondTableId, $manifest['id']);
        $tableId = $this->clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
            $this->emptyOutputBucketId,
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test2', 'name' => 'test2'],
        );
        self::assertEquals($this->emptyOutputBucketId . '.test2', $tableId);

        // check table test3
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test1.manifest');
        self::assertEquals($this->firstTableId, $manifest['id']);

        try {
            $this->clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
                $this->emptyOutputBucketId,
                ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test3', 'name' => 'test3'],
            );

            self::fail('Exception was expected');
        } catch (Throwable $e) {
            self::assertInstanceOf(ClientException::class, $e);
            self::assertStringStartsWith('Invalid columns: _timestamp', $e->getMessage());
        }

        self::assertTrue($testHandler->hasInfoThatContains(sprintf('Table "%s" will be copied.', $this->firstTableId)));
        self::assertTrue($testHandler->hasInfoThatContains(sprintf('Table "%s" will be copied.', $this->secondTableId)));
        self::assertTrue($testHandler->hasInfoThatContains('Processing 1 workspace exports.'));

        // test loading with preserve = false to clean the workspace
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test1',
                'changed_since' => '-2 days',
                'columns' => ['Id'],
            ],
        ]);
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::WORKSPACE_SYNAPSE,
            new ReaderOptions(true, false),
        );
        // the initially loaded tables should not be present in the workspace anymore
        try {
            $this->clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
                $this->emptyOutputBucketId,
                ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test2', 'name' => 'test2'],
            );
            self::fail('should throw 404 for workspace table not found');
        } catch (ClientException $exception) {
            self::assertStringContainsString('Table "test2" not found in schema', $exception->getMessage());
        }
        try {
            $this->clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
                $this->emptyOutputBucketId,
                ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test3', 'name' => 'test3'],
            );
            self::fail('should throw 404 for workspace table not found');
        } catch (ClientException $exception) {
            self::assertStringContainsString('Table "test3" not found in schema', $exception->getMessage());
        }
    }
}
