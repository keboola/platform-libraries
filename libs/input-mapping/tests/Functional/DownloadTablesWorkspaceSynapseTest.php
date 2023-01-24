<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Psr\Log\Test\TestLogger;
use Throwable;

class DownloadTablesWorkspaceSynapseTest extends DownloadTablesWorkspaceTestAbstract
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
                (string) getenv('SYNAPSE_STORAGE_API_TOKEN')
            ),
        );
        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        print(sprintf(
            'Authorized as "%s (%s)" to project "%s (%s)" at "%s" stack.',
            $tokenInfo['description'],
            $tokenInfo['id'],
            $tokenInfo['owner']['name'],
            $tokenInfo['owner']['id'],
            $this->clientWrapper->getBasicClient()->getApiUrl()
        ));
    }

    public function testTablesSynapseBackend(): void
    {
        if (!$this->runSynapseTests) {
            self::markTestSkipped('Synapse tests disabled');
        }
        $logger = new TestLogger();
        $reader = new Reader(
            $this->getStagingFactory(
                null,
                'json',
                $logger,
                [AbstractStrategyFactory::WORKSPACE_SYNAPSE, 'synapse']
            )
        );
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-input-mapping-test.test1',
                'destination' => 'test1',
                'changed_since' => '-2 days',
                'columns' => ['Id'],
            ],
            [
                'source' => 'in.c-input-mapping-test.test2',
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
                'source' => 'in.c-input-mapping-test.test1',
                'destination' => 'test3',
                'use_view' => true,
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::WORKSPACE_SYNAPSE,
            new ReaderOptions(true)
        );

        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test1.manifest');
        $this->clientWrapper->getBasicClient()->dropBucket('out.c-input-mapping-test');
        $this->clientWrapper->getBasicClient()->createBucket(
            'input-mapping-test',
            Client::STAGE_OUT,
            'Docker Testsuite',
            'synapse'
        );

        self::assertEquals('in.c-input-mapping-test.test1', $manifest['id']);
        // test that the table exists in the workspace
        $tableId = $this->clientWrapper->getBasicClient()->createTableAsyncDirect(
            'out.c-input-mapping-test',
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test1', 'name' => 'test1']
        );
        self::assertEquals('out.c-input-mapping-test.test1', $tableId);
        $table = $this->clientWrapper->getBasicClient()->getTable($tableId);
        self::assertEquals(['Id'], $table['columns']);

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test2.manifest');
        self::assertEquals('in.c-input-mapping-test.test2', $manifest['id']);
        $tableId = $this->clientWrapper->getBasicClient()->createTableAsyncDirect(
            'out.c-input-mapping-test',
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test2', 'name' => 'test2']
        );
        self::assertEquals('out.c-input-mapping-test.test2', $tableId);

        // check table test3
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test1.manifest');
        self::assertEquals('in.c-input-mapping-test.test1', $manifest['id']);

        try {
            $this->clientWrapper->getBasicClient()->createTableAsyncDirect(
                'out.c-input-mapping-test',
                ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test3', 'name' => 'test3']
            );

            self::fail('Exception was expected');
        } catch (Throwable $e) {
            self::assertInstanceOf(ClientException::class, $e);
            self::assertStringStartsWith('Invalid columns: _timestamp', $e->getMessage());
        }

        self::assertTrue($logger->hasInfoThatContains('Table "in.c-input-mapping-test.test1" will be copied.'));
        self::assertTrue($logger->hasInfoThatContains('Table "in.c-input-mapping-test.test2" will be copied.'));
        self::assertTrue($logger->hasInfoThatContains('Processing 1 workspace exports.'));

        // test loading with preserve = false to clean the workspace
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-input-mapping-test.test1',
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
            new ReaderOptions(true, false)
        );
        // the initially loaded tables should not be present in the workspace anymore
        try {
            $this->clientWrapper->getBasicClient()->createTableAsyncDirect(
                'out.c-input-mapping-test',
                ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test2', 'name' => 'test2']
            );
            self::fail('should throw 404 for workspace table not found');
        } catch (ClientException $exception) {
            self::assertStringContainsString('Table "test2" not found in schema', $exception->getMessage());
        }
        try {
            $this->clientWrapper->getBasicClient()->createTableAsyncDirect(
                'out.c-input-mapping-test',
                ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test3', 'name' => 'test3']
            );
            self::fail('should throw 404 for workspace table not found');
        } catch (ClientException $exception) {
            self::assertStringContainsString('Table "test3" not found in schema', $exception->getMessage());
        }
    }
}
