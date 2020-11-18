<?php

namespace Keboola\InputMapping\Tests\Reader;

use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Reader\Options\InputTableOptionsList;
use Keboola\InputMapping\Reader\Reader;
use Keboola\InputMapping\Reader\State\InputTableStateList;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Exception;
use Psr\Log\Test\TestLogger;

class DownloadTablesWorkspaceAbsTest extends DownloadTablesWorkspaceTestAbstract
{
    private $runSynapseTests;

    public function setUp()
    {
        $this->runSynapseTests = getenv('RUN_SYNAPSE_TESTS');
        if (!$this->runSynapseTests) {
            return;
        }
        if (getenv('SYNAPSE_STORAGE_API_TOKEN') === false) {
            throw new Exception('SYNAPSE_STORAGE_API_TOKEN must be set for synapse tests');
        }
        if (getenv('SYNAPSE_STORAGE_API_URL') === false) {
            throw new Exception('SYNAPSE_STORAGE_API_URL must be set for synapse tests');
        }
        parent::setUp();
    }

    protected function initClient()
    {
        $token = (string) getenv('SYNAPSE_STORAGE_API_TOKEN');
        $url = (string) getenv('SYNAPSE_STORAGE_API_URL');
        $this->clientWrapper = new ClientWrapper(
            new Client(["token" => $token, "url" => $url]),
            null,
            null
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

    public function testTablesAbsWorkspace()
    {
        if (!$this->runSynapseTests) {
            self::markTestSkipped('Synapse tests disabled');
        }
        $logger = new TestLogger();
        $reader = new Reader($this->client, $logger, $this->getWorkspaceProvider());
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

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download',
            'workspace-abs'
        );

        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test1.manifest');
        self::assertEquals('in.c-input-mapping-test.test1', $manifest['id']);

        $tableId = $this->client->createTableAsyncDirect(
            'out.c-input-mapping-test',
            [
                'dataWorkspaceId' => $this->workspaceId,
                'dataTableName' => 'test1',
                'name' => 'test1',
                'columns' => ['Id', 'Name', 'foo', 'bar']
            ]
        );
        self::assertNotEmpty($tableId);

        // this is copy, so it doesn't contain the _timestamp column
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test2.manifest');
        self::assertEquals('in.c-input-mapping-test.test2', $manifest['id']);
        $tableId = $this->client->createTableAsyncDirect(
            'out.c-input-mapping-test',
            [
                'dataWorkspaceId' => $this->workspaceId,
                'dataTableName' => 'test2',
                'name' => 'test2',
                'columns' => ['Id'],
            ]
        );
        self::assertNotEmpty($tableId);

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test3.manifest');
        self::assertEquals('in.c-input-mapping-test.test3', $manifest['id']);

        $tableId = $this->client->createTableAsyncDirect(
            'out.c-input-mapping-test',
            [
                'dataWorkspaceId' => $this->workspaceId,
                'dataTableName' => 'test3',
                'name' => 'test3',
                'columns' => ['Id', 'Name', 'foo', 'bar'],
            ]
        );
        self::assertNotEmpty($tableId);
        self::assertTrue($logger->hasInfoThatContains('Table "in.c-input-mapping-test.test1" will be copied.'));
        self::assertTrue($logger->hasInfoThatContains('Table "in.c-input-mapping-test.test2" will be copied.'));
        self::assertTrue($logger->hasInfoThatContains('Table "in.c-input-mapping-test.test3" will be copied.'));
        self::assertTrue($logger->hasInfoThatContains('Copying 3 tables to abs workspace.'));
        self::assertTrue($logger->hasInfoThatContains('Processing workspace export.'));
    }
}
