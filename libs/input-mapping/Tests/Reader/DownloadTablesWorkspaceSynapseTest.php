<?php

namespace Keboola\InputMapping\Tests\Reader;

use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Reader\Options\InputTableOptionsList;
use Keboola\InputMapping\Reader\Reader;
use Keboola\InputMapping\Reader\State\InputTableStateList;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Exception;
use Psr\Log\Test\TestLogger;

class DownloadTablesWorkspaceSynapseTest extends DownloadTablesWorkspaceTestAbstract
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

    public function testTablesSynapseBackend()
    {
        if (!$this->runSynapseTests) {
            self::markTestSkipped('Synapse tests disabled');
        }
        $logger = new TestLogger();
        $reader = new Reader($this->clientWrapper, $logger, $this->getWorkspaceProvider());
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
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download',
            'workspace-synapse'
        );

        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test1.manifest');
        $this->clientWrapper->dropBucket('out.c-input-mapping-test');
        $this->clientWrapper->createBucket(
            'input-mapping-test',
            Client::STAGE_OUT,
            'Docker Testsuite',
            'synapse'
        );

        self::assertEquals('in.c-input-mapping-test.test1', $manifest['id']);
        // test that the table exists in the workspace
        $tableId = $this->clientWrapper->createTableAsyncDirect(
            'out.c-input-mapping-test',
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test1', 'name' => 'test1']
        );
        self::assertEquals('out.c-input-mapping-test.test1', $tableId);
        $table = $this->clientWrapper->getTable($tableId);
        self::assertEquals(['Id'], $table['columns']);

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test2.manifest');
        self::assertEquals('in.c-input-mapping-test.test2', $manifest['id']);
        $tableId = $this->clientWrapper->createTableAsyncDirect(
            'out.c-input-mapping-test',
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test2', 'name' => 'test2']
        );
        self::assertEquals('out.c-input-mapping-test.test2', $tableId);

        self::assertTrue($logger->hasInfoThatContains('Table "in.c-input-mapping-test.test1" will be copied.'));
        self::assertTrue($logger->hasInfoThatContains('Table "in.c-input-mapping-test.test2" will be copied.'));
        self::assertTrue($logger->hasInfoThatContains('Processing 1 workspace exports.'));
    }
}
