<?php

namespace Keboola\InputMapping\Tests\Reader;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Reader\Options\InputTableOptionsList;
use Keboola\InputMapping\Reader\Reader;
use Keboola\InputMapping\Reader\State\InputTableStateList;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Exception;
use Psr\Log\Test\TestLogger;

class DownloadTablesWorkspaceSynapseTest extends DownloadTablesWorkspaceTestAbstract
{
    private $runSynapseTests;

    public function setUp()
    {
        //Skip the DownloadTablesWorkspaceTestAbstract::setup because we want to use a different client
        DownloadTablesTestAbstract::setUp();
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
        $token = (string) getenv('SYNAPSE_STORAGE_API_TOKEN');
        $url = (string) getenv('SYNAPSE_STORAGE_API_URL');
        $this->client = new Client(["token" => $token, "url" => $url]);
        try {
            $this->client->dropBucket('in.c-input-mapping-test', ['force' => true]);
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }
        try {
            $this->client->dropBucket('out.c-input-mapping-test', ['force' => true]);
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }
        $this->client->createBucket('input-mapping-test', Client::STAGE_IN, 'Docker Testsuite');
        $this->client->createBucket('input-mapping-test', Client::STAGE_OUT, 'Docker Testsuite');

        // Create table
        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload.csv');
        $csv->writeRow(['Id', 'Name', 'foo', 'bar']);
        $csv->writeRow(['id1', 'name1', 'foo1', 'bar1']);
        $csv->writeRow(['id2', 'name2', 'foo2', 'bar2']);
        $csv->writeRow(['id3', 'name3', 'foo3', 'bar3']);
        $this->client->createTableAsync('in.c-input-mapping-test', 'test1', $csv);
        $this->client->createTableAsync('in.c-input-mapping-test', 'test2', $csv);
        $this->client->createTableAsync('in.c-input-mapping-test', 'test3', $csv);
    }

    public function testTablesSynapseBackend()
    {
        if (!$this->runSynapseTests) {
            $this->markTestSkipped('Synapse tests disabled');
        }
        $logger = new TestLogger();
        $reader = new Reader($this->client, $logger, $this->getWorkspaceProvider());
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
        $this->client->dropBucket('out.c-input-mapping-test');
        $this->client->createBucket(
            'input-mapping-test',
            Client::STAGE_OUT,
            'Docker Testsuite',
            'synapse'
        );

        self::assertEquals('in.c-input-mapping-test.test1', $manifest['id']);
        // test that the table exists in the workspace
        $tableId = $this->client->createTableAsyncDirect(
            'out.c-input-mapping-test',
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test1', 'name' => 'test1']
        );
        self::assertEquals('out.c-input-mapping-test.test1', $tableId);
        $table = $this->client->getTable($tableId);
        self::assertEquals(['Id'], $table['columns']);

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test2.manifest');
        self::assertEquals('in.c-input-mapping-test.test2', $manifest['id']);
        $tableId = $this->client->createTableAsyncDirect(
            'out.c-input-mapping-test',
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test2', 'name' => 'test2']
        );
        self::assertEquals('out.c-input-mapping-test.test2', $tableId);

        self::assertTrue($logger->hasInfoThatContains('Table "in.c-input-mapping-test.test1" will be copied.'));
        self::assertTrue($logger->hasInfoThatContains('Table "in.c-input-mapping-test.test2" will be copied.'));
        self::assertTrue($logger->hasInfoThatContains('Processing 1 workspace exports.'));
    }
}
