<?php

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\FileStorage\Abs\ClientFactory;
use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Exception;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;
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

    protected function assertBlobs($basePath)
    {
        $blobListOptions = new ListBlobsOptions();
        $blobListOptions->setPrefix($basePath);

        $blobClient = ClientFactory::createClientFromConnectionString($this->workspaceCredentials['connectionString']);
        $blobList = $blobClient->listBlobs($this->workspaceCredentials['container'], $blobListOptions);
        self::assertGreaterThan(0, count($blobList->getBlobs()));
    }

    public function testTablesAbsWorkspace()
    {
        if (!$this->runSynapseTests) {
            self::markTestSkipped('Synapse tests disabled');
        }
        $logger = new TestLogger();
        $reader = new Reader($this->getStagingFactory(null, 'json', $logger, [StrategyFactory::WORKSPACE_ABS, 'abs']));
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
            'download',
            StrategyFactory::WORKSPACE_ABS,
            new ReaderOptions(true)
        );

        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test1.manifest');
        self::assertEquals('in.c-input-mapping-test.test1', $manifest['id']);

        $this->assertBlobs('download/test1');

        // make sure the blob exists
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test2.manifest');
        self::assertEquals('in.c-input-mapping-test.test2', $manifest['id']);

        $this->assertBlobs('download/test2');

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test3.manifest');
        self::assertEquals('in.c-input-mapping-test.test3', $manifest['id']);

        $this->assertBlobs('download/test3');

        self::assertTrue($logger->hasInfoThatContains('Using "workspace-abs" table input staging.'));
        self::assertTrue($logger->hasInfoThatContains('Table "in.c-input-mapping-test.test1" will be copied.'));
        self::assertTrue($logger->hasInfoThatContains('Table "in.c-input-mapping-test.test2" will be copied.'));
        self::assertTrue($logger->hasInfoThatContains('Table "in.c-input-mapping-test.test3" will be copied.'));
        self::assertTrue($logger->hasInfoThatContains('Copying 3 tables to workspace.'));
        self::assertTrue($logger->hasInfoThatContains('Processing workspace export.'));
    }

    public function testTablesAbsWorkspaceSlash()
    {
        if (!$this->runSynapseTests) {
            self::markTestSkipped('Synapse tests disabled');
        }
        $logger = new TestLogger();
        $reader = new Reader($this->getStagingFactory(null, 'json', $logger, [StrategyFactory::WORKSPACE_ABS, 'abs']));
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-input-mapping-test.test1',
                'destination' => 'test1',
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download/test/',
            StrategyFactory::WORKSPACE_ABS,
            new ReaderOptions(true)
        );

        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test/test1.manifest');
        self::assertEquals('in.c-input-mapping-test.test1', $manifest['id']);

        $this->assertBlobs('download/test/test1');
        self::assertTrue($logger->hasInfoThatContains('Using "workspace-abs" table input staging.'));
        self::assertTrue($logger->hasInfoThatContains('Table "in.c-input-mapping-test.test1" will be copied.'));
    }

    public function testUseViewFails()
    {
        $logger = new TestLogger();
        $reader = new Reader($this->getStagingFactory(null, 'json', $logger, [StrategyFactory::WORKSPACE_ABS, 'abs']));
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-input-mapping-test.test1',
                'destination' => 'test1',
                'use_view' => true,
            ]
        ]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('View load for table "download/test1" using backend "abs" can\'t be used, only Synapse is supported.');

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            StrategyFactory::WORKSPACE_ABS,
            new ReaderOptions(true)
        );
    }
}
