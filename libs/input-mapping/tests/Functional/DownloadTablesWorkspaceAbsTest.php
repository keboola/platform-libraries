<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\FileStorage\Abs\ClientFactory;
use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\InputMapping\Tests\AbstractTestCase;
use Keboola\InputMapping\Tests\Needs\NeedsStorageBackend;
use Keboola\InputMapping\Tests\Needs\NeedsTestTables;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;
use PHPUnit\Util\Test;

#[NeedsStorageBackend('synapse')]
class DownloadTablesWorkspaceAbsTest extends AbstractTestCase
{
    private bool $runSynapseTests;

    public function setUp(): void
    {
        $this->runSynapseTests = (bool) getenv('RUN_SYNAPSE_TESTS');
        if (!$this->runSynapseTests) {
            return;
        }
        parent::setUp();
    }

    protected function initClient(?string $branchId = null): ClientWrapper
    {
        $clientOptions = (new ClientOptions())
            ->setUrl((string) getenv('SYNAPSE_STORAGE_API_URL'))
            ->setToken((string) getenv('SYNAPSE_STORAGE_API_TOKEN'))
            ->setBranchId($branchId)
            ->setBackoffMaxTries(1)
            ->setJobPollRetryDelay(function () {
                return 1;
            })
            ->setUserAgent(implode('::', Test::describe($this)))
        ;

        $clientWrapper = new ClientWrapper($clientOptions);
        $tokenInfo = $clientWrapper->getBranchClient()->verifyToken();
        print(sprintf(
            'Authorized as "%s (%s)" to project "%s (%s)" at "%s" stack.',
            $tokenInfo['description'],
            $tokenInfo['id'],
            $tokenInfo['owner']['name'],
            $tokenInfo['owner']['id'],
            $clientWrapper->getBranchClient()->getApiUrl(),
        ));
        return $clientWrapper;
    }

    protected function assertBlobs(string $basePath): void
    {
        $blobListOptions = new ListBlobsOptions();
        $blobListOptions->setPrefix($basePath);

        $blobClient = ClientFactory::createClientFromConnectionString($this->workspaceCredentials['connectionString']);
        $blobList = $blobClient->listBlobs($this->workspaceCredentials['container'], $blobListOptions);
        self::assertGreaterThan(0, count($blobList->getBlobs()));
    }

    #[NeedsTestTables(3)]
    public function testTablesAbsWorkspace(): void
    {
        if (!$this->runSynapseTests) {
            self::markTestSkipped('Synapse tests disabled');
        }
        $reader = new Reader(
            $this->getWorkspaceStagingFactory(
                $this->initClient(),
                'json',
                $this->testLogger,
                [AbstractStrategyFactory::WORKSPACE_ABS, 'abs'],
            ),
        );
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
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::WORKSPACE_ABS,
            new ReaderOptions(true),
        );

        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test1.manifest');
        self::assertEquals('in.c-testTablesAbsWorkspaceTest.test1', $manifest['id']);

        $this->assertBlobs('download/test1');

        // make sure the blob exists
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test2.manifest');
        self::assertEquals('in.c-testTablesAbsWorkspaceTest.test2', $manifest['id']);

        $this->assertBlobs('download/test2');

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test3.manifest');
        self::assertEquals('in.c-testTablesAbsWorkspaceTest.test3', $manifest['id']);

        $this->assertBlobs('download/test3');

        self::assertTrue($this->testHandler->hasInfoThatContains('Using "workspace-abs" table input staging.'));
        self::assertTrue(
            $this->testHandler->hasInfoThatContains('Table "in.c-testTablesAbsWorkspaceTest.test1" will be copied.'),
        );
        self::assertTrue(
            $this->testHandler->hasInfoThatContains('Table "in.c-testTablesAbsWorkspaceTest.test2" will be copied.'),
        );
        self::assertTrue(
            $this->testHandler->hasInfoThatContains('Table "in.c-testTablesAbsWorkspaceTest.test3" will be copied.'),
        );
        self::assertTrue($this->testHandler->hasInfoThatContains('Copying 3 tables to workspace.'));
        self::assertTrue($this->testHandler->hasInfoThatContains('Processing workspace export.'));
    }

    #[NeedsTestTables]
    public function testTablesAbsWorkspaceSlash(): void
    {
        if (!$this->runSynapseTests) {
            self::markTestSkipped('Synapse tests disabled');
        }
        $reader = new Reader($this->getWorkspaceStagingFactory(
            $this->initClient(),
            'json',
            $this->testLogger,
            [AbstractStrategyFactory::WORKSPACE_ABS, 'abs'],
        ));
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test1',
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download/test/',
            AbstractStrategyFactory::WORKSPACE_ABS,
            new ReaderOptions(true),
        );

        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test/test1.manifest');
        self::assertEquals('in.c-testTablesAbsWorkspaceSlashTest.test1', $manifest['id']);

        $this->assertBlobs('download/test/test1');
        self::assertTrue($this->testHandler->hasInfoThatContains('Using "workspace-abs" table input staging.'));
        self::assertTrue($this->testHandler->hasInfoThatContains(
            'Table "in.c-testTablesAbsWorkspaceSlashTest.test1" will be copied.',
        ));
    }

    #[NeedsTestTables]
    public function testUseViewFails(): void
    {
        if (time() > 1) {
            $this->markTestSkipped('TODO fix test https://keboola.atlassian.net/browse/PST-961');
        }

        $reader = new Reader($this->getWorkspaceStagingFactory(
            $this->initClient(),
            'json',
            $this->testLogger,
            [AbstractStrategyFactory::WORKSPACE_ABS, 'abs'],
        ));
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test1',
                'use_view' => true,
            ],
        ]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'View load for table "download/test1" using backend "abs" can\'t be used, only Synapse is supported.',
        );

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::WORKSPACE_ABS,
            new ReaderOptions(true),
        );
    }
}
