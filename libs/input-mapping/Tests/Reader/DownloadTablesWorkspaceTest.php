<?php

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Reader\NullWorkspaceProvider;
use Keboola\InputMapping\Reader\Options\InputTableOptionsList;
use Keboola\InputMapping\Reader\Reader;
use Keboola\InputMapping\Reader\State\InputTableStateList;
use Keboola\InputMapping\Reader\WorkspaceProviderInterface;
use Keboola\InputMapping\Tests\Reader\DownloadTablesTestAbstract;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Psr\Log\Test\TestLogger;

class DownloadTablesWorkspaceTest extends DownloadTablesTestAbstract
{
    private $workspaceId;

    public function setUp()
    {
        parent::setUp();
        try {
            $this->client->dropBucket('in.c-input-mapping-test', ['force' => true]);
        } catch (ClientException $e) {
            if ($e->getCode() != 404) {
                throw $e;
            }
        }
        try {
            $this->client->dropBucket('out.c-input-mapping-test', ['force' => true]);
        } catch (ClientException $e) {
            if ($e->getCode() != 404) {
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
        $this->client->createTableAsync('in.c-input-mapping-test', 'test', $csv);
        $this->client->createTableAsync('in.c-input-mapping-test', 'test2', $csv);
        if ($this->workspaceId) {
            $workspaces = new Workspaces($this->client);
            $workspaces->deleteWorkspace($this->workspaceId);
            $this->workspaceId = null;
        }
    }

    public function tearDown()
    {
        if ($this->workspaceId) {
            $workspaces = new Workspaces($this->client);
            $workspaces->deleteWorkspace($this->workspaceId);
            $this->workspaceId = null;
        }
        parent::tearDown();
    }

    private function getWorkspaceProvider()
    {
        $mock = self::getMockBuilder(NullWorkspaceProvider::class)
            ->setMethods(['getWorkspaceId'])
            ->getMock();
        $mock->method('getWorkspaceId')->willReturnCallback(
            function ($type) {
                if (!$this->workspaceId) {
                    $workspaces = new Workspaces($this->client);
                    $workspace = $workspaces->createWorkspace(['backend' => $type]);
                    var_export($workspace);
                    $this->workspaceId = $workspace['id'];
                }
                return $this->workspaceId;
            }
        );
        /** @var WorkspaceProviderInterface $mock */
        return $mock;
    }

    public function testReadTablesWorkspaceSnowflakeBackend()
    {
        $logger = new TestLogger();
        $reader = new Reader($this->client, $logger, $this->getWorkspaceProvider());
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-input-mapping-test.test',
                'destination' => 'test',
            ],
            [
                'source' => 'in.c-input-mapping-test.test2',
                'destination' => 'test2',
                'where_column' => 'Id',
                'where_values' => ['Id2', 'Id3'],
            ]
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download',
            'workspace-snowflake'
        );

        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test.manifest');
        self::assertEquals('in.c-input-mapping-test.test', $manifest['id']);
        /* we want to check that the table exists in the workspace, so we try to load it, which fails, because of
            the _timestamp columns, but that's okay. It means that the table is indeed in the workspace. */
        try {
            $this->client->createTableAsyncDirect(
                'out.c-input-mapping-test',
                ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test', 'name' => 'test']
            );
            self::fail('Must throw exception');
        } catch (ClientException $e) {
            self::assertContains('Invalid columns: _timestamp:', $e->getMessage());
        }

        // this is copy, so it doesn't contain the _timestamp column
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test2.manifest');
        self::assertEquals('in.c-input-mapping-test.test2', $manifest['id']);
        $this->client->createTableAsyncDirect(
            'out.c-input-mapping-test',
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test2', 'name' => 'test2']
        );

        self::assertTrue($logger->hasInfoThatContains('Table "in.c-input-mapping-test.test" will be cloned.'));
        self::assertTrue($logger->hasInfoThatContains('Table "in.c-input-mapping-test.test2" will be copied.'));
        self::assertTrue($logger->hasInfoThatContains('Processing 2 workspace table exports.'));
    }

    public function testReadTablesWorkspaceRedshiftBackend()
    {
        $logger = new TestLogger();
        $reader = new Reader($this->client, $logger, $this->getWorkspaceProvider());
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-input-mapping-test.test',
                'destination' => 'test',
                'changed_since' => 'adaptive',
            ],
            [
                'source' => 'in.c-input-mapping-test.test2',
                'destination' => 'test2',
            ]
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download',
            'workspace-redshift'
        );

        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test.manifest');
        /* because of https://keboola.atlassian.net/browse/KBC-228 we have to create redshift bucket to
            unload data from redshift workspace */
        $this->client->dropBucket('out.c-input-mapping-test');
        $this->client->createBucket('input-mapping-test', Client::STAGE_OUT, 'Docker Testsuite', 'redshift');

        self::assertEquals('in.c-input-mapping-test.test', $manifest['id']);
        // test that the table exists in the workspace
        $tableId = $this->client->createTableAsyncDirect(
            'out.c-input-mapping-test',
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test', 'name' => 'test']
        );
        self::assertEquals('out.c-input-mapping-test.test', $tableId);

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test2.manifest');
        self::assertEquals('in.c-input-mapping-test.test2', $manifest['id']);
        $tableId = $this->client->createTableAsyncDirect(
            'out.c-input-mapping-test',
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test2', 'name' => 'test2']
        );
        self::assertEquals('out.c-input-mapping-test.test2', $tableId);

        self::assertTrue($logger->hasInfoThatContains('Table "in.c-input-mapping-test.test" will be copied.'));
        self::assertTrue($logger->hasInfoThatContains('Table "in.c-input-mapping-test.test2" will be copied.'));
        self::assertTrue($logger->hasInfoThatContains('Processing 2 workspace table exports.'));
    }
}
