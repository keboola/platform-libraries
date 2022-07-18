<?php

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\InputMapping\Staging\Operation;
use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Psr\Log\NullLogger;

class DownloadTablesWorkspaceTestAbstract extends DownloadTablesTestAbstract
{
    protected $workspaceId;

    protected $workspaceCredentials;

    public function setUp()
    {
        parent::setUp();
        try {
            $this->clientWrapper->getBasicClient()->dropBucket('in.c-input-mapping-test', ['force' => true]);
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }
        try {
            $this->clientWrapper->getBasicClient()->dropBucket('out.c-input-mapping-test', ['force' => true]);
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }
        $this->clientWrapper->getBasicClient()->createBucket(
            'input-mapping-test',
            Client::STAGE_IN,
            'Docker Testsuite'
        );
        $this->clientWrapper->getBasicClient()->createBucket(
            'input-mapping-test',
            Client::STAGE_OUT,
            'Docker Testsuite'
        );

        // Create table
        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload.csv');
        $csv->writeRow(['Id', 'Name', 'foo', 'bar']);
        $csv->writeRow(['id1', 'name1', 'foo1', 'bar1']);
        $csv->writeRow(['id2', 'name2', 'foo2', 'bar2']);
        $csv->writeRow(['id3', 'name3', 'foo3', 'bar3']);
        $this->clientWrapper->getBasicClient()->createTableAsync('in.c-input-mapping-test', 'test1', $csv);
        $this->clientWrapper->getBasicClient()->createTableAsync('in.c-input-mapping-test', 'test2', $csv);
        $this->clientWrapper->getBasicClient()->createTableAsync('in.c-input-mapping-test', 'test3', $csv);
    }

    public function tearDown()
    {
        if ($this->workspaceId) {
            $workspaces = new Workspaces($this->clientWrapper->getBasicClient());
            $workspaces->deleteWorkspace($this->workspaceId);
            $this->workspaceId = null;
        }
        parent::tearDown();
    }

    protected function getStagingFactory($clientWrapper = null, $format = 'json', $logger = null, $backend = [StrategyFactory::WORKSPACE_SNOWFLAKE, 'snowflake'])
    {
        $stagingFactory = new StrategyFactory(
            $clientWrapper ? $clientWrapper : $this->clientWrapper,
            $logger ? $logger : new NullLogger(),
            $format
        );
        $mockWorkspace = self::getMockBuilder(NullProvider::class)
            ->setMethods(['getWorkspaceId'])
            ->getMock();
        $mockWorkspace->method('getWorkspaceId')->willReturnCallback(
            function () use ($backend) {
                if (!$this->workspaceId) {
                    $workspaces = new Workspaces($this->clientWrapper->getBasicClient());
                    $workspace = $workspaces->createWorkspace(['backend' => $backend[1]]);
                    $this->workspaceId = $workspace['id'];
                    $this->workspaceCredentials = $workspace['connection'];
                }
                return $this->workspaceId;
            }
        );
        $mockLocal = self::getMockBuilder(NullProvider::class)
            ->setMethods(['getPath'])
            ->getMock();
        $mockLocal->method('getPath')->willReturnCallback(
            function () {
                return $this->temp->getTmpFolder();
            }
        );
        /** @var ProviderInterface $mockLocal */
        /** @var ProviderInterface $mockWorkspace */
        $stagingFactory->addProvider(
            $mockLocal,
            [
                $backend[0] => new Operation([Operation::TABLE_METADATA]),
            ]
        );
        $stagingFactory->addProvider(
            $mockWorkspace,
            [
                $backend[0] => new Operation([Operation::TABLE_DATA])
            ]
        );
        return $stagingFactory;
    }
}
