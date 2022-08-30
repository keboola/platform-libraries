<?php

namespace Keboola\OutputMapping\Tests\Writer\Workspace;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\InputMapping\Staging\Scope;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Tests\Writer\BaseWriterTest;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Temp\Temp;
use Psr\Log\NullLogger;

abstract class BaseWriterWorkspaceTest extends BaseWriterTest
{
    /** @var string */
    protected $workspaceId;

    /** @var array */
    protected $workspaceCredentials;

    /** @var array */
    protected $workspace;

    public function tearDown()
    {
        if ($this->workspaceId) {
            $workspaces = new Workspaces($this->clientWrapper->getBasicClient());
            try {
                $workspaces->deleteWorkspace($this->workspaceId, ['async' => 1]);
            } catch (ClientException $e) {
                if ($e->getCode() !== 404) {
                    throw $e;
                }
            }
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
                    $this->workspace = $workspace;
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
                return $this->tmp->getTmpFolder();
            }
        );
        /** @var ProviderInterface $mockLocal */
        /** @var ProviderInterface $mockWorkspace */
        $stagingFactory->addProvider(
            $mockLocal,
            [
                $backend[0] => new Scope([Scope::TABLE_METADATA]),
            ]
        );
        $stagingFactory->addProvider(
            $mockWorkspace,
            [
                $backend[0] => new Scope([Scope::TABLE_DATA])
            ]
        );
        return $stagingFactory;
    }

    protected function prepareWorkspaceWithTables($type, $bucketName, $tablePrefix = '')
    {
        $temp = new Temp();
        $temp->initRunFolder();
        $root = $temp->getTmpFolder();
        $backendType = $type;
        $bucketId = 'in.c-' . $bucketName;
        // abs is a workspace type, but not a backendType
        if ($type === 'abs') {
            $backendType = 'synapse';
        }
        $this->clientWrapper->getBasicClient()->createBucket($bucketName, 'in', '', $backendType);
        // Create tables
        $csv1a = new CsvFile($root . DIRECTORY_SEPARATOR . 'table1a.csv');
        $csv1a->writeRow(['Id', 'Name']);
        $csv1a->writeRow(['test', 'test']);
        $csv1a->writeRow(['aabb', 'ccdd']);
        $this->clientWrapper->getBasicClient()->createTable($bucketId, 'table1a', $csv1a);
        $csv2a = new CsvFile($root . DIRECTORY_SEPARATOR . 'table2a.csv');
        $csv2a->writeRow(['Id2', 'Name2']);
        $csv2a->writeRow(['test2', 'test2']);
        $csv2a->writeRow(['aabb2', 'ccdd2']);
        $this->clientWrapper->getBasicClient()->createTable($bucketId, 'table2a', $csv2a);

        $workspaces = new Workspaces($this->clientWrapper->getBasicClient());
        $workspaces->loadWorkspaceData(
            $this->workspaceId,
            [
                'input' => [
                    [
                        'source' => $bucketId . '.table1a',
                        'destination' => $tablePrefix . 'table1a',
                    ],
                    [
                        'source' => $bucketId . '.table2a',
                        'destination' => $tablePrefix . 'table2a',
                    ],
                ],
            ]
        );
    }
}
