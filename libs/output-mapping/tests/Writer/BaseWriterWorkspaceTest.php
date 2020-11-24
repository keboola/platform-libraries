<?php

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Reader\NullWorkspaceProvider;
use Keboola\InputMapping\Reader\WorkspaceProviderInterface;
use Keboola\StorageApi\Workspaces;
use Keboola\Temp\Temp;

abstract class BaseWriterWorkspaceTest extends BaseWriterTest
{
    /**
     * @var string
     */
    protected $workspaceId;

    public function setUp()
    {
        parent::setUp();
        $this->clearBuckets([
            'out.c-output-mapping-test',
            'in.c-output-mapping-test',
            'out.c-dev-123-output-mapping-test',
        ]);
        $this->clearFileUploads(['output-mapping-test']);
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

    protected function getWorkspaceProvider()
    {
        $mock = self::getMockBuilder(NullWorkspaceProvider::class)
            ->setMethods(['getWorkspaceId'])
            ->getMock();
        $mock->method('getWorkspaceId')->willReturnCallback(
            function ($type) {
                if (!$this->workspaceId) {
                    $workspaces = new Workspaces($this->clientWrapper->getBasicClient());
                    $workspace = $workspaces->createWorkspace(['backend' => $type]);
                    $this->workspaceId = $workspace['id'];
                }
                return $this->workspaceId;
            }
        );
        /** @var WorkspaceProviderInterface $mock */
        return $mock;
    }

    protected function prepareWorkspaceWithTables($type)
    {
        $temp = new Temp();
        $temp->initRunFolder();
        $root = $temp->getTmpFolder();
        $backendType = $type;
        // abs is a workspace type, but not a backendType
        if ($type === 'abs') {
            $backendType = 'synapse';
        }
        $this->clientWrapper->getBasicClient()->createBucket('output-mapping-test', 'in', '', $backendType);
        // Create tables
        $csv1a = new CsvFile($root . DIRECTORY_SEPARATOR . 'table1a.csv');
        $csv1a->writeRow(['Id', 'Name']);
        $csv1a->writeRow(['test', 'test']);
        $csv1a->writeRow(['aabb', 'ccdd']);
        $this->clientWrapper->getBasicClient()->createTable('in.c-output-mapping-test', 'table1a', $csv1a);
        $csv2a = new CsvFile($root . DIRECTORY_SEPARATOR . 'table2a.csv');
        $csv2a->writeRow(['Id2', 'Name2']);
        $csv2a->writeRow(['test2', 'test2']);
        $csv2a->writeRow(['aabb2', 'ccdd2']);
        $this->clientWrapper->getBasicClient()->createTable('in.c-output-mapping-test', 'table2a', $csv2a);

        $workspaces = new Workspaces($this->clientWrapper->getBasicClient());
        $workspaceProvider = $this->getWorkspaceProvider();
        $workspaces->loadWorkspaceData(
            $workspaceProvider->getWorkspaceId($type),
            [
                'input' => [
                    [
                        'source' => 'in.c-output-mapping-test.table1a',
                        'destination' => 'table1a',
                    ],
                    [
                        'source' => 'in.c-output-mapping-test.table2a',
                        'destination' => 'table2a',
                    ],
                ],
            ]
        );
    }
}
