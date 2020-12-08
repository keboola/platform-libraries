<?php

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Exception;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\NullLogger;

class AbsWriterWorkspaceTest extends BaseWriterWorkspaceTest
{
    use InitSynapseStorageClientTrait;
    
    /** @var array */
    protected $workspace;

    public function setUp()
    {
        if (!$this->checkSynapseTests()) {
            self::markTestSkipped('Synapse tests disabled.');
        }
        parent::setUp();
        $this->clearBuckets([
            'in.c-output-mapping-test',
            'out.c-output-mapping-test',
        ]);
    }

    protected function initClient()
    {
        $this->clientWrapper = $this->getSynapseClientWrapper();
    }

    public function testAbsTableOutputMapping()
    {
        $root = $this->tmp->getTmpFolder();
        $this->prepareWorkspaceWithTables('abs');

        $configs = [
            [
                'source' => 'table1a',
                'destination' => 'out.c-output-mapping-test.table1a',
                'incremental' => true,
                'columns' => ['Id'],
            ],
            [
                'source' => 'table2a',
                'destination' => 'out.c-output-mapping-test.table2a',
            ],
        ];
        file_put_contents(
            $root . '/table1a.manifest',
            json_encode(
                ['columns' => ['Id', 'Name']]
            )
        );
        file_put_contents(
            $root . '/table2a.manifest',
            json_encode(
                ['columns' => ['Id2', 'Name2']]
            )
        );
        $writer = new TableWriter($this->clientWrapper, new NullLogger(), $this->getWorkspaceProvider());

        $tableQueue = $writer->uploadTables(
            $root,
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'workspace-abs'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(2, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables('out.c-output-mapping-test');
        $this->assertCount(2, $tables);
        $tableIds = [$tables[0]['id'], $tables[1]['id']];
        sort($tableIds);
        $this->assertEquals(['out.c-output-mapping-test.table1a', 'out.c-output-mapping-test.table2a'], $tableIds);
        $this->assertCount(2, $jobIds);
        $this->assertNotEmpty($jobIds[0]);
        $this->assertNotEmpty($jobIds[1]);
        $job = $this->clientWrapper->getBasicClient()->getJob($jobIds[0]);
        $this->assertEquals('out.c-output-mapping-test.table1a', $job['tableId']);
        $this->assertEquals(true, $job['operationParams']['params']['incremental']);
        $this->assertEquals(['Id'], $job['operationParams']['params']['columns']);
        $data = $this->clientWrapper->getBasicClient()->getTableDataPreview('out.c-output-mapping-test.table1a');
        $job = $this->clientWrapper->getBasicClient()->getJob($jobIds[1]);
        $this->assertEquals('out.c-output-mapping-test.table2a', $job['tableId']);
        $this->assertEquals(false, $job['operationParams']['params']['incremental']);
        $this->assertEquals(['Id2', 'Name2'], $job['operationParams']['params']['columns']);

        $rows = explode("\n", trim($data));
        sort($rows);
        // convert to lowercase because of https://keboola.atlassian.net/browse/KBC-864
        $rows = array_map(
            'strtolower',
            $rows
        );
        // 1a has only the id column
        $this->assertEquals(['"id"', '"test"'], $rows);
    }

    protected function getWorkspaceProvider($workspaceData = [])
    {
        $mock = self::getMockBuilder(NullWorkspaceProvider::class)
            ->setMethods(['getCredentials', 'getWorkspaceId'])
            ->getMock();
        $mock->method('getCredentials')->willReturnCallback(
            function ($type) use ($workspaceData) {
                if (!$this->workspace) {
                    $workspaces = new Workspaces($this->clientWrapper->getBasicClient());
                    $workspace = $workspaces->createWorkspace(['backend' => $type]);
                    $this->workspace = $workspaceData ? $workspaceData : $workspace;
                }
                return $this->workspace['connection'];
            }
        );
        $mock->method('getWorkspaceId')->willReturnCallback(
            function ($type) use ($workspaceData) {
                if (!$this->workspace) {
                    $workspaces = new Workspaces($this->clientWrapper->getBasicClient());
                    $workspace = $workspaces->createWorkspace(['backend' => $type]);
                    $this->workspace = $workspaceData ? $workspaceData : $workspace;
                }
                return $this->workspace['id'];
            }
        );
        /** @var WorkspaceProviderInterface $mock */
        return $mock;
    }
    
    public function testWriteBasicFiles()
    {
        $workspaceProvider = $this->getWorkspaceProvider();
        $workspaceProvider->getCredentials('abs'); //initializes $this->workspace
        $blobClient = BlobRestProxy::createBlobService($this->workspace['connection']['connectionString']);
        $blobClient->createBlockBlob($this->workspace['connection']['container'], 'upload/file1', 'test');
        $blobClient->createBlockBlob($this->workspace['connection']['container'], 'upload/file2', 'test');
        $blobClient->createBlockBlob(
            $this->workspace['connection']['container'],
            'upload/file2.manifest',
            '{"tags": ["output-mapping-test", "xxx"],"is_public": false}'
        );
        $blobClient->createBlockBlob(
            $this->workspace['connection']['container'],
            'upload/file3',
            'test'
        );
        $blobClient->createBlockBlob(
            $this->workspace['connection']['container'],
            'upload/file3.manifest',
            '{"tags": ["output-mapping-test"],"is_permanent": true}'
        );
        $configs = [
            [
                'source' => 'file1',
                'tags' => ['output-mapping-test']
            ],
            [
                'source' => 'file2',
                'tags' => ['output-mapping-test', 'another-tag'],
                'is_permanent' => true
            ]
        ];

        $writer = new FileWriter($this->clientWrapper, new NullLogger(), $workspaceProvider);

        $writer->uploadFiles('/upload', ['mapping' => $configs], Reader::STAGING_ABS_WORKSPACE);
        sleep(1);

        $options = new ListFilesOptions();
        $options->setTags(['output-mapping-test']);
        $files = $this->clientWrapper->getBasicClient()->listFiles($options);
        $this->assertCount(3, $files);

        $file1 = $file2 = $file3 = null;
        foreach ($files as $file) {
            if ($file['name'] == 'file1') {
                $file1 = $file;
            }
            if ($file['name'] == 'file2') {
                $file2 = $file;
            }
            if ($file['name'] == 'file3') {
                $file3 = $file;
            }
        }

        $this->assertNotNull($file1);
        $this->assertNotNull($file2);
        $this->assertNotNull($file3);
        $this->assertEquals(4, $file1['sizeBytes']);
        $this->assertEquals(['output-mapping-test'], $file1['tags']);
        $this->assertEquals(['output-mapping-test', 'another-tag'], $file2['tags']);
        $this->assertEquals(['output-mapping-test'], $file3['tags']);
        $this->assertNotNull($file1['maxAgeDays']);
        $this->assertNull($file2['maxAgeDays']);
        $this->assertNull($file3['maxAgeDays']);
    }
}
