<?php

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\InputMapping\Staging\Scope;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Tests\InitSynapseStorageClientTrait;
use Keboola\OutputMapping\Writer\FileWriter;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApi\Workspaces;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use Psr\Log\NullLogger;
use Symfony\Component\Filesystem\Filesystem;

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

    protected function getStagingFactory($clientWrapper = null, $format = 'json', $logger = null, $backend = [StrategyFactory::WORKSPACE_SNOWFLAKE, 'snowflake'])
    {
        $stagingFactory = new StrategyFactory(
            $clientWrapper ? $clientWrapper : $this->clientWrapper,
            $logger ? $logger : new NullLogger(),
            $format
        );
        $mockWorkspace = self::getMockBuilder(NullProvider::class)
            ->setMethods(['getWorkspaceId', 'getCredentials'])
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
        $mockWorkspace->method('getCredentials')->willReturnCallback(
            function () use ($backend) {
                if (!$this->workspaceId) {
                    $workspaces = new Workspaces($this->clientWrapper->getBasicClient());
                    $workspace = $workspaces->createWorkspace(['backend' => $backend[1]]);
                    $this->workspaceId = $workspace['id'];
                    $this->workspace = $workspace;
                    $this->workspaceCredentials = $workspace['connection'];
                }
                return $this->workspaceCredentials;
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
                $backend[0] => new Scope([Scope::FILE_METADATA, Scope::TABLE_METADATA]),
            ]
        );
        $stagingFactory->addProvider(
            $mockWorkspace,
            [
                $backend[0] => new Scope([Scope::FILE_METADATA, Scope::FILE_DATA, Scope::TABLE_DATA])
            ]
        );
        return $stagingFactory;
    }

    public function testAbsTableSlicedManifestOutputMapping()
    {
        $factory = $this->getStagingFactory(null, 'json', null, [StrategyFactory::WORKSPACE_ABS, 'abs']);
        // initialize the workspace mock
        $factory->getTableOutputStrategy(StrategyFactory::WORKSPACE_ABS)->getDataStorage()->getWorkspaceId();
        $root = $this->tmp->getTmpFolder();
        $this->prepareWorkspaceWithTables('abs', 'someday/');

        $configs = [
            [
                'source' => 'table1a',
                'destination' => 'out.c-output-mapping-test.table1a',
            ],
        ];
        $fs = new Filesystem();
        $fs->mkdir($root . '/someday');
        file_put_contents(
            $root . '/someday/table1a.manifest',
            json_encode(
                ['columns' => ['Id', 'Name']]
            )
        );

        $writer = new TableWriter($factory);
        $tableQueue = $writer->uploadTables(
            'someday',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'workspace-abs'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables('out.c-output-mapping-test');
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-output-mapping-test.table1a', $tables[0]['id']);
        $job = $this->clientWrapper->getBasicClient()->getJob($jobIds[0]);
        $this->assertEquals('out.c-output-mapping-test.table1a', $job['tableId']);
        $this->assertEquals(false, $job['operationParams']['params']['incremental']);
        $this->assertEquals(['Id', 'Name'], $job['operationParams']['params']['columns']);
        $data = $this->clientWrapper->getBasicClient()->getTableDataPreview('out.c-output-mapping-test.table1a');
        $rows = explode("\n", trim($data));
        sort($rows);
        $this->assertEquals(['"Id","Name"', '"aabb","ccdd"', '"test","test"'], $rows);
    }

    public function testAbsTableSlicedOutputMapping()
    {
        $factory = $this->getStagingFactory(null, 'json', null, [StrategyFactory::WORKSPACE_ABS, 'abs']);
        // initialize the workspace mock
        $factory->getTableOutputStrategy(StrategyFactory::WORKSPACE_ABS)->getDataStorage()->getWorkspaceId();
        $root = $this->tmp->getTmpFolder();
        $blobClient = BlobRestProxy::createBlobService($this->workspaceCredentials['connectionString']);
        $content = "\"first value\",\"second value\"\n";
        $blobClient->createBlockBlob($this->workspaceCredentials['container'], 'data/out/tables/table1a.csv/slice1', $content);
        $content = "\"secondRow1\",\"secondRow2\"\n";
        $blobClient->createBlockBlob($this->workspaceCredentials['container'], 'data/out/tables/table1a.csv/slice2', $content);

        $configs = [
            [
                'source' => 'table1a.csv',
                'destination' => 'out.c-output-mapping-test.table1a',
            ]
        ];
        $fs = new Filesystem();
        $fs->mkdir($root . '/data/out/tables/');
        file_put_contents(
            $root . '/data/out/tables/table1a.csv.manifest',
            json_encode(
                ['columns' => ['First column', 'Second column']]
            )
        );

        $writer = new TableWriter($factory);
        $tableQueue = $writer->uploadTables(
            'data/out/tables/',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'workspace-abs'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables('out.c-output-mapping-test');
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-output-mapping-test.table1a', $tables[0]['id']);
        $this->assertNotEmpty($jobIds[0]);
        $job = $this->clientWrapper->getBasicClient()->getJob($jobIds[0]);
        $this->assertEquals('out.c-output-mapping-test.table1a', $job['tableId']);
        $this->assertEquals(false, $job['operationParams']['params']['incremental']);
        $this->assertEquals(['First column', 'Second column'], $job['operationParams']['params']['columns']);
        $data = $this->clientWrapper->getBasicClient()->getTableDataPreview('out.c-output-mapping-test.table1a');
        $rows = explode("\n", trim($data));
        sort($rows);
        $this->assertEquals(
            ['"First_column","Second_column"', '"first value","second value"', '"secondRow1","secondRow2"'],
            $rows
        );
    }

    public function testAbsTableSingleFileOutputMapping()
    {
        $factory = $this->getStagingFactory(null, 'json', null, [StrategyFactory::WORKSPACE_ABS, 'abs']);
        // initialize the workspace mock
        $factory->getTableOutputStrategy(StrategyFactory::WORKSPACE_ABS)->getDataStorage()->getWorkspaceId();
        $root = $this->tmp->getTmpFolder();
        $blobClient = BlobRestProxy::createBlobService($this->workspaceCredentials['connectionString']);
        $content = "\"First column\",\"Second Column\"\n\"first value\",\"second value\"\n\"secondRow1\",\"secondRow2\"";
        $blobClient->createBlockBlob($this->workspaceCredentials['container'], 'data/out/tables/table1a.csv', $content);
        $content = "\"First column\",\"Id\"\n\"first\",\"second\"\n\"third\",\"fourth\"";
        $blobClient->createBlockBlob($this->workspaceCredentials['container'], 'data/out/tables/table1a.csv2', $content);

        $configs = [
            [
                'source' => 'table1a.csv',
                'destination' => 'out.c-output-mapping-test.table1a',
                'incremental' => true,
                'columns' => ['first column'],
            ],
            [
                'source' => 'table1a.csv2',
                'destination' => 'out.c-output-mapping-test.table2a',
            ],
        ];
        $fs = new Filesystem();
        $fs->mkdir($root . '/data/out/tables/');
        file_put_contents(
            $root . '/data/out/tables/table1a.csv.manifest',
            json_encode(
                ['columns' => ['first column', 'second column']]
            )
        );
        file_put_contents(
            $root . '/data/out/tables/table1a.csv2.manifest',
            json_encode(
                ['columns' => ['first column', 'second column']]
            )
        );

        $writer = new TableWriter($factory);
        $tableQueue = $writer->uploadTables(
            'data/out/tables/',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'workspace-abs'
        );
        $jobIds = $tableQueue->waitForAll();

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
        $this->assertEquals(['first column'], $job['operationParams']['params']['columns']);
        $data = $this->clientWrapper->getBasicClient()->getTableDataPreview('out.c-output-mapping-test.table1a');
        $rows = explode("\n", trim($data));
        sort($rows);
        // column name is lowercase because of https://keboola.atlassian.net/browse/KBC-864
        // 1a has only the first_column column
        $this->assertEquals(['"first value"', '"first_column"', '"secondRow1"'], $rows);

        $job = $this->clientWrapper->getBasicClient()->getJob($jobIds[1]);
        $this->assertEquals('out.c-output-mapping-test.table2a', $job['tableId']);
        $this->assertEquals(false, $job['operationParams']['params']['incremental']);
        $this->assertEquals(['first column', 'second column'], $job['operationParams']['params']['columns']);

        $data = $this->clientWrapper->getBasicClient()->getTableDataPreview('out.c-output-mapping-test.table2a');
        $rows = explode("\n", trim($data));
        sort($rows);
        // column name is lowercase because of https://keboola.atlassian.net/browse/KBC-864
        $this->assertEquals(['"first","second"', '"first_column","second_column"', '"third","fourth"'], $rows);
    }

    public function testWriteBasicFiles()
    {
        $factory = $this->getStagingFactory(null, 'json', null, [StrategyFactory::WORKSPACE_ABS, 'abs']);
        // initialize the workspace mock
        $factory->getFileOutputStrategy(StrategyFactory::WORKSPACE_ABS);
        $blobClient = BlobRestProxy::createBlobService($this->workspaceCredentials['connectionString']);
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

        $systemMetadata = [
            "componentId" => "testComponent",
            "configurationId" => "metadata-write-test",
            "configurationRowId" => "12345",
            "branchId" => "1234",
        ];

        $writer = new FileWriter($factory);
        $writer->uploadFiles('/upload', ['mapping' => $configs], $systemMetadata, StrategyFactory::WORKSPACE_ABS);
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

        $expectedTags = [
            'output-mapping-test',
            'componentId: testComponent',
            'configurationId: metadata-write-test',
            'configurationRowId: 12345',
            'branchId: 1234',
        ];
        $file2expectedTags = array_merge($expectedTags, ['another-tag']);

        $this->assertNotNull($file1);
        $this->assertNotNull($file2);
        $this->assertNotNull($file3);
        $this->assertEquals(4, $file1['sizeBytes']);
        $this->assertEquals($expectedTags, $file1['tags']);
        $this->assertEquals(sort($file2expectedTags), sort($file2['tags']));
        $this->assertEquals($expectedTags, $file3['tags']);
        $this->assertNotNull($file1['maxAgeDays']);
        $this->assertNull($file2['maxAgeDays']);
        $this->assertNull($file3['maxAgeDays']);
    }
}
