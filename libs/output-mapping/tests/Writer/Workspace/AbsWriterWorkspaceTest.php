<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Workspace;

use Keboola\FileStorage\Abs\ClientFactory;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\InputMapping\Staging\Scope;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Tests\InitSynapseStorageClientTrait;
use Keboola\OutputMapping\Writer\FileWriter;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Symfony\Component\Filesystem\Filesystem;

class AbsWriterWorkspaceTest extends BaseWriterWorkspaceTest
{
    use InitSynapseStorageClientTrait;

    private const INPUT_BUCKET = 'in.c-AbsWriterWorkspaceTest';
    private const OUTPUT_BUCKET = 'out.c-AbsWriterWorkspaceTest';
    private const FILE_TAG = 'AbsWriterWorkspaceTest';

    public function setUp(): void
    {
        if (!$this->checkSynapseTests()) {
            self::markTestSkipped('Synapse tests disabled.');
        }
        parent::setUp();
        $this->clearBuckets([self::INPUT_BUCKET, self::OUTPUT_BUCKET]);
        $this->clearFileUploads([self::FILE_TAG]);
    }

    protected function initClient(?string $branchId = null): void
    {
        $this->clientWrapper = $this->getSynapseClientWrapper();
    }

    protected function getStagingFactory(
        ?ClientWrapper $clientWrapper = null,
        string $format = 'json',
        ?LoggerInterface $logger = null,
        array $backend = [AbstractStrategyFactory::WORKSPACE_SNOWFLAKE, 'snowflake']
    ): StrategyFactory {
        $stagingFactory = new StrategyFactory(
            $clientWrapper ?: $this->clientWrapper,
            $logger ?: new NullLogger(),
            $format
        );
        $mockWorkspace = self::getMockBuilder(NullProvider::class)
            ->setMethods(['getWorkspaceId', 'getCredentials'])
            ->getMock();
        $mockWorkspace->method('getWorkspaceId')->willReturnCallback(
            function () use ($backend) {
                if (!$this->workspaceId) {
                    $workspaces = new Workspaces($this->clientWrapper->getBasicClient());
                    $workspace = $workspaces->createWorkspace(['backend' => $backend[1]], true);
                    $this->workspaceId = (string) $workspace['id'];
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
                    $workspace = $workspaces->createWorkspace(['backend' => $backend[1]], true);
                    $this->workspaceId = (string) $workspace['id'];
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
                $backend[0] => new Scope([Scope::FILE_METADATA, Scope::FILE_DATA, Scope::TABLE_DATA]),
            ]
        );
        return $stagingFactory;
    }

    public function testAbsTableSlicedManifestOutputMapping(): void
    {
        $factory = $this->getStagingFactory(
            null,
            'json',
            null,
            [AbstractStrategyFactory::WORKSPACE_ABS, 'abs']
        );
        // initialize the workspace mock
        $factory->getTableOutputStrategy(AbstractStrategyFactory::WORKSPACE_ABS)->getDataStorage()->getWorkspaceId();
        $root = $this->tmp->getTmpFolder();
        $this->prepareWorkspaceWithTables('abs', 'AbsWriterWorkspaceTest', 'someday/');

        $configs = [
            [
                'source' => 'table1a',
                'destination' => self::OUTPUT_BUCKET . '.table1a',
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
            'workspace-abs',
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        self::assertCount(1, $tables);
        self::assertEquals(self::OUTPUT_BUCKET . '.table1a', $tables[0]['id']);

        $this->assertJobParamsMatches([
            'incremental' => false,
            'columns' => ['Id', 'Name'],
        ], $jobIds[0]);

        $this->assertTableRowsEquals(self::OUTPUT_BUCKET . '.table1a', [
            '"Id","Name"',
            '"aabb","ccdd"',
            '"test","test"',
        ]);
    }

    public function testAbsTableSlicedOutputMapping(): void
    {
        $factory = $this->getStagingFactory(
            null,
            'json',
            null,
            [AbstractStrategyFactory::WORKSPACE_ABS, 'abs']
        );
        // initialize the workspace mock
        $factory->getTableOutputStrategy(AbstractStrategyFactory::WORKSPACE_ABS)->getDataStorage()->getWorkspaceId();
        $root = $this->tmp->getTmpFolder();
        $blobClient = ClientFactory::createClientFromConnectionString($this->workspaceCredentials['connectionString']);
        $content = "\"first value\",\"second value\"\n";
        $blobClient->createBlockBlob(
            $this->workspaceCredentials['container'],
            'data/out/tables/table1a.csv/slice1',
            $content
        );
        $content = "\"secondRow1\",\"secondRow2\"\n";
        $blobClient->createBlockBlob(
            $this->workspaceCredentials['container'],
            'data/out/tables/table1a.csv/slice2',
            $content
        );

        $configs = [
            [
                'source' => 'table1a.csv',
                'destination' => self::OUTPUT_BUCKET . '.table1a',
            ],
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
            'workspace-abs',
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        self::assertCount(1, $tables);
        self::assertEquals(self::OUTPUT_BUCKET . '.table1a', $tables[0]['id']);
        self::assertNotEmpty($jobIds[0]);

        $this->assertJobParamsMatches([
            'incremental' => false,
            'columns' => ['First column', 'Second column'],
        ], $jobIds[0]);

        $this->assertTableRowsEquals(self::OUTPUT_BUCKET . '.table1a', [
            '"First_column","Second_column"',
            '"first value","second value"',
            '"secondRow1","secondRow2"',
        ]);
    }

    public function testAbsTableSingleFileOutputMapping(): void
    {
        $factory = $this->getStagingFactory(
            null,
            'json',
            null,
            [AbstractStrategyFactory::WORKSPACE_ABS, 'abs']
        );
        // initialize the workspace mock
        $factory->getTableOutputStrategy(
            AbstractStrategyFactory::WORKSPACE_ABS
        )->getDataStorage()->getWorkspaceId();

        $root = $this->tmp->getTmpFolder();
        $blobClient = ClientFactory::createClientFromConnectionString($this->workspaceCredentials['connectionString']);
        $content = "\"First column\",\"Second Column\"\n\"first value\"," .
            "\"second value\"\n\"secondRow1\",\"secondRow2\"";
        $blobClient->createBlockBlob(
            $this->workspaceCredentials['container'],
            'data/out/tables/table1a.csv',
            $content
        );
        $content = "\"First column\",\"Id\"\n\"first\",\"second\"\n\"third\",\"fourth\"";
        $blobClient->createBlockBlob(
            $this->workspaceCredentials['container'],
            'data/out/tables/table1a.csv2',
            $content
        );

        $configs = [
            [
                'source' => 'table1a.csv',
                'destination' => self::OUTPUT_BUCKET . '.table1a',
                'incremental' => true,
                'columns' => ['first column'],
            ],
            [
                'source' => 'table1a.csv2',
                'destination' => self::OUTPUT_BUCKET . '.table2a',
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
            'workspace-abs',
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(2, $jobIds);
        self::assertNotEmpty($jobIds[0]);
        self::assertNotEmpty($jobIds[1]);

        $this->assertJobParamsMatches([
            'incremental' => true,
            'columns' => ['first column'],
        ], $jobIds[0]);

        $this->assertJobParamsMatches([
            'incremental' => false,
            'columns' => ['first column', 'second column'],
        ], $jobIds[1]);

        $this->assertTablesExists(
            self::OUTPUT_BUCKET,
            [
                self::OUTPUT_BUCKET . '.table1a',
                self::OUTPUT_BUCKET . '.table2a',
            ]
        );
        $this->assertTableRowsEquals(self::OUTPUT_BUCKET . '.table1a', [
            '"first_column"',
            '"first value"',
            '"secondRow1"',
        ]);
        $this->assertTableRowsEquals(self::OUTPUT_BUCKET . '.table2a', [
            '"first_column","second_column"',
            '"first","second"',
            '"third","fourth"',
        ]);
    }

    public function testWriteBasicFiles(): void
    {
        $factory = $this->getStagingFactory(
            null,
            'json',
            null,
            [AbstractStrategyFactory::WORKSPACE_ABS, 'abs']
        );
        // initialize the workspace mock
        $factory->getFileOutputStrategy(AbstractStrategyFactory::WORKSPACE_ABS);
        $blobClient = ClientFactory::createClientFromConnectionString($this->workspaceCredentials['connectionString']);
        $blobClient->createBlockBlob($this->workspace['connection']['container'], 'upload/file1', 'test');
        $blobClient->createBlockBlob($this->workspace['connection']['container'], 'upload/file2', 'test');
        $blobClient->createBlockBlob(
            $this->workspace['connection']['container'],
            'upload/file2.manifest',
            '{"tags": ["' . self::FILE_TAG . '", "xxx"],"is_public": false}'
        );
        $blobClient->createBlockBlob(
            $this->workspace['connection']['container'],
            'upload/file3',
            'test'
        );
        $blobClient->createBlockBlob(
            $this->workspace['connection']['container'],
            'upload/file3.manifest',
            '{"tags": ["' . self::FILE_TAG . '"],"is_permanent": true}'
        );
        $configs = [
            [
                'source' => 'file1',
                'tags' => [self::FILE_TAG],
            ],
            [
                'source' => 'file2',
                'tags' => [self::FILE_TAG, 'another-tag'],
                'is_permanent' => true,
            ],
        ];

        $systemMetadata = [
            'componentId' => 'testComponent',
            'configurationId' => 'metadata-write-test',
            'configurationRowId' => '12345',
            'branchId' => '1234',
        ];

        $writer = new FileWriter($factory);
        $writer->uploadFiles(
            '/upload',
            ['mapping' => $configs],
            $systemMetadata,
            AbstractStrategyFactory::WORKSPACE_ABS,
            [],
            false
        );
        sleep(1);

        $options = new ListFilesOptions();
        $options->setTags([self::FILE_TAG]);
        $files = $this->clientWrapper->getBasicClient()->listFiles($options);
        self::assertCount(3, $files);

        $file1 = $file2 = $file3 = null;
        foreach ($files as $file) {
            if ($file['name'] === 'file1') {
                $file1 = $file;
            }
            if ($file['name'] === 'file2') {
                $file2 = $file;
            }
            if ($file['name'] === 'file3') {
                $file3 = $file;
            }
        }

        $expectedTags = [
            self::FILE_TAG,
            'componentId: testComponent',
            'configurationId: metadata-write-test',
            'configurationRowId: 12345',
            'branchId: 1234',
        ];
        $file2expectedTags = array_merge($expectedTags, ['another-tag']);

        self::assertNotNull($file1);
        self::assertNotNull($file2);
        self::assertNotNull($file3);
        self::assertEquals(4, $file1['sizeBytes']);
        self::assertEquals($expectedTags, $file1['tags']);
        self::assertEquals(sort($file2expectedTags), sort($file2['tags']));
        self::assertEquals($expectedTags, $file3['tags']);
        self::assertNotNull($file1['maxAgeDays']);
        self::assertNull($file2['maxAgeDays']);
        self::assertNull($file3['maxAgeDays']);
    }
}
