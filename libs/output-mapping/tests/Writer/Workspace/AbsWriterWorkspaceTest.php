<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Workspace;

use Keboola\Csv\CsvFile;
use Keboola\FileStorage\Abs\ClientFactory;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\InitSynapseStorageClientTrait;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\OutputMapping\Tests\Needs\TestSatisfyer;
use Keboola\OutputMapping\Writer\FileWriter;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\ListFilesOptions;
use Symfony\Component\Filesystem\Filesystem;

class AbsWriterWorkspaceTest extends AbstractTestCase
{
    use InitSynapseStorageClientTrait;

    private const FILE_TAG = 'AbsWriterWorkspaceTest';

    public function setUp(): void
    {
        if (!$this->checkSynapseTests()) {
            self::markTestSkipped('Synapse tests disabled.');
        }
        parent::setUp();
        $this->clearFileUploads([self::FILE_TAG]);
    }

    protected function initClient(?string $branchId = null): void
    {
        $this->clientWrapper = $this->getSynapseClientWrapper();
    }

    #[NeedsEmptyOutputBucket]
    public function testAbsTableSlicedManifestOutputMapping(): void
    {
        $factory = $this->getWorkspaceStagingFactory(
            null,
            'json',
            null,
            [AbstractStrategyFactory::WORKSPACE_ABS, 'abs'],
        );

        $bucketName = 'testAbsTableSlicedManifestOutputMapping';
        $bucketId = TestSatisfyer::getBucketIdByDisplayName($this->clientWrapper, $bucketName, Client::STAGE_IN);
        if ($bucketId !== null) {
            $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($bucketId, ['include' => '']);
            foreach ($tables as $table) {
                $this->clientWrapper->getTableAndFileStorageClient()->dropTable($table['id']);
            }
        } else {
            $bucketId  = $this->clientWrapper->getTableAndFileStorageClient()->createBucket(
                name: $bucketName,
                stage: Client::STAGE_IN,
                backend: 'synapse',
            );
        }
        $tableIds = [];
        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload.csv');
        $csv->writeRow(['Id', 'Name', 'foo', 'bar']);
        $csv->writeRow(['id1', 'name1', 'foo1', 'bar1']);
        $csv->writeRow(['id2', 'name2', 'foo2', 'bar2']);
        $csv->writeRow(['id3', 'name3', 'foo3', 'bar3']);

        for ($i = 0; $i < 2; $i++) {
            $tableIds[$i] = $this->clientWrapper->getTableAndFileStorageClient()->createTableAsync(
                $bucketId,
                'test' . ($i + 1),
                $csv,
            );
        }

        // initialize the workspace mock
        $factory->getTableOutputStrategy(AbstractStrategyFactory::WORKSPACE_ABS)->getDataStorage()->getWorkspaceId();
        $root = $this->temp->getTmpFolder();
        $this->prepareWorkspaceWithTables($bucketId, 'someday/');

        $configs = [
            [
                'source' => 'table1a',
                'destination' => $this->emptyOutputBucketId . '.table1a',
            ],
        ];
        $fs = new Filesystem();
        $fs->mkdir($root . '/someday');
        file_put_contents(
            $root . '/someday/table1a.manifest',
            json_encode(
                ['columns' => ['Id', 'Name']],
            ),
        );

        $writer = new TableWriter($factory);
        $tableQueue = $writer->uploadTables(
            'someday',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'workspace-abs',
            false,
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);
        self::assertEquals($this->emptyOutputBucketId . '.table1a', $tables[0]['id']);

        $this->assertJobParamsMatches([
            'incremental' => false,
            'columns' => ['Id', 'Name'],
        ], $jobIds[0]);

        $this->assertTableRowsEquals($this->emptyOutputBucketId . '.table1a', [
            '"id","name"',
            '"id1","name1"',
            '"id2","name2"',
            '"id3","name3"',
        ]);
    }

    #[NeedsEmptyOutputBucket]
    public function testAbsTableSlicedOutputMapping(): void
    {
        $factory = $this->getWorkspaceStagingFactory(
            null,
            'json',
            null,
            [AbstractStrategyFactory::WORKSPACE_ABS, 'abs'],
        );
        // initialize the workspace mock
        $factory->getTableOutputStrategy(AbstractStrategyFactory::WORKSPACE_ABS)->getDataStorage()->getWorkspaceId();
        $root = $this->temp->getTmpFolder();
        $blobClient = ClientFactory::createClientFromConnectionString($this->workspaceCredentials['connectionString']);
        $content = "\"first value\",\"second value\"\n";
        $blobClient->createBlockBlob(
            $this->workspaceCredentials['container'],
            'data/out/tables/table1a.csv/slice1',
            $content,
        );
        $content = "\"secondRow1\",\"secondRow2\"\n";
        $blobClient->createBlockBlob(
            $this->workspaceCredentials['container'],
            'data/out/tables/table1a.csv/slice2',
            $content,
        );

        $configs = [
            [
                'source' => 'table1a.csv',
                'destination' => $this->emptyOutputBucketId . '.table1a',
            ],
        ];
        $fs = new Filesystem();
        $fs->mkdir($root . '/data/out/tables/');
        file_put_contents(
            $root . '/data/out/tables/table1a.csv.manifest',
            json_encode(
                ['columns' => ['First column', 'Second column']],
            ),
        );

        $writer = new TableWriter($factory);
        $tableQueue = $writer->uploadTables(
            'data/out/tables/',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'workspace-abs',
            false,
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);
        self::assertEquals($this->emptyOutputBucketId . '.table1a', $tables[0]['id']);
        self::assertNotEmpty($jobIds[0]);

        $this->assertJobParamsMatches([
            'incremental' => false,
            'columns' => ['First column', 'Second column'],
        ], $jobIds[0]);

        $this->assertTableRowsEquals($this->emptyOutputBucketId . '.table1a', [
            '"first value","second value"',
            '"first_column","second_column"',
            '"secondrow1","secondrow2"',
        ]);
    }

    #[NeedsEmptyOutputBucket]
    public function testAbsTableSingleFileOutputMapping(): void
    {
        $factory = $this->getWorkspaceStagingFactory(
            null,
            'json',
            null,
            [AbstractStrategyFactory::WORKSPACE_ABS, 'abs'],
        );
        // initialize the workspace mock
        $factory->getTableOutputStrategy(
            AbstractStrategyFactory::WORKSPACE_ABS,
        )->getDataStorage()->getWorkspaceId();

        $root = $this->temp->getTmpFolder();
        $blobClient = ClientFactory::createClientFromConnectionString($this->workspaceCredentials['connectionString']);
        $content = "\"First column\",\"Second Column\"\n\"first value\"," .
            "\"second value\"\n\"secondRow1\",\"secondRow2\"";
        $blobClient->createBlockBlob(
            $this->workspaceCredentials['container'],
            'data/out/tables/table1a.csv',
            $content,
        );
        $content = "\"First column\",\"Id\"\n\"first\",\"second\"\n\"third\",\"fourth\"";
        $blobClient->createBlockBlob(
            $this->workspaceCredentials['container'],
            'data/out/tables/table1a.csv2',
            $content,
        );

        $configs = [
            [
                'source' => 'table1a.csv',
                'destination' => $this->emptyOutputBucketId . '.table1a',
                'incremental' => true,
                'columns' => ['first column'],
            ],
            [
                'source' => 'table1a.csv2',
                'destination' => $this->emptyOutputBucketId . '.table2a',
            ],
        ];
        $fs = new Filesystem();
        $fs->mkdir($root . '/data/out/tables/');
        file_put_contents(
            $root . '/data/out/tables/table1a.csv.manifest',
            json_encode(
                ['columns' => ['first column', 'second column']],
            ),
        );
        file_put_contents(
            $root . '/data/out/tables/table1a.csv2.manifest',
            json_encode(
                ['columns' => ['first column', 'second column']],
            ),
        );

        $writer = new TableWriter($factory);
        $tableQueue = $writer->uploadTables(
            'data/out/tables/',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'workspace-abs',
            false,
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
            $this->emptyOutputBucketId,
            [
                $this->emptyOutputBucketId . '.table1a',
                $this->emptyOutputBucketId . '.table2a',
            ],
        );
        $this->assertTableRowsEquals($this->emptyOutputBucketId . '.table1a', [
            '"first_column"',
            '"first value"',
            '"secondrow1"',
        ]);
        $this->assertTableRowsEquals($this->emptyOutputBucketId . '.table2a', [
            '"first_column","second_column"',
            '"first","second"',
            '"third","fourth"',
        ]);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteBasicFiles(): void
    {
        $factory = $this->getWorkspaceStagingFactory(
            null,
            'json',
            null,
            [AbstractStrategyFactory::WORKSPACE_ABS, 'abs'],
        );
        // initialize the workspace mock
        $factory->getFileOutputStrategy(AbstractStrategyFactory::WORKSPACE_ABS);
        $blobClient = ClientFactory::createClientFromConnectionString($this->workspaceCredentials['connectionString']);
        $blobClient->createBlockBlob($this->workspaceCredentials['container'], 'upload/file1', 'test');
        $blobClient->createBlockBlob($this->workspaceCredentials['container'], 'upload/file2', 'test');
        $blobClient->createBlockBlob(
            $this->workspaceCredentials['container'],
            'upload/file2.manifest',
            '{"tags": ["' . self::FILE_TAG . '", "xxx"],"is_public": false}',
        );
        $blobClient->createBlockBlob(
            $this->workspaceCredentials['container'],
            'upload/file3',
            'test',
        );
        $blobClient->createBlockBlob(
            $this->workspaceCredentials['container'],
            'upload/file3.manifest',
            '{"tags": ["' . self::FILE_TAG . '"],"is_permanent": true}',
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
            false,
        );
        sleep(1);

        $options = new ListFilesOptions();
        $options->setTags([self::FILE_TAG]);
        $files = $this->clientWrapper->getTableAndFileStorageClient()->listFiles($options);
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
