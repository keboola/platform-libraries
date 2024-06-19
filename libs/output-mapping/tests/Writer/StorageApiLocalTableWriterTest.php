<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Table\Result\TableInfo;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\OutputMappingSettings;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\OutputMapping\Tests\Needs\NeedsTestTables;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\TableExporter;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\StorageApiToken;

class StorageApiLocalTableWriterTest extends AbstractTestCase
{
    use CreateBranchTrait;

    private const BRANCH_NAME = self::class;

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMapping(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/table1a.csv',
            "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n",
        );
        file_put_contents(
            $root . '/upload/table2a.csv',
            "\"Id2\",\"Name2\"\n\"test2\",\"test2\"\n\"aabb2\",\"ccdd2\"\n",
        );

        $configs = [
            [
                'source' => 'table1a.csv',
                'destination' => $this->emptyOutputBucketId . '.table1a',
            ],
            [
                'source' => 'table2a.csv',
                'destination' => $this->emptyOutputBucketId . '.table2a',
            ],
        ];

        $writer = new TableWriter($this->getLocalStagingFactory());

        $tableQueue =  $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(2, $jobIds);

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(2, $tables);
        $tableIds = [$tables[0]['id'], $tables[1]['id']];
        sort($tableIds);
        self::assertEquals(
            [$this->emptyOutputBucketId . '.table1a', $this->emptyOutputBucketId . '.table2a'],
            $tableIds,
        );
        self::assertCount(2, $jobIds);
        self::assertNotEmpty($jobIds[0]);
        self::assertNotEmpty($jobIds[1]);

        $job = $this->clientWrapper->getBranchClient()->getJob($jobIds[0]);
        $fileId = $job['operationParams']['source']['fileId'];
        $file = $this->clientWrapper->getTableAndFileStorageClient()->getFile($fileId);
        self::assertEquals([], $file['tags']);

        /** @var TableInfo[] $tables */
        $tables = iterator_to_array($tableQueue->getTableResult()->getTables());
        self::assertCount(2, $tables);

        /** @var TableInfo[] $tables */
        $tables = iterator_to_array($tableQueue->getTableResult()->getTables());
        self::assertCount(2, $tables);

        $tableIds = array_map(function ($table) {
            return $table->getId();
        }, $tables);

        sort($tableIds);
        self::assertSame([
            $this->emptyOutputBucketId . '.table1a',
            $this->emptyOutputBucketId . '.table2a',
        ], $tableIds);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableTagStagingFiles(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/table1a.csv',
            "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n",
        );
        file_put_contents(
            $root . '/upload/table2a.csv',
            "\"Id2\",\"Name2\"\n\"test2\",\"test2\"\n\"aabb2\",\"ccdd2\"\n",
        );

        $configs = [
            [
                'source' => 'table1a.csv',
                'destination' => $this->emptyOutputBucketId . '.table1a',
            ],
            [
                'source' => 'table2a.csv',
                'destination' => $this->emptyOutputBucketId . '.table2a',
            ],
        ];

        $tokenHasOutputMappingSliceFeature = $this->clientWrapper->getToken()
            ->hasFeature(TableWriter::OUTPUT_MAPPING_SLICE_FEATURE)
        ;

        $token = $this->createMock(StorageApiToken::class);
        $token
            ->method('hasFeature')
            ->willReturnCallback(function (string $feature) use ($tokenHasOutputMappingSliceFeature): bool {
                if ($feature === TableWriter::OUTPUT_MAPPING_SLICE_FEATURE) {
                    return $tokenHasOutputMappingSliceFeature;
                }

                return $feature === 'tag-staging-files';
            })
        ;

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getToken')->willReturn($token);
        $clientWrapper->method('getBranchClient')->willReturn(
            $this->clientWrapper->getBranchClient(),
        );
        $clientWrapper->method('getTableAndFileStorageClient')->willReturn(
            $this->clientWrapper->getBranchClient(),
        );
        $writer = new TableWriter($this->getLocalStagingFactory($clientWrapper));

        $tableQueue =  $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(2, $jobIds);
        self::assertNotEmpty($jobIds[0]);
        self::assertNotEmpty($jobIds[1]);

        $this->assertTablesExists(
            $this->emptyOutputBucketId,
            [
                $this->emptyOutputBucketId . '.table1a',
                $this->emptyOutputBucketId . '.table2a',
            ],
        );

        $job = $this->clientWrapper->getBranchClient()->getJob($jobIds[0]);
        $fileId = $job['operationParams']['source']['fileId'];
        $file = $this->clientWrapper->getTableAndFileStorageClient()->getFile($fileId);
        self::assertEquals(
            ['componentId: foo'],
            $file['tags'],
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingFakeDevMode(): void
    {
        $clientWrapper= new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN_MASTER'),
            ),
        );
        $branchId = $this->createBranch($clientWrapper, self::class);
        $this->clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN'),
                $branchId,
            ),
        );

        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/table11a.csv',
            "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n",
        );
        file_put_contents(
            $root . '/upload/table21a.csv',
            "\"Id2\",\"Name2\"\n\"test2\",\"test2\"\n\"aabb2\",\"ccdd2\"\n",
        );

        $configs = [
            [
                'source' => 'table11a.csv',
                'destination' => $this->emptyOutputBucketId . '.table11a',
            ],
            [
                'source' => 'table21a.csv',
                'destination' => $this->emptyOutputBucketId . '.table21a',
            ],
        ];
        $writer = new TableWriter($this->getLocalStagingFactory());
        $tableQueue =  $writer->uploadTables(
            '/upload',
            ['mapping' => $configs],
            ['componentId' => 'foo', 'branchId' => $branchId],
            'local',
            false,
            'none',
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(2, $jobIds);
        self::assertNotEmpty($jobIds[0]);
        self::assertNotEmpty($jobIds[1]);

        $jobDetail = $this->clientWrapper->getBranchClient()->getJob($jobIds[0]);
        $tableIds[] = $this->getTableIdFromJobDetail($jobDetail);
        $jobDetail = $this->clientWrapper->getBranchClient()->getJob($jobIds[1]);
        $tableIds[] = $this->getTableIdFromJobDetail($jobDetail);

        sort($tableIds);
        self::assertMatchesRegularExpression(
            '#out\.(c-)?' . $branchId . '-testWriteTableOutputMappingFakeDevModeEmpty\.table11a#',
            $tableIds[0],
        );

        self::assertMatchesRegularExpression(
            '#out\.(c-)?' . $branchId . '-testWriteTableOutputMappingFakeDevModeEmpty\.table21a#',
            $tableIds[1],
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingRealDevMode(): void
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions(
                url: (string) getenv('STORAGE_API_URL'),
                token: (string) getenv('STORAGE_API_TOKEN_MASTER'),
                useBranchStorage: true, // this is the important part
            ),
        );
        $branchId = $this->createBranch($clientWrapper, self::class);
        $this->clientWrapper = new ClientWrapper(
            new ClientOptions(
                url: (string) getenv('STORAGE_API_URL'),
                token: (string) getenv('STORAGE_API_TOKEN'),
                branchId: $branchId,
                useBranchStorage: true,
            ),
        );

        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/table11a.csv',
            "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n",
        );
        file_put_contents(
            $root . '/upload/table21a.csv',
            "\"Id2\",\"Name2\"\n\"test2\",\"test2\"\n\"aabb2\",\"ccdd2\"\n",
        );

        $configs = [
            [
                'source' => 'table11a.csv',
                'destination' => $this->emptyOutputBucketId . '.table11a',
            ],
            [
                'source' => 'table21a.csv',
                'destination' => $this->emptyOutputBucketId . '.table21a',
            ],
        ];
        // pass the special client wrapper
        $writer = new TableWriter($this->getLocalStagingFactory($this->clientWrapper));
        $tableQueue =  $writer->uploadTables(
            '/upload',
            ['mapping' => $configs],
            ['componentId' => 'foo', 'branchId' => $branchId],
            'local',
            false,
            'none',
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(2, $jobIds);
        self::assertNotEmpty($jobIds[0]);
        self::assertNotEmpty($jobIds[1]);

        $jobDetail = $this->clientWrapper->getBranchClient()->getJob($jobIds[0]);
        $tableIds[] = $this->getTableIdFromJobDetail($jobDetail);
        $jobDetail = $this->clientWrapper->getBranchClient()->getJob($jobIds[1]);
        $tableIds[] = $this->getTableIdFromJobDetail($jobDetail);

        sort($tableIds);
        self::assertSame('out.c-testWriteTableOutputMappingRealDevModeEmpty.table11a', $tableIds[0]);
        self::assertSame('out.c-testWriteTableOutputMappingRealDevModeEmpty.table21a', $tableIds[1]);
        // tables exist in the branch
        $this->clientWrapper->getBranchClient()->tableExists(
            'out.c-testWriteTableOutputMappingRealDevModeEmpty.table11a',
        );
        $this->clientWrapper->getBranchClient()->tableExists(
            'out.c-testWriteTableOutputMappingRealDevModeEmpty.table21a',
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingExistingTable(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/table21.csv',
            "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n",
        );

        $configs = [
            [
                'source' => 'table21.csv',
                'destination' => $this->emptyOutputBucketId . '.table21',
            ],
        ];

        $writer = new TableWriter($this->getLocalStagingFactory());
        $tableQueue =  $writer->uploadTables(
            '/upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        // And again
        $tableQueue =  $writer->uploadTables(
            '/upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);
        self::assertEquals($this->emptyOutputBucketId . '.table21', $tables[0]['id']);
        self::assertNotEmpty($jobIds[0]);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingWithoutCsv(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/table31',
            "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n",
        );

        $configs = [
            [
                'source' => 'table31',
                'destination' => $this->emptyOutputBucketId . '.table31',
            ],
        ];

        $writer = new TableWriter($this->getLocalStagingFactory());

        $tableQueue =  $writer->uploadTables(
            '/upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);
        self::assertEquals($this->emptyOutputBucketId . '.table31', $tables[0]['id']);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingEmptyFile(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/table41', '');

        $configs = [
            [
                'source' => 'table41',
                'destination' => $this->emptyOutputBucketId . '.table41',
            ],
        ];

        $writer = new TableWriter($this->getLocalStagingFactory());
        $tableQueue = $writer->uploadTables(
            '/upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage(
            'Failed to load table "' . $this->emptyOutputBucketId . '.table41": There are no data in import file',
        );
        $tableQueue->waitForAll();
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingAndManifest(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . 'upload/table2.csv',
            "\"Id\",\"Name\"\n\"test\",\"test\"\n",
        );
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . 'upload/table2.csv.manifest',
            '{"destination": "' . $this->emptyOutputBucketId . '.table2","primary_key": ["Id"]}',
        );

        $configs = [
            [
                'source' => 'table2.csv',
                'destination' => $this->emptyOutputBucketId . '.table',
            ],
        ];

        $writer = new TableWriter($this->getLocalStagingFactory());

        $tableQueue =  $writer->uploadTables(
            '/upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);
        self::assertEquals($this->emptyOutputBucketId . '.table', $tables[0]['id']);
        self::assertEquals(['Id'], $tables[0]['primaryKey']);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableInvalidManifest(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . 'upload/' . $this->emptyOutputBucketId . '.table3b.csv',
            "\"Id\",\"Name\"\n\"test\",\"test\"\n",
        );
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . 'upload/' . $this->emptyOutputBucketId . '.table3b.csv.manifest',
            '{"destination": "' . $this->emptyOutputBucketId . '.table3","primary_key": "Id"}',
        );

        $writer = new TableWriter($this->getLocalStagingFactory());
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Invalid type for path');
        $writer->uploadTables(
            '/upload',
            [],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableManifestCsvDefaultBackend(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . 'upload/' . $this->emptyOutputBucketId . '.table3c.csv',
            "'Id'\t'Name'\n'test'\t'test''s'\n",
        );
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . 'upload/' . $this->emptyOutputBucketId . '.table3c.csv.manifest',
            '{"destination": "' . $this->emptyOutputBucketId . '.table3c","delimiter": "\t","enclosure": "\'"}',
        );

        $writer = new TableWriter($this->getLocalStagingFactory());

        $tableQueue =  $writer->uploadTables(
            '/upload',
            [],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);
        self::assertEquals($this->emptyOutputBucketId . '.table3c', $tables[0]['id']);
        $exporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $downloadedFile = $root . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyOutputBucketId . '.table3c', $downloadedFile, []);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->parseCsv(
            (string) file_get_contents($downloadedFile),
        );
        self::assertCount(1, $table);
        self::assertCount(2, $table[0]);
        self::assertArrayHasKey('Id', $table[0]);
        self::assertArrayHasKey('Name', $table[0]);
        self::assertEquals('test', $table[0]['Id']);
        self::assertEquals('test\'s', $table[0]['Name']);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOrphanedManifest(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . 'upload/table.csv.manifest',
            '{"destination": "' . $this->emptyOutputBucketId . '.table3e","primary_key": ["Id", "Name"]}',
        );
        $writer = new TableWriter($this->getLocalStagingFactory());
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Found orphaned table manifest: "table.csv.manifest"');
        $writer->uploadTables(
            '/upload',
            [],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingMissing(): void
    {
        $configs = [
            [
                'source' => 'table81.csv',
                'destination' => $this->emptyOutputBucketId . '.table81',
            ],
        ];
        $writer = new TableWriter($this->getLocalStagingFactory());
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Table sources not found: "table81.csv"');
        $writer->uploadTables(
            '/upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableMetadataMissing(): void
    {
        $writer = new TableWriter($this->getLocalStagingFactory());
        $this->expectException(OutputOperationException::class);
        $this->expectExceptionMessage('Component Id must be set');
        $writer->uploadTables(
            '/upload',
            [],
            [],
            'local',
            false,
            'none',
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableIncrementalWithDeleteDefault(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/table51.csv',
            "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n",
        );

        $configs = [
            [
                'source' => 'table51.csv',
                'destination' => $this->emptyOutputBucketId . '.table51',
                'delete_where_column' => 'Id',
                'delete_where_values' => ['aabb'],
                'delete_where_operator' => 'eq',
                'incremental' => true,
            ],
        ];

        $writer = new TableWriter($this->getLocalStagingFactory());

        $tableQueue =  $writer->uploadTables(
            '/upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);
        $this->clientWrapper->getBranchClient()->handleAsyncTasks($jobIds);

        // And again, check first incremental table
        $tableQueue =  $writer->uploadTables(
            '/upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);
        $this->clientWrapper->getBranchClient()->handleAsyncTasks($jobIds);

        $exporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $exporter->exportTable(
            $this->emptyOutputBucketId . '.table51',
            $root . DIRECTORY_SEPARATOR . 'download.csv',
            [],
        );
        $table = $this->clientWrapper->getTableAndFileStorageClient()->parseCsv(
            (string) file_get_contents($root . DIRECTORY_SEPARATOR . 'download.csv'),
        );
        usort($table, function ($a, $b) {
            return strcasecmp($a['Id'], $b['Id']);
        });
        self::assertCount(3, $table);
        self::assertCount(2, $table[0]);
        self::assertArrayHasKey('Id', $table[0]);
        self::assertArrayHasKey('Name', $table[0]);
        self::assertEquals('aabb', $table[0]['Id']);
        self::assertEquals('ccdd', $table[0]['Name']);
        self::assertEquals('test', $table[1]['Id']);
        self::assertEquals('test', $table[1]['Name']);
        self::assertEquals('test', $table[2]['Id']);
        self::assertEquals('test', $table[2]['Name']);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableToDefaultBucket(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/table71.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n");
        file_put_contents($root . '/upload/table71.csv.manifest', '{}');

        $writer = new TableWriter($this->getLocalStagingFactory());

        $tableQueue =  $writer->uploadTables(
            '/upload',
            ['bucket' => $this->emptyOutputBucketId],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);
        self::assertEquals(1, $tableQueue->getTaskCount());

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);

        self::assertEquals($this->emptyOutputBucketId . '.table71', $tables[0]['id']);
        $tableInfo = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId . '.table71',
        );
        self::assertEquals(['Id', 'Name'], $tableInfo['columns']);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableManifestWithDefaultBucket(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/table6.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n");
        file_put_contents($root . '/upload/table6.csv.manifest', '{"primary_key": ["Id", "Name"]}');

        $writer = new TableWriter($this->getLocalStagingFactory());

        $tableQueue =  $writer->uploadTables(
            '/upload',
            ['bucket' => $this->emptyOutputBucketId],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);
        self::assertEquals($this->emptyOutputBucketId . '.table6', $tables[0]['id']);
        self::assertEquals(['Id', 'Name'], $tables[0]['primaryKey']);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingWithPk(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/table16.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $writer = new TableWriter($this->getLocalStagingFactory());
        $tableQueue =  $writer->uploadTables(
            'upload',
            [
                'mapping' => [
                    [
                        'source' => 'table16.csv',
                        'destination' => $this->emptyOutputBucketId . '.table16',
                        'primary_key' => ['Id'],
                    ],
                ],
            ],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);
        $tableInfo = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId . '.table16',
        );
        self::assertEquals(['Id'], $tableInfo['primaryKey']);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingWithPkOverwrite(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/table15.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $writer = new TableWriter($this->getLocalStagingFactory());
        $tableQueue = $writer->uploadTables(
            'upload',
            [
                'mapping' => [
                    [
                        'source' => 'table15.csv',
                        'destination' => $this->emptyOutputBucketId . '.table15',
                        'primary_key' => ['Id'],
                    ],
                ],
            ],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        $tableQueue->waitForAll();

        $writer = new TableWriter($this->getLocalStagingFactory(
            logger: $this->testLogger,
        ));
        $tableQueue =  $writer->uploadTables(
            '/upload',
            [
                'mapping' => [
                    [
                        'source' => 'table15.csv',
                        'destination' => $this->emptyOutputBucketId . '.table15',
                        'primary_key' => ['Id'],
                    ],
                ],
            ],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tableInfo = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId . '.table15',
        );
        self::assertFalse($this->testHandler->hasWarning(
            'Modifying primary key of table "out.c-testWriteTableOutputMappingWithPkOverwriteEmpty.table15" ' .
            'from "Id" to "Id".',
        ));
        self::assertEquals(['Id'], $tableInfo['primaryKey']);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingWithEmptyStringPk(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/table12.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $writer = new TableWriter($this->getLocalStagingFactory(null, 'json', $this->testLogger));
        $tableQueue = $writer->uploadTables(
            'upload',
            [
                'mapping' => [
                    [
                        'source' => 'table12.csv',
                        'destination' => $this->emptyOutputBucketId . '.table12',
                        'primary_key' => [],
                    ],
                ],
            ],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        $tableQueue->waitForAll();
        $tableInfo = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId . '.table12',
        );
        self::assertEquals([], $tableInfo['primaryKey']);

        $writer = new TableWriter($this->getLocalStagingFactory(null, 'json', $this->testLogger));
        $tableQueue =  $writer->uploadTables(
            'upload',
            [
                'mapping' => [
                    [
                        'source' => 'table12.csv',
                        'destination' => $this->emptyOutputBucketId . '.table12',
                        'primary_key' => [''],
                    ],
                ],
            ],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);
        $this->clientWrapper->getBranchClient()->handleAsyncTasks($jobIds);
        self::assertFalse(
            $this->testHandler->hasWarningThatContains('Output mapping does not match destination table'),
        );
        $tableInfo = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId . '.table12',
        );
        self::assertEquals([], $tableInfo['primaryKey']);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingWithEmptyStringPkInManifest(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/table11.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $writer = new TableWriter($this->getLocalStagingFactory(null, 'json', $this->testLogger));
        $tableQueue =  $writer->uploadTables(
            'upload',
            [
                'mapping' => [
                    [
                        'source' => 'table11.csv',
                        'destination' => $this->emptyOutputBucketId . '.table11',
                        'primary_key' => [],
                    ],
                ],
            ],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $root = $this->createTemp()->getTmpFolder();
        file_put_contents($root . '/upload/table11.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $writer = new TableWriter($this->getLocalStagingFactory(null, 'json', $this->testLogger, $root));
        file_put_contents(
            $root . '/upload/table11.csv.manifest',
            '{"destination": "' . $this->emptyOutputBucketId . '.table11","primary_key": [""]}',
        );
        $tableQueue =  $writer->uploadTables(
            'upload',
            [],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);
        self::assertFalse(
            $this->testHandler->hasWarningThatContains(
                "Output mapping does not match destination table: primary key '' does not match '' in '" .
                $this->emptyOutputBucketId . ".table9'.",
            ),
        );
        $tableInfo = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId . '.table11',
        );
        self::assertEquals([], $tableInfo['primaryKey']);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableColumnsOverwrite(): void
    {
        if ($this->clientWrapper->getToken()->hasFeature(TableWriter::OUTPUT_MAPPING_SLICE_FEATURE)) {
            $this->expectException(InvalidOutputException::class);
            $this->expectExceptionMessage(
                'Params "delimiter", "enclosure" or "columns" '
                . 'specified in mapping are not longer supported.',
            );
        }

        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/' . $this->emptyOutputBucketId . '.table10.csv',
            "\"Id\",\"Name\"\n\"test\",\"test\"\n",
        );
        $writer = new TableWriter($this->getLocalStagingFactory(stagingPath: $root));
        $configuration = [
            'mapping' => [
                [
                    'source' => $this->emptyOutputBucketId . '.table10.csv',
                    'destination' => $this->emptyOutputBucketId . '.table10',
                ],
            ],
        ];
        $tableQueue = $writer->uploadTables(
            'upload',
            $configuration,
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        $tableQueue->waitForAll();

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);

        self::assertEquals($this->emptyOutputBucketId . '.table10', $tables[0]['id']);
        $tableInfo = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId . '.table10',
        );
        self::assertEquals(['Id', 'Name'], $tableInfo['columns']);

        $root = $this->createTemp()->getTmpFolder();
        file_put_contents(
            $root . '/upload/' . $this->emptyOutputBucketId . '.table10.csv',
            "\"foo\",\"bar\"\n\"baz\",\"bat\"\n",
        );
        $writer = new TableWriter($this->getLocalStagingFactory(stagingPath: $root));
        $configuration = [
            'mapping' => [
                [
                    'source' => $this->emptyOutputBucketId . '.table10.csv',
                    'destination' => $this->emptyOutputBucketId . '.table10',
                    'columns' => ['Boing', 'Tschak'],
                ],
            ],
        ];
        $tableQueue = $writer->uploadTables(
            'upload',
            $configuration,
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessageMatches(
            '/Some columns are missing in the csv file. Missing columns: id,name./i',
        );
        $tableQueue->waitForAll();
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteMultipleErrors(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/' . $this->emptyOutputBucketId . '.table10a.csv',
            "\"id\",\"name\"\n\"test\",\"test\"\n",
        );
        file_put_contents(
            $root . '/upload/' . $this->emptyOutputBucketId . '.table10b.csv',
            "\"foo\",\"bar\"\n\"baz\",\"bat\"\n",
        );
        $writer = new TableWriter($this->getLocalStagingFactory());
        $configuration = [
            'mapping' => [
                [
                    'source' => $this->emptyOutputBucketId . '.table10a.csv',
                    'destination' => $this->emptyOutputBucketId . '.table10a',
                ],
                [
                    'source' => $this->emptyOutputBucketId . '.table10b.csv',
                    'destination' => $this->emptyOutputBucketId . '.table10b',
                ],
            ],
        ];

        $tableQueue =  $writer->uploadTables(
            'upload',
            $configuration,
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        $tableQueue->waitForAll();

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(2, $tables);
        $tableInfo = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId . '.table10a',
        );
        self::assertEquals(['id', 'name'], $tableInfo['columns']);
        $tableInfo = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId . '.table10b',
        );
        self::assertEquals(['foo', 'bar'], $tableInfo['columns']);

        file_put_contents(
            $root . '/upload/' . $this->emptyOutputBucketId . '.table10a.csv.manifest',
            json_encode([
                'columns' => ['Boing', 'Tschak'],
            ]),
        );
        file_put_contents(
            $root . '/upload/' . $this->emptyOutputBucketId . '.table10b.csv.manifest',
            json_encode([
                'columns' => ['bum', 'tschak'],
            ]),
        );

        $writer = new TableWriter($this->getLocalStagingFactory());
        $configuration = [
            'mapping' => [
                [
                    'source' => $this->emptyOutputBucketId . '.table10a.csv',
                    'destination' => $this->emptyOutputBucketId . '.table10a',
                ],
                [
                    'source' => $this->emptyOutputBucketId . '.table10b.csv',
                    'destination' => $this->emptyOutputBucketId . '.table10b',
                ],
            ],
        ];
        $tableQueue = $writer->uploadTables(
            'upload',
            $configuration,
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        try {
            $tableQueue->waitForAll();
            self::fail('Must raise exception');
        } catch (InvalidOutputException $e) {
            self::assertStringContainsString(
                'Failed to load table "' . $this->emptyOutputBucketId . '.table10a": Some columns are ' .
                'missing in the csv file. Missing columns: id,name. Expected columns: id,name,Boing,Tschak. ' .
                'Please check if the expected delimiter "," is used in the csv file.',
                $e->getMessage(),
            );
            self::assertStringContainsString(
                'Failed to load table "' . $this->emptyOutputBucketId . '.table10b": Some columns are ' .
                'missing in the csv file. Missing columns: foo,bar. Expected columns: foo,bar,bum,tschak. ' .
                'Please check if the expected delimiter "," is used in the csv file.',
                $e->getMessage(),
            );
        }
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableExistingBucketDevModeNoDev(): void
    {
        $root = $this->temp->getTmpFolder();
        $this->clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN_MASTER'),
            ),
        );

        $branchId = $this->createBranch($this->clientWrapper, self::BRANCH_NAME);
        $this->clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN_MASTER'),
                $branchId,
            ),
        );

        file_put_contents(
            $root . '/upload/table21.csv',
            "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n",
        );

        $configs = [
            [
                'source' => 'table21.csv',
                'destination' => $this->emptyOutputBucketId . '.table21',
            ],
        ];

        $writer = new TableWriter($this->getLocalStagingFactory());
        $tableQueue =  $writer->uploadTables(
            '/upload',
            ['mapping' => $configs],
            ['componentId' => 'foo', 'branchId' => $branchId],
            AbstractStrategyFactory::LOCAL,
            false,
            'none',
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);
        $jobDetail = $this->clientWrapper->getTableAndFileStorageClient()->getJob($jobIds[0]);
        $tableId = $this->getTableIdFromJobDetail($jobDetail);
        $tableParts = explode('.', $tableId);
        array_pop($tableParts);
        $branchBucketId = implode('.', $tableParts);

        // drop the dev branch metadata
        $metadata = new Metadata($this->clientWrapper->getTableAndFileStorageClient());
        foreach ($metadata->listBucketMetadata($branchBucketId) as $metadatum) {
            if (($metadatum['key'] === 'KBC.createdBy.branch.id')
                || ($metadatum['key'] === 'KBC.lastUpdatedBy.branch.id')
            ) {
                $metadata->deleteBucketMetadata($branchBucketId, $metadatum['id']);
            }
        }
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage(sprintf(
            'Trying to create a table in the development bucket ' .
            '"' . $branchBucketId . '" on branch "' . self::BRANCH_NAME .
            '" (ID "%s"), but the bucket is not assigned ' .
            'to any development branch.',
            $branchId,
        ));
        $writer->uploadTables(
            '/upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::LOCAL,
            false,
            'none',
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableExistingBucketDevModeDifferentDev(): void
    {
        $root = $this->temp->getTmpFolder();
        $this->clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN_MASTER'),
            ),
        );

        $branchId = $this->createBranch($this->clientWrapper, self::BRANCH_NAME);
        $this->clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN_MASTER'),
                $branchId,
            ),
        );

        file_put_contents(
            $root . '/upload/table21.csv',
            "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n",
        );

        $configs = [
            [
                'source' => 'table21.csv',
                'destination' => $this->emptyOutputBucketId . '.table21',
            ],
        ];

        $writer = new TableWriter($this->getLocalStagingFactory());
        $tableQueue =  $writer->uploadTables(
            '/upload',
            ['mapping' => $configs],
            ['componentId' => 'foo', 'branchId' => $branchId],
            AbstractStrategyFactory::LOCAL,
            false,
            'none',
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);
        $jobDetail = $this->clientWrapper->getBranchClient()->getJob($jobIds[0]);
        $tableId = $this->getTableIdFromJobDetail($jobDetail);
        $tableParts = explode('.', $tableId);
        array_pop($tableParts);
        $branchBucketId = implode('.', $tableParts);

        // drop the dev branch metadata and create bucket metadata referencing a different branch
        $metadata = new Metadata($this->clientWrapper->getTableAndFileStorageClient());
        foreach ($metadata->listBucketMetadata($branchBucketId) as $metadatum) {
            if (($metadatum['key'] === 'KBC.createdBy.branch.id') ||
                ($metadatum['key'] === 'KBC.lastUpdatedBy.branch.id')
            ) {
                $metadata->deleteBucketMetadata($branchBucketId, $metadatum['id']);
                $metadata->postBucketMetadata(
                    $branchBucketId,
                    'system',
                    [
                        [
                            'key' => $metadatum['key'],
                            'value' => '12345',
                        ],
                    ],
                );
            }
        }

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage(sprintf(
            'Trying to create a table in the development bucket ' .
            '"' . $branchBucketId . '" on branch "' . self::BRANCH_NAME . '" (ID "%s"). ' .
            'The bucket metadata marks it as assigned to branch with ID "12345".',
            $branchId,
        ));
        $writer->uploadTables(
            '/upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::LOCAL,
            false,
            'none',
        );
    }

    /**
     * @dataProvider provideAllowedDestinationConfigurations
     */
    #[NeedsEmptyOutputBucket]
    public function testAllowedDestinationConfigurations(
        ?array $manifestTemplate,
        ?string $defaultBucketTemplate,
        ?array $mappingTemplate,
        array $expectedTablesTemplate,
        bool $isFailedJob = false,
    ): void {
        $manifest = $manifestTemplate;
        if (is_array($manifest)) {
            array_walk_recursive($manifest, function (&$value) {
                $value = sprintf($value, $this->emptyOutputBucketId);
            });
        }
        $defaultBucket = is_string($defaultBucketTemplate) ?
            sprintf($defaultBucketTemplate, $this->emptyOutputBucketId) :
            $defaultBucketTemplate;
        $mapping = $mappingTemplate;
        if (is_array($mapping)) {
            array_walk_recursive($mapping, function (&$value) {
                $value = is_string($value) ? sprintf($value, $this->emptyOutputBucketId) : $value;
            });
        }
        $expectedTables = $expectedTablesTemplate;
        array_walk($expectedTables, function (&$value) {
            $value = sprintf($value, $this->emptyOutputBucketId);
        });

        $root = $this->temp->getTmpFolder() . '/upload/';

        file_put_contents($root . 'table.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n");
        if ($manifest !== null) {
            file_put_contents($root . 'table.csv.manifest', json_encode($manifest));
        }

        $tableWriter = new TableWriter($this->getLocalStagingFactory());

        $queue = $tableWriter->uploadTables(
            'upload',
            ['bucket' => $defaultBucket, 'mapping' => $mapping],
            ['componentId' => 'foo'],
            'local',
            $isFailedJob,
            'none',
        );
        $queue->waitForAll();

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        $tablesIds = array_map(function (array $table) {
            return $table['id'];
        }, $tables);
        self::assertSame($expectedTables, $tablesIds);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteAlwaysFlag(): void
    {
        $root = $this->temp->getTmpFolder() . '/upload/';
        file_put_contents($root . 'table0.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $this->testAllowedDestinationConfigurations(
            null,
            null,
            [

                [
                    'source' => 'table0.csv',
                    'destination' => '%s.table1',
                    'write_always' => false,
                ],

                [
                    'source' => 'table.csv',
                    'destination' => '%s.table2',
                    'write_always' => true,
                ],
            ],
            ['%s.table2'],
            true,
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testAllowedMultipleMappingsOfSameSource(): void
    {
        if ($this->clientWrapper->getToken()->hasFeature(TableWriter::OUTPUT_MAPPING_SLICE_FEATURE)) {
            self::markTestSkipped('This test is not relevant for slice feature.');
        }

        $this->testAllowedDestinationConfigurations(
            null,
            null,
            [
                ['source' => 'table.csv', 'destination' => '%s.table1'],
                ['source' => 'table.csv', 'destination' => '%s.table2'],
            ],
            ['%s.table1', '%s.table2'],
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testForbiddenMultipleMappingsOfSameSource(): void
    {
        if (!$this->clientWrapper->getToken()->hasFeature(TableWriter::OUTPUT_MAPPING_SLICE_FEATURE)) {
            self::markTestSkipped('This test is relevant only for slice feature.');
        }

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Source "table.csv" has multiple destinations set.');

        $this->testAllowedDestinationConfigurations(
            null,
            null,
            [
                ['source' => 'table.csv', 'destination' => '%s.table1'],
                ['source' => 'table.csv', 'destination' => '%s.table2'],
            ],
            ['%s.table1', '%s.table2'],
        );
    }

    public function provideAllowedDestinationConfigurations(): array
    {
        return [
            'table ID in mapping' => [
                'manifest' => null,
                'defaultBucket' => null,
                'mapping' => [
                    ['source' => 'table.csv', 'destination' => '%s.tableA'],
                ],
                'expectedTables' => ['%s.tableA'],
            ],

            'table ID in manifest' => [
                'manifest' => ['destination' => '%s.tableA'],
                'defaultBucket' => null,
                'mapping' => null,
                'expectedTables' => ['%s.tableA'],
            ],

            'table name in manifest with bucket' => [
                'manifest' => ['destination' => 'tableA'],
                'defaultBucket' => '%s',
                'mapping' => null,
                'expectedTables' => ['%s.tableA'],
            ],

            'no destination in manifest with bucket' => [
                'manifest' => ['columns' => ['Id', 'Name']],
                'defaultBucket' => '%s',
                'mapping' => null,
                'expectedTables' => ['%s.table'],
            ],

            'table ID in mapping overrides manifest' => [
                'manifest' => ['destination' => '%s.tableA'],
                'defaultBucket' => null,
                'mapping' => [
                    ['source' => 'table.csv', 'destination' => '%s.table1'],
                ],
                'expectedTables' => ['%s.table1'],
            ],
        ];
    }

    /**
     * @dataProvider provideForbiddenDestinationConfigurations
     */
    #[NeedsEmptyOutputBucket]
    public function testForbiddenDestinationConfigurations(
        ?array $manifest,
        ?string $defaultBucket,
        ?array $mapping,
        string $expectedError,
    ): void {
        $defaultBucket = is_string($defaultBucket) ? sprintf($defaultBucket, $this->emptyOutputBucketId) : null;
        $root = $this->temp->getTmpFolder() . '/upload/';

        file_put_contents($root . 'table.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n");
        if ($manifest !== null) {
            file_put_contents($root . 'table.csv.manifest', json_encode($manifest));
        }

        $tableWriter = new TableWriter($this->getLocalStagingFactory());

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage($expectedError);

        $tableQueue = $tableWriter->uploadTables(
            'upload',
            ['bucket' => $defaultBucket, 'mapping' => $mapping],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        $tableQueue->waitForAll();
    }

    public function provideForbiddenDestinationConfigurations(): array
    {
        return [
            'no destination in manifest without bucket' => [
                'manifest' => ['columns' => ['Id', 'Name']],
                'defaultBucket' => null,
                'mapping' => null,
                'expectedError' => 'Failed to resolve destination for output table "table.csv".',
            ],

            'table name in mapping is not accepted' => [
                'manifest' => null,
                'defaultBucket' => null,
                'mapping' => [
                    ['source' => 'table.csv', 'destination' => 'table'],
                ],
                'expectedError' => 'Failed to resolve destination for output table "table.csv".',
            ],

            'table name in mapping does not combine with default bucket' => [
                'manifest' => null,
                'defaultBucket' => '%s',
                'mapping' => [
                    ['source' => 'table.csv', 'destination' => 'table'],
                ],
                'expectedError' => 'Failed to resolve destination for output table "table.csv".',
            ],

            'table name in manifest without bucket' => [
                'manifest' => ['destination' => 'table'],
                'defaultBucket' => null,
                'mapping' => null,
                'expectedError' => 'Failed to resolve destination for output table "table.csv".',
            ],
        ];
    }

    /**
     * @dataProvider provideWriteTableBareAllowedVariants
     */
    #[NeedsEmptyOutputBucket]
    public function testWriteTableBareAllowedVariants(
        string $fileName,
        ?string $defaultBucket,
        string $expectedTableName,
    ): void {
        $expectedTableName = sprintf($expectedTableName, $this->emptyOutputBucketId);
        $fileName = sprintf($fileName, $this->emptyOutputBucketId);
        $defaultBucket = is_string($defaultBucket) ?
            sprintf($defaultBucket, $this->emptyOutputBucketId) :
            $defaultBucket;

        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/' . $fileName, "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $writer = new TableWriter($this->getLocalStagingFactory(null, 'json', $this->testLogger));

        $tableQueue =  $writer->uploadTables(
            '/upload',
            ['bucket' => $defaultBucket],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);

        self::assertEquals($expectedTableName, $tables[0]['id']);
        $tableInfo = $this->clientWrapper->getTableAndFileStorageClient()->getTable($expectedTableName);
        self::assertEquals(['Id', 'Name'], $tableInfo['columns']);
    }

    public function provideWriteTableBareAllowedVariants(): array
    {
        return [
            'table ID as filename without default bucket' => [
                'filename' => '%s.table41.csv',
                'defaultBucket' => null,
                'expectedTableName' => '%s.table41',
            ],

            'table ID as filename with default bucket' => [
                'filename' => '%s.table42.csv',
                'defaultBucket' => 'out.c-bucket-is-not-used',
                'expectedTableName' => '%s.table42',
            ],

            'table name as filename with default bucket' => [
                'filename' => 'table43.csv',
                'defaultBucket' => '%s',
                'expectedTableName' => '%s.table43',
            ],
        ];
    }

    /**
     * @dataProvider provideWriteTableBareForbiddenVariants
     */
    public function testWriteTableBareForbiddenVariants(
        string $fileName,
        ?string $defaultBucket,
        string $expectedError,
    ): void {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/' . $fileName, "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $writer = new TableWriter($this->getLocalStagingFactory(null, 'json', $this->testLogger));

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage($expectedError);

        $writer->uploadTables(
            '/upload',
            ['bucket' => $defaultBucket],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
    }

    public function provideWriteTableBareForbiddenVariants(): array
    {
        return [
            'table name as filename without default bucket' => [
                'filename' => 'table51.csv',
                'bucketName' => null,
                'expectedError' => 'Failed to resolve destination for output table "table51.csv".',
            ],
        ];
    }

    public function testLocalTableUploadRequiresComponentId(): void
    {
        $tableWriter = new TableWriter($this->getLocalStagingFactory());

        $this->expectException(OutputOperationException::class);
        $this->expectExceptionMessage('Component Id must be set');

        $tableWriter->uploadTables(
            'upload',
            [],
            [],
            'local',
            false,
            'none',
        );
    }

    public function testLocalTableUploadChecksForOrphanedManifests(): void
    {
        $root = $this->temp->getTmpFolder() . '/upload/';
        file_put_contents($root . 'table.csv.manifest', json_encode([]));

        $tableWriter = new TableWriter($this->getLocalStagingFactory());

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Found orphaned table manifest: "table.csv.manifest"');

        $tableWriter->uploadTables(
            'upload',
            [],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
    }

    public function testLocalTableUploadChecksForUnusedMappingEntries(): void
    {
        $tableWriter = new TableWriter($this->getLocalStagingFactory());

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Table sources not found: "unknown.csv"');

        $tableWriter->uploadTables(
            'upload',
            [
                'mapping' => [
                    [
                        'source' => 'unknown.csv',
                        'destination' => 'unknown',
                    ],
                ],
            ],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testLocalTableUploadChecksForWriteAlwaysMappingEntries(): void
    {
        $tableWriter = new TableWriter($this->getLocalStagingFactory());
        $root = $this->temp->getTmpFolder() . '/upload/';
        file_put_contents($root . 'write-always.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $queue = $tableWriter->uploadTables(
            'upload',
            [
                'mapping' => [
                    [
                        'source' => 'never-created.csv',
                        'destination' => $this->emptyOutputBucketId . '.never-created',
                    ], [
                        'source' => 'write-always.csv',
                        'destination' => $this->emptyOutputBucketId . '.write-always',
                        'write_always' => true,
                    ],
                ],
            ],
            ['componentId' => 'foo'],
            'local',
            true,
            'none',
        );
        $queue->waitForAll();
        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);
        self::assertEquals($this->emptyOutputBucketId . '.write-always', $tables[0]['id']);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteAlwaysWhenMissingMappingEntries(): void
    {
        $tableWriter = new TableWriter($this->getLocalStagingFactory());
        $root = $this->temp->getTmpFolder() . '/upload/';
        file_put_contents($root . 'write-always-2.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n");
        file_put_contents($root . 'something-unexpected.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $queue = $tableWriter->uploadTables(
            'upload',
            [
                'mapping' => [
                    [
                        'source' => 'never-created.csv',
                        'destination' => $this->emptyOutputBucketId . '.never-created',
                    ], [
                        'source' => 'write-always-2.csv',
                        'destination' => $this->emptyOutputBucketId . '.write-always-2',
                        'write_always' => true,
                    ],
                ],
            ],
            ['componentId' => 'foo'],
            'local',
            true,
            'none',
        );
        $queue->waitForAll();
        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);
        self::assertEquals($this->emptyOutputBucketId . '.write-always-2', $tables[0]['id']);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingWithPkUpdate(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/table14.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n");
        $writer = new TableWriter($this->getLocalStagingFactory());
        $tableQueue = $writer->uploadTables(
            'upload',
            [
                'mapping' => [
                    [
                        'source' => 'table14.csv',
                        'destination' => $this->emptyOutputBucketId . '.table14',
                        'primary_key' => ['Id'],
                    ],
                ],
            ],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $tableInfo = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId . '.table14',
        );
        $this->assertEquals(['Id'], $tableInfo['primaryKey']);

        $tableQueue = $writer->uploadTables(
            'upload',
            [
                'mapping' => [
                    [
                        'source' => 'table14.csv',
                        'destination' => $this->emptyOutputBucketId . '.table14',
                        'primary_key' => ['Id', 'Name'],
                    ],
                ],
            ],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );

        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $tableInfo = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId . '.table14',
        );
        $this->assertEquals(['Id', 'Name'], $tableInfo['primaryKey']);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingWithPkHavingWhitespaceUpdate(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/table13.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n");
        $writer = new TableWriter($this->getLocalStagingFactory());
        $tableQueue = $writer->uploadTables(
            'upload',
            [
                'mapping' => [
                    [
                        'source' => 'table13.csv',
                        'destination' => $this->emptyOutputBucketId . '.table13',
                        'primary_key' => ['Id '],
                    ],
                ],
            ],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $tableInfo = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId . '.table13',
        );
        $this->assertEquals(['Id'], $tableInfo['primaryKey']);

        $tableQueue = $writer->uploadTables(
            'upload',
            [
                'mapping' => [
                    [
                        'source' => 'table13.csv',
                        'destination' => $this->emptyOutputBucketId . '.table13',
                        'primary_key' => ['Id ', 'Name '],
                    ],
                ],
            ],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $tableInfo = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId . '.table13',
        );
        $this->assertEquals(['Id', 'Name'], $tableInfo['primaryKey']);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableFailedUploadDelete(): void
    {
        $tableId = $this->emptyOutputBucketId . '.table_failed_upload';
        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/table_failed_upload.csv',
            "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\",\"dddd\"\n",
        );

        $configs = [
            [
                'source' => 'table_failed_upload.csv',
                'destination' => $tableId,
            ],
        ];

        $writer = new TableWriter($this->getLocalStagingFactory(logger: $this->testLogger));

        $tableQueue =  $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        try {
            $jobIds = $tableQueue->waitForAll();
            self::fail('Must throw exception');
        } catch (InvalidOutputException $e) {
            self::assertStringContainsString(
                // phpcs:ignore Generic.Files.LineLength.MaxExceeded
                'An exception occurred while executing a query: Field delimiter \',\' found while expecting record delimiter',
                $e->getMessage(),
            );
        }
        self::assertFalse($this->clientWrapper->getTableAndFileStorageClient()->tableExists($tableId));
        self::assertTrue(
            $this->testHandler->hasWarningThatContains(sprintf('Failed to load table "%s". Dropping table.', $tableId)),
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableFailedUploadNoDelete(): void
    {
        $tableId = $this->emptyOutputBucketId . '.table_failed_upload';
        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/table_failed_upload.csv',
            "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\",\"dddd\"\n",
        );

        $configs = [
            [
                'source' => 'table_failed_upload.csv',
                'destination' => $tableId,
            ],
        ];

        $csv = new CsvFile($root . '/table_header.csv');
        $csv->writeRow(['Id', 'Name']);
        $this->clientWrapper->getTableAndFileStorageClient()->createTableAsync(
            $this->emptyOutputBucketId,
            'table_failed_upload',
            $csv,
        );

        $writer = new TableWriter($this->getLocalStagingFactory(logger: $this->testLogger));

        $tableQueue =  $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        try {
            $jobIds = $tableQueue->waitForAll();
            self::fail('Must throw exception');
        } catch (InvalidOutputException $e) {
            self::assertStringContainsString(
                // phpcs:ignore Generic.Files.LineLength.MaxExceeded
                'An exception occurred while executing a query: Field delimiter \',\' found while expecting record delimiter',
                $e->getMessage(),
            );
        }
        self::assertTrue($this->clientWrapper->getTableAndFileStorageClient()->tableExists($tableId));
        self::assertFalse(
            $this->testHandler->hasWarningThatContains(sprintf('Failed to load table "%s". Dropping table.', $tableId)),
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingWithTimestampColumnsThrowsError(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/table1a.csv.manifest',
            json_encode([
                'columns' => [
                    'Id',
                    'Name',
                    '_timestamp',
                ],
            ]),
        );
        file_put_contents(
            $root . '/upload/table1a.csv',
            "\"Id\",\"Name\",\"_timestamp\"\n\"test\",\"test\",\"test\"\n\"aabb\",\"ccdd\",\"eeff\"\n",
        );

        $table1Mapping = [
            'source' => 'table1a.csv',
            'destination' => $this->emptyOutputBucketId . '.table1a',
        ];

        $writer = new TableWriter($this->getLocalStagingFactory());

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Failed to process mapping for table table1a.csv: '
            . 'System columns "_timestamp" cannot be imported to the table.');
        $this->expectExceptionCode(0);

        $writer->uploadTables(
            'upload',
            ['mapping' => [$table1Mapping]],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testSlicingTableOutputMapping(): void
    {
        $root = $this->temp->getTmpFolder();
        for ($rows= 0; $rows < 2000000; $rows++) {
            file_put_contents(
                $root . '/upload/table1a.csv',
                "longlonglongrow{$rows}, abcdefghijklnoppqrstuvwxyz\n",
                FILE_APPEND,
            );
        }

        $configs = [
            [
                'source' => 'table1a.csv',
                'destination' => $this->emptyOutputBucketId . '.table1a',
            ],
        ];

        $writer = new TableWriter($this->getLocalStagingFactory(logger: $this->testLogger));

        $tableQueue =  $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);
        $tableIds = [];
        sort($tableIds);
        self::assertEquals($this->emptyOutputBucketId . '.table1a', $tables[0]['id']);
        self::assertNotEmpty($jobIds[0]);

        $job = $this->clientWrapper->getBranchClient()->getJob($jobIds[0]);
        $fileId = $job['operationParams']['source']['fileId'];
        $file = $this->clientWrapper->getTableAndFileStorageClient()->getFile($fileId);
        self::assertEquals([], $file['tags']);

        /** @var TableInfo[] $tables */
        $tables = iterator_to_array($tableQueue->getTableResult()->getTables());
        self::assertCount(1, $tables);

        $hasSlicingTableMessage = $this->testHandler->hasInfo('Slicing table "table1a.csv".');
        $hasSlicedTableMessage = $this->testHandler->hasInfoThatContains(
            'Table "table1a.csv" sliced: in/out: 1 / 1 slices',
        );
        if ($this->clientWrapper->getToken()->hasFeature(OutputMappingSettings::OUTPUT_MAPPING_SLICE_FEATURE)) {
            self::assertTrue($hasSlicingTableMessage);
            self::assertTrue($hasSlicedTableMessage);
        } else {
            self::assertFalse($hasSlicingTableMessage);
            self::assertFalse($hasSlicedTableMessage);
        }
    }

    #[NeedsTestTables(1)]
    public function testReorderingColumns(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/test1.csv',
            "\"newName 1\",\"ID1\",\"new bar 1\",\"new foo 1\"\n" .
            "\"newName 2\",\"ID2\",\"new bar 2\",\"new foo 2\"\n",
        );

        file_put_contents(
            $root . DIRECTORY_SEPARATOR . 'upload/test1.csv.manifest',
            json_encode([
                'destination' => $this->testBucketId . '.test1',
                'primary_key' => ['Id'],
                'columns' => ['Name', 'Id', 'bar', 'foo'],
                'incremental' => true,
            ]),
        );

        $writer = new TableWriter($this->getLocalStagingFactory());
        $tableQueue =  $writer->uploadTables(
            'upload',
            [],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $exporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $downloadedFile = $root . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->testBucketId . '.test1', $downloadedFile, []);

        $expectedData = <<<CSV
"Id","Name","foo","bar"
"id1","name1","foo1","bar1"
"id2","name2","foo2","bar2"
"id3","name3","foo3","bar3"
"ID1","newName 1","new foo 1","new bar 1"
"ID2","newName 2","new foo 2","new bar 2"

CSV;

        self::assertEquals($expectedData, (string) file_get_contents($downloadedFile));
    }

    private function getTableIdFromJobDetail(array $jobData): string
    {
        $operationName = $jobData['operationName'];
        if ($operationName === 'tableCreate') {
            return  $jobData['results']['id'];
        }
        if ($operationName === 'tableImport') {
            return  $jobData['tableId'];
        }

        self::fail(sprintf('Cannot detect tableId from %s job', $operationName));
    }
}
