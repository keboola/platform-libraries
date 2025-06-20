<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Table\Result\TableInfo;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\OutputMappingSettings;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsDevBranch;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\OutputMapping\Tests\Needs\NeedsTestTables;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\TableExporter;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\StorageApiToken;

class StorageApiLocalTableWriterTest extends AbstractTestCase
{
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

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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

        /** @var array{
         *     operationParams: array{source: array{fileId: string}},
         * } $job
         */
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
            ->hasFeature(OutputMappingSettings::OUTPUT_MAPPING_SLICE_FEATURE)
        ;

        $token = $this->createMock(StorageApiToken::class);
        $token
            ->method('hasFeature')
            ->willReturnCallback(function (string $feature) use ($tokenHasOutputMappingSliceFeature): bool {
                if ($feature === OutputMappingSettings::OUTPUT_MAPPING_SLICE_FEATURE) {
                    return $tokenHasOutputMappingSliceFeature;
                }

                return $feature === 'tag-staging-files';
            })
        ;
        $token->method('getProjectBackend')->willReturn($this->clientWrapper->getToken()->getProjectBackend());

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getToken')->willReturn($token);
        $clientWrapper->method('getBranchClient')->willReturn(
            $this->clientWrapper->getBranchClient(),
        );
        $clientWrapper->method('getTableAndFileStorageClient')->willReturn(
            $this->clientWrapper->getBranchClient(),
        );

        /** @var ClientWrapper $clientWrapper */
        $stagingFactory = $this->getLocalStagingFactory(clientWrapper: $clientWrapper);
        $tableQueue = $this->getTableLoader(
            clientWrapper: $clientWrapper,
            strategyFactory: $stagingFactory,
        )->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: 'upload',
                storageApiToken: $clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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

        /** @var array{
         *     operationParams: array{source: array{fileId: string}},
         * } $job
         */
        $job = $this->clientWrapper->getBranchClient()->getJob($jobIds[0]);
        $fileId = $job['operationParams']['source']['fileId'];
        $file = $this->clientWrapper->getTableAndFileStorageClient()->getFile($fileId);
        self::assertEquals(
            ['componentId: foo'],
            $file['tags'],
        );
    }

    #[NeedsEmptyOutputBucket]
    #[NeedsDevBranch]
    public function testWriteTableOutputMappingFakeDevMode(): void
    {
        $this->initClient($this->devBranchId);

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

        $stagingFactory = $this->getLocalStagingFactory();
        $tableQueue = $this->getTableLoader(
            strategyFactory: $stagingFactory,
        )->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo', 'branchId' => $this->devBranchId]),
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

        $fakeDevEmptyOutputBucketId = str_replace(
            'out.c-',
            'out.(c-)?' . $this->devBranchId . '-',
            $this->emptyOutputBucketId,
        );

        self::assertMatchesRegularExpression('#' . $fakeDevEmptyOutputBucketId . '.table11a#', $tableIds[0]);
        self::assertMatchesRegularExpression('#' . $fakeDevEmptyOutputBucketId . '.table21a#', $tableIds[1]);
    }

    #[NeedsEmptyOutputBucket]
    #[NeedsDevBranch]
    public function testWriteTableOutputMappingRealDevMode(): void
    {
        $clientOptions = $this->clientWrapper->getClientOptionsReadOnly()
            ->setBranchId($this->devBranchId)
            ->setUseBranchStorage(true) // this is the important part
        ;
        $this->clientWrapper = new ClientWrapper($clientOptions);

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
        $stagingFactory = $this->getLocalStagingFactory();
        $tableQueue = $this->getTableLoader(
            strategyFactory: $stagingFactory,
        )->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo', 'branchId' => $this->devBranchId]),
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
        self::assertSame($this->emptyOutputBucketId . '.table11a', $tableIds[0]);
        self::assertSame($this->emptyOutputBucketId . '.table21a', $tableIds[1]);
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

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: '/upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        // And again
        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: '/upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: '/upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: '/upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: '/upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Invalid type for path');
        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [],
                sourcePathPrefix: '/upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );

        $tableQueue->waitForAll();
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

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [],
                sourcePathPrefix: '/upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Found orphaned table manifest: "table.csv.manifest"');
        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [],
                sourcePathPrefix: '/upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
        $tableQueue->waitForAll();
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
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Table sources not found: "table81.csv"');
        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: '/upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
        $tableQueue->waitForAll();
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableMetadataMissing(): void
    {
        $this->expectException(OutputOperationException::class);
        $this->expectExceptionMessage('Component Id must be set');
        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [],
                sourcePathPrefix: '/upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata([]), // prázdná metadata
        );

        $tableQueue->waitForAll();
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

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: '/upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);
        $this->clientWrapper->getBranchClient()->handleAsyncTasks($jobIds);

        // And again, check first incremental table
        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: '/upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['bucket' => $this->emptyOutputBucketId],
                sourcePathPrefix: '/upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['bucket' => $this->emptyOutputBucketId],
                sourcePathPrefix: '/upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [
                    'mapping' => [
                        [
                            'source' => 'table16.csv',
                            'destination' => $this->emptyOutputBucketId . '.table16',
                            'primary_key' => ['Id'],
                        ],
                    ],
                ],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [
                    'mapping' => [
                        [
                            'source' => 'table15.csv',
                            'destination' => $this->emptyOutputBucketId . '.table15',
                            'primary_key' => ['Id'],
                        ],
                    ],
                ],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
        $tableQueue->waitForAll();

        $tableQueue = $this->getTableLoader(
            logger: $this->testLogger,
            strategyFactory: $this->getLocalStagingFactory(
                logger: $this->testLogger,
            ),
        )->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [
                    'mapping' => [
                        [
                            'source' => 'table15.csv',
                            'destination' => $this->emptyOutputBucketId . '.table15',
                            'primary_key' => ['Id'],
                        ],
                    ],
                ],
                sourcePathPrefix: '/upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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

        $configs = [
            [
                'source' => 'table12.csv',
                'destination' => $this->emptyOutputBucketId . '.table12',
                'primary_key' => [],
            ],
        ];

        $stagingFactory = $this->getLocalStagingFactory(logger: $this->testLogger);
        $tableQueue = $this->getTableLoader(
            logger: $this->testLogger,
            strategyFactory: $stagingFactory,
        )->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
        $tableQueue->waitForAll();
        $tableInfo = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId . '.table12',
        );
        self::assertEquals([], $tableInfo['primaryKey']);

        $tableQueue = $this->getTableLoader(
            logger: $this->testLogger,
            strategyFactory: $this->getLocalStagingFactory(
                logger: $this->testLogger,
            ),
        )->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [
                    'mapping' => [
                        [
                            'source' => 'table12.csv',
                            'destination' => $this->emptyOutputBucketId . '.table12',
                            'primary_key' => [''],
                        ],
                    ],
                ],
                sourcePathPrefix: '/upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [
                    'mapping' => [
                        [
                            'source' => 'table11.csv',
                            'destination' => $this->emptyOutputBucketId . '.table11',
                            'primary_key' => [],
                        ],
                    ],
                ],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $root = $this->createTemp()->getTmpFolder();
        file_put_contents($root . '/upload/table11.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        file_put_contents(
            $root . '/upload/table11.csv.manifest',
            '{"destination": "' . $this->emptyOutputBucketId . '.table11","primary_key": [""]}',
        );

        $tableQueue = $this->getTableLoader(
            logger: $this->testLogger,
            strategyFactory:  $this->getLocalStagingFactory(
                logger: $this->testLogger,
                stagingPath: $root,
            ),
        )->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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
        if ($this->clientWrapper->getToken()->hasFeature(OutputMappingSettings::OUTPUT_MAPPING_SLICE_FEATURE)) {
            $this->expectException(InvalidOutputException::class);
            $this->expectExceptionMessage(
                'Params "delimiter", "enclosure" or "columns" ' .
                'specified in mapping are not longer supported.',
            );
        }

        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/' . $this->emptyOutputBucketId . '.table10.csv',
            "\"Id\",\"Name\"\n\"test\",\"test\"\n",
        );

        $tableQueue = $this->getTableLoader(
            strategyFactory: $this->getLocalStagingFactory(
                stagingPath: $root,
            ),
        )->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [
                    'mapping' => [
                        [
                            'source' => $this->emptyOutputBucketId . '.table10.csv',
                            'destination' => $this->emptyOutputBucketId . '.table10',
                        ],
                    ],
                ],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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

        $tableQueue = $this->getTableLoader(
            strategyFactory: $this->getLocalStagingFactory(
                stagingPath: $root,
            ),
        )->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [
                    'mapping' => [
                        [
                            'source' => $this->emptyOutputBucketId . '.table10.csv',
                            'destination' => $this->emptyOutputBucketId . '.table10',
                            'columns' => ['Boing', 'Tschak'],
                        ],
                    ],
                ],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: $configuration,
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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
        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: $configuration,
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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
    #[NeedsDevBranch]
    public function testWriteTableExistingBucketDevModeNoDev(): void
    {
        $this->initClient($this->devBranchId);

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

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo', 'branchId' => $this->devBranchId]),
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
            '"' . $branchBucketId . '" on branch "' . $this->devBranchName .
            '" (ID "%s"), but the bucket is not assigned ' .
            'to any development branch.',
            $this->devBranchId,
        ));
        $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: '/upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
    }

    #[NeedsEmptyOutputBucket]
    #[NeedsDevBranch]
    public function testWriteTableExistingBucketDevModeDifferentDev(): void
    {
        $this->initClient($this->devBranchId);

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

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo', 'branchId' => $this->devBranchId]),
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
            '"' . $branchBucketId . '" on branch "' . $this->devBranchName . '" (ID "%s"). ' .
            'The bucket metadata marks it as assigned to branch with ID "12345".',
            $this->devBranchId,
        ));

        $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: '/upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableIntColumnMetadataNames(): void
    {
        $root = $this->temp->getTmpFolder();

        file_put_contents(
            $root . '/upload/table21.csv',
            "\"aaa\",\"bbb\"\n",
        );
        file_put_contents(
            $root . '/upload/table21.csv.manifest',
            json_encode(
                [
                    'columns' => ['0', '2'],
                    'column_metadata' => [
                        '0' => [
                            [
                                'key' => 'key1',
                                'value' => 'value1',
                            ],
                        ],
                        '2' => [
                            [
                                'key' => 'key2',
                                'value' => 'value2',
                            ],
                        ],
                    ],
                ],
            ),
        );

        $configs = [
            [
                'source' => 'table21.csv',
                'destination' => $this->emptyOutputBucketId . '.table21',
            ],
        ];

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: '/upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);
        $jobDetail = $this->clientWrapper->getBranchClient()->getJob($jobIds[0]);
        $tableId = $this->getTableIdFromJobDetail($jobDetail);

        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($tableId);
        self::assertEquals(['0', '2'], $table['columns']);

        $columnsMetadata = [];
        foreach ($table['columnMetadata'] as $columnName => $metadata) {
            $columnsMetadata[$columnName] = array_map(function (array $metadata) {
                return [
                    'key' => $metadata['key'],
                    'value' => $metadata['value'],
                ];
            }, $metadata);
        }

        self::assertEquals(
            [
                '0' => [
                    [
                        'key' => 'key1',
                        'value' => 'value1',
                    ],
                ],
                '2' => [
                    [
                        'key' => 'key2',
                        'value' => 'value2',
                    ],
                ],
            ],
            $columnsMetadata,
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

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['bucket' => $defaultBucket, 'mapping' => $mapping],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: $isFailedJob,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );

        $tableQueue->waitForAll();

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
        if ($this->clientWrapper->getToken()->hasFeature(OutputMappingSettings::OUTPUT_MAPPING_SLICE_FEATURE)) {
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
        if (!$this->clientWrapper->getToken()->hasFeature(OutputMappingSettings::OUTPUT_MAPPING_SLICE_FEATURE)) {
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

                $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage($expectedError);
        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['bucket' => $defaultBucket, 'mapping' => $mapping],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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

        $stagingFactory = $this->getLocalStagingFactory(logger: $this->testLogger);
        $tableQueue = $this->getTableLoader(
            logger: $this->testLogger,
            strategyFactory: $stagingFactory,
        )->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['bucket' => $defaultBucket],
                sourcePathPrefix: '/upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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

        $stagingFactory = $this->getLocalStagingFactory(logger: $this->testLogger);
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage($expectedError);
        $this->getTableLoader(
            logger: $this->testLogger,
            strategyFactory: $stagingFactory,
        )->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['bucket' => $defaultBucket],
                sourcePathPrefix: '/upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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
        $this->expectException(OutputOperationException::class);
        $this->expectExceptionMessage('Component Id must be set');
        $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [],
                sourcePathPrefix: '/upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata([]),
        );
    }

    public function testLocalTableUploadChecksForOrphanedManifests(): void
    {
        $root = $this->temp->getTmpFolder() . '/upload/';
        file_put_contents($root . 'table.csv.manifest', json_encode([]));

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Found orphaned table manifest: "table.csv.manifest"');
        $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [],
                sourcePathPrefix: '/upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
    }

    public function testLocalTableUploadChecksForUnusedMappingEntries(): void
    {
        $stagingFactory = $this->getLocalStagingFactory(logger: $this->testLogger);

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Table sources not found: "unknown.csv"');
        $this->getTableLoader(
            logger: $this->testLogger,
            strategyFactory: $stagingFactory,
        )->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [
                    'mapping' => [
                        [
                            'source' => 'unknown.csv',
                            'destination' => 'unknown',
                        ],
                    ],
                ],
                sourcePathPrefix: '/upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testLocalTableUploadChecksForWriteAlwaysMappingEntries(): void
    {
        $root = $this->temp->getTmpFolder() . '/upload/';
        file_put_contents($root . 'write-always.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [
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
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: true,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
        $tableQueue->waitForAll();
        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);
        self::assertEquals($this->emptyOutputBucketId . '.write-always', $tables[0]['id']);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteAlwaysWhenMissingMappingEntries(): void
    {
        $root = $this->temp->getTmpFolder() . '/upload/';
        file_put_contents($root . 'write-always-2.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n");
        file_put_contents($root . 'something-unexpected.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [
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
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: true,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );

        $tableQueue->waitForAll();
        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);
        self::assertEquals($this->emptyOutputBucketId . '.write-always-2', $tables[0]['id']);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingWithPkUpdate(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/table14.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [
                    'mapping' => [
                        [
                            'source' => 'table14.csv',
                            'destination' => $this->emptyOutputBucketId . '.table14',
                            'primary_key' => ['Id'],
                        ],
                    ],
                ],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );

        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $tableInfo = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId . '.table14',
        );
        $this->assertEquals(['Id'], $tableInfo['primaryKey']);

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [
                    'mapping' => [
                        [
                            'source' => 'table14.csv',
                            'destination' => $this->emptyOutputBucketId . '.table14',
                            'primary_key' => ['Id', 'Name'],
                        ],
                    ],
                ],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [
                    'mapping' => [
                        [
                            'source' => 'table13.csv',
                            'destination' => $this->emptyOutputBucketId . '.table13',
                            'primary_key' => ['Id '],
                        ],
                    ],
                ],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $tableInfo = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId . '.table13',
        );
        $this->assertEquals(['Id'], $tableInfo['primaryKey']);

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [
                    'mapping' => [
                        [
                            'source' => 'table13.csv',
                            'destination' => $this->emptyOutputBucketId . '.table13',
                            'primary_key' => ['Id ', 'Name '],
                        ],
                    ],
                ],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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

        $stagingFactory = $this->getLocalStagingFactory(logger: $this->testLogger);
        $tableQueue = $this->getTableLoader(
            logger: $this->testLogger,
            strategyFactory: $stagingFactory,
        )->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );

        try {
            $tableQueue->waitForAll();
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

        $stagingFactory = $this->getLocalStagingFactory(logger: $this->testLogger);
        $tableQueue = $this->getTableLoader(
            strategyFactory: $stagingFactory,
        )->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );

        try {
            $tableQueue->waitForAll();
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

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Failed to process mapping for table table1a.csv: '
            . 'System columns "_timestamp" cannot be imported to the table.');
        $this->expectExceptionCode(0);
        $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => [$table1Mapping]],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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

        $stagingFactory = $this->getLocalStagingFactory(logger: $this->testLogger);
        $tableQueue = $this->getTableLoader(
            logger: $this->testLogger,
            strategyFactory: $stagingFactory,
        )->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );

        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);
        $tableIds = [];
        sort($tableIds);
        self::assertEquals($this->emptyOutputBucketId . '.table1a', $tables[0]['id']);
        self::assertNotEmpty($jobIds[0]);

        /** @var array{
         *     operationParams: array{source: array{fileId: string}},
         * } $job
         */
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

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
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

        self::assertLinesEqualsSorted($expectedData, (string) file_get_contents($downloadedFile));
    }

    #[NeedsEmptyOutputBucket]
    public function testLongColumnName(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/test1.csv',
            'newName 1',
        );

        file_put_contents(
            $root . DIRECTORY_SEPARATOR . 'upload/test1.csv.manifest',
            json_encode([
                'destination' => $this->emptyOutputBucketId . '.test1',
                'primary_key' => [],
                'columns' => ['LongLongLongLongLongLongLongLongLongLongLongLongLongLongLongLongName'],
                'incremental' => true,
            ]),
        );

        try {
            $this->getTableLoader()->uploadTables(
                configuration: new OutputMappingSettings(
                    configuration: [],
                    sourcePathPrefix: 'upload',
                    storageApiToken: $this->clientWrapper->getToken(),
                    isFailedJob: false,
                    dataTypeSupport: 'none',
                ),
                systemMetadata: new SystemMetadata(['componentId' => 'foo']),
            );
            $this->fail('Must throw exception');
        } catch (InvalidOutputException $e) {
            $this->assertStringContainsString(
                '\'LongLongLongLongLongLongLongLongLongLongLongLongLongLongLongLongName\' '.
                'is more than 64 characters long',
                $e->getMessage(),
            );
        }
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingWithHasHeader(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/test1.csv',
            '"fileHeader-Id","fileHeader-Name","fileHeader-foo","fileHeader-bar"' . "\n" .
                '"id1","name1","foo1","bar1"' . "\n" .
                '"id2","name2","foo2","bar2"',
        );

        file_put_contents(
            $root . DIRECTORY_SEPARATOR . 'upload/test1.csv.manifest',
            json_encode([
                'destination' => $this->emptyOutputBucketId . '.test1',
                'primary_key' => ['Id'],
                'columns' => ['Id', 'Name', 'foo', 'bar'],
                'has_header' => true,
            ]),
        );

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );

        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $exporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $downloadedFile = $root . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyOutputBucketId . '.test1', $downloadedFile, []);

        $expectedData = <<<CSV
"Id","Name","foo","bar"
"id1","name1","foo1","bar1"
"id2","name2","foo2","bar2"

CSV;

        self::assertEquals($expectedData, (string) file_get_contents($downloadedFile));
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingTreatEmptyValuesAsNull(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/table1a.csv',
            "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n",
        );
        file_put_contents(
            $root . '/upload/table2a.csv',
            "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n",
        );
        file_put_contents(
            $root . '/upload/table3a.csv',
            "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n",
        );
        file_put_contents(
            $root . '/upload/table4a.csv',
            "\"test\",\"test\"\n\"aabb\",\"ccdd\"\n",
        );
        file_put_contents(
            $root . '/upload/table4a.csv.manifest',
            (string) json_encode(['columns' => ['Id', 'Name']]),
        );

        // non-typed table prepare
        $this->clientWrapper->getTableAndFileStorageClient()->createTableAsync(
            $this->emptyOutputBucketId,
            'table2a',
            new CsvFile($root . '/upload/table2a.csv'),
        );

        // typed table prepare
        $this->clientWrapper->getTableAndFileStorageClient()->createTableDefinition(
            $this->emptyOutputBucketId,
            [
                'name' => 'table3a',
                'columns' => [
                    [
                        'name' => 'Id',
                        'basetype' => 'STRING',
                    ],
                    [
                        'name' => 'Name',
                        'basetype' => 'STRING',
                    ],
                ],
            ],
        );

        $table1Id = $this->emptyOutputBucketId . '.table1a';
        $table2Id = $this->emptyOutputBucketId . '.table2a';
        $table3Id = $this->emptyOutputBucketId . '.table3a';
        $table4Id = $this->emptyOutputBucketId . '.table4a';

        $configs = [
            [
                'source' => 'table1a.csv',
                'destination' => $table1Id,
            ],
            [
                'source' => 'table2a.csv',
                'destination' => $table2Id,
            ],
            [
                'source' => 'table3a.csv',
                'destination' => $table3Id,
            ],
            [
                'source' => 'table4a.csv',
                'destination' => $table4Id,
            ],
        ];

        $stagingFactory = $this->getLocalStagingFactory(logger: $this->testLogger);
        $tableQueue = $this->getTableLoader(
            logger: $this->testLogger,
            strategyFactory: $stagingFactory,
        )->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [
                    'mapping' => $configs,
                    'treat_values_as_null' => ['aabb'],
                ],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );

        $jobIds = $tableQueue->waitForAll();
        self::assertCount(4, $jobIds);

        // new table
        $job = $this->findTableCreateJobForTable($table1Id, $jobIds);
        self::assertNotNull($job);
        self::assertArrayNotHasKey('treatValuesAsNull', $job['operationParams']['params']);

        // non-typed table
        $job = $this->findTableImportJobForTable($table2Id, $jobIds);
        self::assertNotNull($job);
        self::assertArrayHasKey('treatValuesAsNull', $job['operationParams']['params']);
        self::assertSame(['aabb'], $job['operationParams']['params']['treatValuesAsNull']);

        self::assertTrue($this->testHandler->hasWarningThatContains(
            'Treating values as null for table "table1a" was skipped.',
        ));

        /** @var string|array $data */
        $data = $this->clientWrapper->getTableAndFileStorageClient()->getTableDataPreview(
            $table2Id,
            [
                'format' => 'json',
                'orderBy' => [
                    [
                        'column' => 'Name',
                        'order' => 'DESC',
                    ],
                ],
            ],
        );

        self::assertIsArray($data);
        self::assertArrayHasKey('rows', $data);
        self::assertSame(
            [
                [
                    [
                        'columnName' => 'Id',
                        'value' => 'test',
                        'isTruncated' => false,
                    ],
                    [
                        'columnName' => 'Name',
                        'value' => 'test',
                        'isTruncated' => false,
                    ],
                ],
                [
                    [
                        'columnName' => 'Id',
                        'value' => '',
                        'isTruncated' => false,
                    ],
                    [
                        'columnName' => 'Name',
                        'value' => 'ccdd',
                        'isTruncated' => false,
                    ],
                ],
            ],
            $data['rows'],
        );

        // typed table
        $job = $this->findTableImportJobForTable($table3Id, $jobIds);
        self::assertNotNull($job);
        self::assertArrayHasKey('treatValuesAsNull', $job['operationParams']['params']);
        self::assertSame(['aabb'], $job['operationParams']['params']['treatValuesAsNull']);

        /** @var string|array $data */
        $data = $this->clientWrapper->getTableAndFileStorageClient()->getTableDataPreview(
            $table3Id,
            [
                'format' => 'json',
                'orderBy' => [
                    [
                        'column' => 'Name',
                        'order' => 'DESC',
                    ],
                ],
            ],
        );

        self::assertIsArray($data);
        self::assertArrayHasKey('rows', $data);
        self::assertSame(
            [
                [
                    [
                        'columnName' => 'Id',
                        'value' => 'test',
                        'isTruncated' => false,
                    ],
                    [
                        'columnName' => 'Name',
                        'value' => 'test',
                        'isTruncated' => false,
                    ],
                ],
                [
                    [
                        'columnName' => 'Id',
                        'value' => null,
                        'isTruncated' => false,
                    ],
                    [
                        'columnName' => 'Name',
                        'value' => 'ccdd',
                        'isTruncated' => false,
                    ],
                ],
            ],
            $data['rows'],
        );

        // non-typed table with freshlyCreatedTable on OM process
        $job = $this->findTableImportJobForTable($table4Id, $jobIds);
        self::assertNotNull($job);
        self::assertArrayHasKey('treatValuesAsNull', $job['operationParams']['params']);
        self::assertSame(['aabb'], $job['operationParams']['params']['treatValuesAsNull']);

        /** @var string|array $data */
        $data = $this->clientWrapper->getTableAndFileStorageClient()->getTableDataPreview(
            $table4Id,
            [
                'format' => 'json',
                'orderBy' => [
                    [
                        'column' => 'Name',
                        'order' => 'DESC',
                    ],
                ],
            ],
        );

        self::assertIsArray($data);
        self::assertArrayHasKey('rows', $data);
        self::assertSame(
            [
                [
                    [
                        'columnName' => 'Id',
                        'value' => 'test',
                        'isTruncated' => false,
                    ],
                    [
                        'columnName' => 'Name',
                        'value' => 'test',
                        'isTruncated' => false,
                    ],
                ],
                [
                    [
                        'columnName' => 'Id',
                        'value' => '',
                        'isTruncated' => false,
                    ],
                    [
                        'columnName' => 'Name',
                        'value' => 'ccdd',
                        'isTruncated' => false,
                    ],
                ],
            ],
            $data['rows'],
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableWithoutComponentId(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/table71.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n");
        file_put_contents($root . '/upload/table71.csv.manifest', '{}');

        $this->expectException(OutputOperationException::class);
        $this->expectExceptionMessage('Component Id must be set');

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['bucket' => $this->emptyOutputBucketId],
                sourcePathPrefix: '/upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata([]),
        );
        $tableQueue->waitForAll();
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

    private function findTableCreateJobForTable(string $tableId, array $jobsIds): ?array
    {
        foreach ($jobsIds as $jobId) {
            /** @var array{
             *      operationName: string,
             *      operationParams: array{
             *          source: array{fileId: string},
             *          params: array,
             *      },
             *      results: array{id: string},
             * } $job
             */
            $job = $this->clientWrapper->getBranchClient()->getJob($jobId);
            if ($job['operationName'] === 'tableCreate' && $job['results']['id'] === $tableId) {
                return $job;
            }
        }

        return null;
    }

    private function findTableImportJobForTable(string $tableId, array $jobsIds): ?array
    {
        foreach ($jobsIds as $jobId) {
            /** @var array{
             *      operationName: string,
             *      tableId: string,
             *      operationParams: array{
             *          source: array{fileId: string},
             *          params: array,
             *      },
             *      results: array{id: string},
             * } $job
             */
            $job = $this->clientWrapper->getBranchClient()->getJob($jobId);
            if ($job['operationName'] === 'tableImport' && $job['tableId'] === $tableId) {
                return $job;
            }
        }

        return null;
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingWithPkOverwriteWithDifferentPk(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/table14.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        // První nahrání s primárním klíčem Id
        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [
                    'mapping' => [
                        [
                            'source' => 'table14.csv',
                            'destination' => $this->emptyOutputBucketId . '.table14',
                            'primary_key' => ['Id'],
                        ],
                    ],
                ],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
        $tableQueue->waitForAll();

        $tableQueue = $this->getTableLoader(
            logger: $this->testLogger,
            strategyFactory: $this->getLocalStagingFactory(
                logger: $this->testLogger,
            ),
        )->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [
                    'mapping' => [
                        [
                            'source' => 'table14.csv',
                            'destination' => $this->emptyOutputBucketId . '.table14',
                            'primary_key' => ['Name'],
                        ],
                    ],
                ],
                sourcePathPrefix: '/upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);
    }
}
