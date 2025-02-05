<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\OutputMappingSettings;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\StorageApi\TableExporter;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\StorageApiToken;

class StorageApiSlicedWriterTest extends AbstractTestCase
{
    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMapping(): void
    {
        $root = $this->temp->getTmpFolder();
        mkdir($root . '/upload/table.csv');
        file_put_contents($root . '/upload/table.csv/part1', "\"test\",\"test\"\n");
        file_put_contents($root . '/upload/table.csv/part2', "\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                'source' => 'table.csv',
                'destination' => $this->emptyOutputBucketId . '.table',
                'columns' => ['Id','Name'],
            ],
        ];

        $tableQueue = $this->getTableLoader()->uploadTables(
            outputStaging: AbstractStrategyFactory::LOCAL,
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->getLocalStagingFactory()->getClientWrapper()->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->emptyOutputBucketId . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyOutputBucketId . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->parseCsv(
            (string) file_get_contents($downloadedFile),
        );
        self::assertCount(2, $table);
        self::assertContains(['Id' => 'test', 'Name' => 'test'], $table);
        self::assertContains(['Id' => 'aabb', 'Name' => 'ccdd'], $table);

        /** @var array{
         *     operationParams: array{source: array{fileId: string}},
         * } $job
         */
        $job = $this->clientWrapper->getBranchClient()->getJob($jobIds[0]);
        $fileId = $job['operationParams']['source']['fileId'];
        $file = $this->clientWrapper->getTableAndFileStorageClient()->getFile($fileId);
        self::assertEquals([], $file['tags']);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableTagStagingFile(): void
    {
        $root = $this->temp->getTmpFolder();
        mkdir($root . '/upload/table.csv');
        file_put_contents($root . '/upload/table.csv/part1', "\"test\",\"test\"\n");
        file_put_contents($root . '/upload/table.csv/part2', "\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                'source' => 'table.csv',
                'destination' => $this->emptyOutputBucketId . '.table',
                'columns' => ['Id','Name'],
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

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getToken')->willReturn($token);
        $clientWrapper->method('getBranchClient')->willReturn(
            $this->clientWrapper->getBranchClient(),
        );
        $clientWrapper->method('getTableAndFileStorageClient')->willReturn(
            $this->clientWrapper->getBranchClient(),
        );

        $stagingFactory = $this->getWorkspaceStagingFactory($clientWrapper);
        $tableQueue = $this->getTableLoader($stagingFactory)->uploadTables(
            outputStaging: AbstractStrategyFactory::LOCAL,
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: 'upload',
                storageApiToken: $stagingFactory->getClientWrapper()->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->emptyOutputBucketId . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyOutputBucketId . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->parseCsv(
            (string) file_get_contents($downloadedFile),
        );
        self::assertCount(2, $table);
        self::assertContains(['Id' => 'test', 'Name' => 'test'], $table);
        self::assertContains(['Id' => 'aabb', 'Name' => 'ccdd'], $table);

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
    public function testWriteTableOutputMappingEmptySlice(): void
    {
        $root = $this->temp->getTmpFolder();
        mkdir($root . '/upload/table');
        file_put_contents($root . '/upload/table/part1', '');
        $configs = [
            [
                'source' => 'table',
                'destination' => $this->emptyOutputBucketId . '.table',
                'columns' => ['Id','Name'],
            ],
        ];

        $tableQueue = $this->getTableLoader()->uploadTables(
            outputStaging: AbstractStrategyFactory::LOCAL,
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->getLocalStagingFactory()->getClientWrapper()->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->emptyOutputBucketId . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyOutputBucketId . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->parseCsv(
            (string) file_get_contents($downloadedFile),
        );
        self::assertCount(0, $table);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingEmptySliceExistingTable(): void
    {
        $fileName = $this->temp->getTmpFolder() . uniqid('csv-');
        file_put_contents($fileName, "\"Id\",\"Name\"\n\"ab\",\"cd\"\n");
        $csv = new CsvFile($fileName);
        $this->clientWrapper->getTableAndFileStorageClient()->createTableAsync(
            $this->emptyOutputBucketId,
            'table16',
            $csv,
        );

        $root = $this->temp->getTmpFolder();
        mkdir($root . '/upload/table16');
        file_put_contents($root . '/upload/table16/part1', '');
        $configs = [
            [
                'source' => 'table16',
                'destination' => $this->emptyOutputBucketId . '.table16',
                'columns' => ['Id','Name'],
            ],
        ];

        $tableQueue = $this->getTableLoader()->uploadTables(
            outputStaging: AbstractStrategyFactory::LOCAL,
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->getLocalStagingFactory()->getClientWrapper()->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId . '.table16',
        );
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyOutputBucketId . '.table16', $downloadedFile, []);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->parseCsv(
            (string) file_get_contents($downloadedFile),
        );
        self::assertCount(0, $table);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingEmptyDir(): void
    {
        $root = $this->temp->getTmpFolder();
        mkdir($root . '/upload/table15');

        $configs = [
            [
                'source' => 'table15',
                'destination' => $this->emptyOutputBucketId . '.table15',
                'columns' => ['Id','Name'],
            ],
        ];

        $tableQueue = $this->getTableLoader()->uploadTables(
            outputStaging: AbstractStrategyFactory::LOCAL,
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->getLocalStagingFactory()->getClientWrapper()->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId . '.table15',
        );
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyOutputBucketId . '.table15', $downloadedFile, []);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->parseCsv(
            (string) file_get_contents($downloadedFile),
        );
        self::assertCount(0, $table);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingEmptyDirExistingTable(): void
    {
        $fileName = $this->temp->getTmpFolder() . uniqid('csv-');
        file_put_contents($fileName, "\"Id\",\"Name\"\n\"ab\",\"cd\"\n");
        $csv = new CsvFile($fileName);
        $this->clientWrapper->getTableAndFileStorageClient()->createTableAsync(
            $this->emptyOutputBucketId,
            'table17',
            $csv,
        );

        $root = $this->temp->getTmpFolder();
        mkdir($root . '/upload/table17');

        $configs = [
            [
                'source' => 'table17',
                'destination' => $this->emptyOutputBucketId . '.table17',
                'columns' => ['Id','Name'],
            ],
        ];

        $tableQueue = $this->getTableLoader()->uploadTables(
            outputStaging: AbstractStrategyFactory::LOCAL,
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->getLocalStagingFactory()->getClientWrapper()->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId . '.table17',
        );
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyOutputBucketId . '.table17', $downloadedFile, []);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->parseCsv(
            (string) file_get_contents($downloadedFile),
        );
        self::assertCount(0, $table);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingMissingHeaders(): void
    {
        $root = $this->temp->getTmpFolder();
        mkdir($root . '/upload/table');

        $configs = [
            [
                'source' => 'table',
                'destination' => $this->emptyOutputBucketId . '.table',
            ],
        ];

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Sliced file "table" columns specification missing.');
        $this->getTableLoader()->uploadTables(
            outputStaging: AbstractStrategyFactory::LOCAL,
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->getLocalStagingFactory()->getClientWrapper()->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingExistingTable(): void
    {
        $csvFile = new CsvFile($this->temp->createFile('header')->getPathname());
        $csvFile->writeRow(['Id', 'Name']);
        $this->clientWrapper->getTableAndFileStorageClient()->createTableAsync(
            $this->emptyOutputBucketId,
            'table',
            $csvFile,
        );
        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->emptyOutputBucketId . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $root = $this->temp->getTmpFolder();
        mkdir($root . '/upload/table.csv');
        file_put_contents($root . '/upload/table.csv/part1', "\"test\",\"test\"\n");
        file_put_contents($root . '/upload/table.csv/part2', "\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                'source' => 'table.csv',
                'destination' => $this->emptyOutputBucketId . '.table',
                'columns' => ['Id','Name'],
            ],
        ];

        $tableQueue = $this->getTableLoader()->uploadTables(
            outputStaging: AbstractStrategyFactory::LOCAL,
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->getLocalStagingFactory()->getClientWrapper()->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->emptyOutputBucketId . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyOutputBucketId . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->parseCsv(
            (string) file_get_contents($downloadedFile),
        );
        self::assertCount(2, $table);
        self::assertContains(['Id' => 'test', 'Name' => 'test'], $table);
        self::assertContains(['Id' => 'aabb', 'Name' => 'ccdd'], $table);
    }

    /**
     * @dataProvider incrementalFlagProvider
     */
    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingExistingTableAddColumns(bool $incrementalFlag): void
    {
        $csvFile = new CsvFile($this->temp->createFile('header')->getPathname());
        $csvFile->writeRow(['Id', 'Name']);
        $tableId = $this->clientWrapper->getTableAndFileStorageClient()->createTableAsync(
            $this->emptyOutputBucketId,
            'table',
            $csvFile,
        );
        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->emptyOutputBucketId . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $root = $this->temp->getTmpFolder();
        mkdir($root . '/upload/table.csv');
        file_put_contents($root . '/upload/table.csv/part1', "\"test\",\"test\",\"test\"\n");
        file_put_contents($root . '/upload/table.csv/part2', "\"aabb\",\"ccdd\",\"eeff\"\n");

        $configs = [
            [
                'source' => 'table.csv',
                'destination' => $tableId,
                'incremental' => $incrementalFlag,
                'columns' => ['Id','Name','City'],
            ],
        ];

        $runId = $this->clientWrapper->getBasicClient()->generateRunId();
        $this->clientWrapper->getTableAndFileStorageClient()->setRunId($runId);

        $tableQueue = $this->getTableLoader()->uploadTables(
            outputStaging: AbstractStrategyFactory::LOCAL,
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->getLocalStagingFactory()->getClientWrapper()->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $writerJobs = array_filter(
            $this->clientWrapper->getTableAndFileStorageClient()->listJobs(),
            function (array $job) use ($runId) {
                return $runId === $job['runId'];
            },
        );

        self::assertCount(2, $writerJobs);

        self::assertTableColumnAddJob(array_pop($writerJobs), 'City');
        self::assertTableImportJob(array_pop($writerJobs), $incrementalFlag);

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->emptyOutputBucketId . '.table');
        self::assertEquals(['Id', 'Name', 'City'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($tableId, $downloadedFile, []);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->parseCsv(
            (string) file_get_contents($downloadedFile),
        );
        self::assertCount(2, $table);
        self::assertContains(['Id' => 'test', 'Name' => 'test', 'City' => 'test'], $table);
        self::assertContains(['Id' => 'aabb', 'Name' => 'ccdd', 'City' => 'eeff'], $table);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingDifferentDelimiterEnclosure(): void
    {
        $root = $this->temp->getTmpFolder();
        mkdir($root . '/upload/table.csv');
        file_put_contents($root . '/upload/table.csv/part1', "'test'|'test'\n");
        file_put_contents($root . '/upload/table.csv/part2', "'aabb'|'ccdd'\n");

        $configs = [
            [
                'source' => 'table.csv',
                'destination' => $this->emptyOutputBucketId . '.table',
                'columns' => ['Id','Name'],
                'delimiter' => '|',
                'enclosure' => "'",
            ],
        ];

        $tableQueue = $this->getTableLoader()->uploadTables(
            outputStaging: AbstractStrategyFactory::LOCAL,
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->getLocalStagingFactory()->getClientWrapper()->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->emptyOutputBucketId . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyOutputBucketId . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->parseCsv(
            (string) file_get_contents($downloadedFile),
        );
        self::assertCount(2, $table);
        self::assertContains(['Id' => 'test', 'Name' => 'test'], $table);
        self::assertContains(['Id' => 'aabb', 'Name' => 'ccdd'], $table);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingCombination(): void
    {
        $root = $this->temp->getTmpFolder();
        mkdir($root . '/upload/table.csv');
        file_put_contents($root . '/upload/table.csv.manifest', json_encode([
            'columns' => ['Id','Name'],
            'primary_key' => ['Id','Name'],
        ]));
        file_put_contents($root . '/upload/table.csv/part1', "\"test\",\"test\"\n");
        file_put_contents($root . '/upload/table.csv/part2', "\"aabb\",\"ccdd\"\n");
        file_put_contents($root . '/upload/table2.csv.manifest', json_encode([
            'columns' => ['Id','Name'],
            'primary_key' => ['Id','Name'],
        ]));
        file_put_contents($root . '/upload/table2.csv', "\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                'source' => 'table.csv',
                'destination' => $this->emptyOutputBucketId . '.table',
                'primary_key' => ['Id'],
            ],
            [
                'source' => 'table2.csv',
                'destination' => $this->emptyOutputBucketId . '.table2',
                'primary_key' => ['Id'],
            ],
        ];

        $tableQueue = $this->getTableLoader()->uploadTables(
            outputStaging: AbstractStrategyFactory::LOCAL,
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->getLocalStagingFactory()->getClientWrapper()->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(2, $jobIds);

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(2, $tables);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->emptyOutputBucketId . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);
        self::assertEquals(['Id'], $table['primaryKey']);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->emptyOutputBucketId . '.table2');
        self::assertEquals(['Id', 'Name'], $table['columns']);
        self::assertEquals(['Id'], $table['primaryKey']);

        $exporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyOutputBucketId . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->parseCsv(
            (string) file_get_contents($downloadedFile),
        );
        self::assertCount(2, $table);
        self::assertContains(['Id' => 'test', 'Name' => 'test'], $table);
        self::assertContains(['Id' => 'aabb', 'Name' => 'ccdd'], $table);

        $exporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyOutputBucketId . '.table2', $downloadedFile, []);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->parseCsv(
            (string) file_get_contents($downloadedFile),
        );
        self::assertCount(2, $table);
        self::assertContains(['Id' => 'test', 'Name' => 'test'], $table);
        self::assertContains(['Id' => 'aabb', 'Name' => 'ccdd'], $table);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingCompression(): void
    {
        $root = $this->temp->getTmpFolder();
        mkdir($root . '/upload/table18.csv');
        file_put_contents($root . '/upload/table18.csv/part1', "\"test\",\"test\"\n");
        file_put_contents($root . '/upload/table18.csv/part2', "\"aabb\",\"ccdd\"\n");
        exec('gzip ' . $root . '/upload/table18.csv/part1');
        exec('gzip ' . $root . '/upload/table18.csv/part2');

        $configs = [
            [
                'source' => 'table18.csv',
                'destination' => $this->emptyOutputBucketId . '.table18',
                'columns' => ['Id','Name'],
            ],
        ];

        $tableQueue = $this->getTableLoader()->uploadTables(
            outputStaging: AbstractStrategyFactory::LOCAL,
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => $configs],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->getLocalStagingFactory()->getClientWrapper()->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId . '.table18',
        );
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyOutputBucketId . '.table18', $downloadedFile, []);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->parseCsv(
            (string) file_get_contents($downloadedFile),
        );
        self::assertCount(2, $table);
        self::assertContains(['Id' => 'test', 'Name' => 'test'], $table);
        self::assertContains(['Id' => 'aabb', 'Name' => 'ccdd'], $table);
    }
}
