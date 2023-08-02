<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\TableExporter;
use Keboola\StorageApiBranch\ClientWrapper;

class StorageApiSlicedWriterTest extends AbstractTestCase
{
    public function initBucket(string $backendType): void
    {
        $this->clientWrapper->getTableAndFileStorageClient()->createBucket(
            'StorageApiSlicedWriterTest',
            'out',
            '',
            $backendType
        );
    }

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

        $writer = new TableWriter($this->getWorkspaceStagingFactory());
        $tableQueue =  $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::LOCAL,
            false,
            false
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
            (string) file_get_contents($downloadedFile)
        );
        self::assertCount(2, $table);
        self::assertContains(['Id' => 'test', 'Name' => 'test'], $table);
        self::assertContains(['Id' => 'aabb', 'Name' => 'ccdd'], $table);

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

        $tokenInfo = $this->clientWrapper->getBranchClient()->verifyToken();
        $client = $this->getMockBuilder(BranchAwareClient::class)
            ->setConstructorArgs([[
                'url' => (string) getenv('STORAGE_API_URL'),
                'token' => (string) getenv('STORAGE_API_TOKEN'),
            ]])
            ->onlyMethods(['verifyToken'])
            ->getMock();
        $tokenInfo['owner']['features'][] = 'tag-staging-files';
        $client->method('verifyToken')->willReturn($tokenInfo);
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getBranchClient')->willReturn($client);
        $clientWrapper->method('getTableAndFileStorageClient')->willReturn($client);
        $writer = new TableWriter($this->getWorkspaceStagingFactory($clientWrapper));

        $tableQueue =  $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::LOCAL,
            false,
            false
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
            (string) file_get_contents($downloadedFile)
        );
        self::assertCount(2, $table);
        self::assertContains(['Id' => 'test', 'Name' => 'test'], $table);
        self::assertContains(['Id' => 'aabb', 'Name' => 'ccdd'], $table);

        $job = $this->clientWrapper->getBranchClient()->getJob($jobIds[0]);
        $fileId = $job['operationParams']['source']['fileId'];
        $file = $this->clientWrapper->getTableAndFileStorageClient()->getFile($fileId);
        self::assertEquals(
            ['componentId: foo'],
            $file['tags']
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

        $writer = new TableWriter($this->getWorkspaceStagingFactory());
        $tableQueue =  $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::LOCAL,
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->emptyOutputBucketId . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyOutputBucketId . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->parseCsv(
            (string) file_get_contents($downloadedFile)
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
            $csv
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

        $writer = new TableWriter($this->getWorkspaceStagingFactory());
        $tableQueue =  $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::LOCAL,
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId . '.table16'
        );
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyOutputBucketId . '.table16', $downloadedFile, []);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->parseCsv(
            (string) file_get_contents($downloadedFile)
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

        $writer = new TableWriter($this->getWorkspaceStagingFactory());
        $tableQueue =  $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::LOCAL,
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId . '.table15'
        );
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyOutputBucketId . '.table15', $downloadedFile, []);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->parseCsv(
            (string) file_get_contents($downloadedFile)
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
            $csv
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

        $writer = new TableWriter($this->getWorkspaceStagingFactory());
        $tableQueue =  $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::LOCAL,
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId . '.table17'
        );
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyOutputBucketId . '.table17', $downloadedFile, []);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->parseCsv(
            (string) file_get_contents($downloadedFile)
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

        $writer = new TableWriter($this->getWorkspaceStagingFactory());
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Sliced file "table" columns specification missing.');
        $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::LOCAL,
            false,
            false
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
            $csvFile
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

        $writer = new TableWriter($this->getWorkspaceStagingFactory());

        $tableQueue =  $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::LOCAL,
            false,
            false
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
            (string) file_get_contents($downloadedFile)
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
        $this->clientWrapper->getTableAndFileStorageClient()->createTableAsync(
            $this->emptyOutputBucketId,
            'table',
            $csvFile
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
                'destination' => $this->emptyOutputBucketId . '.table',
                'incremental' => $incrementalFlag,
                'columns' => ['Id','Name','City'],
            ],
        ];

        $runId = $this->clientWrapper->getBasicClient()->generateRunId();
        $this->clientWrapper->getTableAndFileStorageClient()->setRunId($runId);

        $writer = new TableWriter($this->getWorkspaceStagingFactory());

        $tableQueue =  $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::LOCAL,
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $writerJobs = array_filter(
            $this->clientWrapper->getTableAndFileStorageClient()->listJobs(),
            function (array $job) use ($runId) {
                return $runId === $job['runId'];
            }
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
        $exporter->exportTable($this->emptyOutputBucketId . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->parseCsv(
            (string) file_get_contents($downloadedFile)
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

        $writer = new TableWriter($this->getWorkspaceStagingFactory());

        $tableQueue =  $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::LOCAL,
            false,
            false
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
            (string) file_get_contents($downloadedFile)
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
        file_put_contents($root . '/upload/table.csv/part1', "\"test\",\"test\"\n");
        file_put_contents($root . '/upload/table.csv/part2', "\"aabb\",\"ccdd\"\n");
        file_put_contents($root . '/upload/table2.csv', "\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                'source' => 'table.csv',
                'destination' => $this->emptyOutputBucketId . '.table',
                'columns' => ['Id','Name'],
            ],
            [
                'source' => 'table2.csv',
                'destination' => $this->emptyOutputBucketId . '.table2',
                'columns' => ['Id','Name'],
            ],
        ];

        $writer = new TableWriter($this->getWorkspaceStagingFactory());
        $tableQueue =  $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::LOCAL,
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(2, $jobIds);

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(2, $tables);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->emptyOutputBucketId . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->emptyOutputBucketId . '.table2');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyOutputBucketId . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->parseCsv(
            (string) file_get_contents($downloadedFile)
        );
        self::assertCount(2, $table);
        self::assertContains(['Id' => 'test', 'Name' => 'test'], $table);
        self::assertContains(['Id' => 'aabb', 'Name' => 'ccdd'], $table);

        $exporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyOutputBucketId . '.table2', $downloadedFile, []);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->parseCsv(
            (string) file_get_contents($downloadedFile)
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

        $writer = new TableWriter($this->getWorkspaceStagingFactory());

        $tableQueue =  $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::LOCAL,
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
            $this->emptyOutputBucketId . '.table18'
        );
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyOutputBucketId . '.table18', $downloadedFile, []);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->parseCsv(
            (string) file_get_contents($downloadedFile)
        );
        self::assertCount(2, $table);
        self::assertContains(['Id' => 'test', 'Name' => 'test'], $table);
        self::assertContains(['Id' => 'aabb', 'Name' => 'ccdd'], $table);
    }
}
