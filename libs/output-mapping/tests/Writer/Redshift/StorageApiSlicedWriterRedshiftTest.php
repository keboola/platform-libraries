<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Redshift;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyRedshiftOutputBucket;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\TableExporter;
use Keboola\StorageApiBranch\ClientWrapper;

class StorageApiSlicedWriterRedshiftTest extends AbstractTestCase
{
    public function initBucket(string $backendType): void
    {
        $this->clientWrapper->getBasicClient()->createBucket(
            'StorageApiSlicedWriterRedshiftTest',
            'out',
            '',
            $backendType
        );
    }

    #[NeedsEmptyRedshiftOutputBucket]
    public function testWriteTableOutputMapping(): void
    {
        $root = $this->temp->getTmpFolder();
        mkdir($root . '/upload/table.csv');
        file_put_contents($root . '/upload/table.csv/part1', "\"test\",\"test\"\n");
        file_put_contents($root . '/upload/table.csv/part2', "\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                'source' => 'table.csv',
                'destination' => $this->emptyRedshiftOutputBucketId . '.table',
                'columns' => ['Id', 'Name'],
            ],
        ];

        $writer = new TableWriter($this->getLocalStagingFactory());
        $tableQueue = $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::LOCAL,
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables($this->emptyRedshiftOutputBucketId);
        self::assertCount(1, $tables);
        $table = $this->clientWrapper->getBasicClient()->getTable($this->emptyRedshiftOutputBucketId . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyRedshiftOutputBucketId . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv((string) file_get_contents($downloadedFile));
        self::assertCount(2, $table);
        self::assertContains(['Id' => 'test', 'Name' => 'test'], $table);
        self::assertContains(['Id' => 'aabb', 'Name' => 'ccdd'], $table);

        $job = $this->clientWrapper->getBasicClient()->getJob($jobIds[0]);
        $fileId = $job['operationParams']['source']['fileId'];
        $file = $this->clientWrapper->getBasicClient()->getFile($fileId);
        self::assertEquals([], $file['tags']);
    }

    #[NeedsEmptyRedshiftOutputBucket]
    public function testWriteTableTagStagingFile(): void
    {
        $root = $this->temp->getTmpFolder();
        mkdir($root . '/upload/table.csv');
        file_put_contents($root . '/upload/table.csv/part1', "\"test\",\"test\"\n");
        file_put_contents($root . '/upload/table.csv/part2', "\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                'source' => 'table.csv',
                'destination' => $this->emptyRedshiftOutputBucketId . '.table',
                'columns' => ['Id', 'Name'],
            ],
        ];

        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        $client = $this->getMockBuilder(Client::class)
            ->setConstructorArgs([[
                'url' => (string) getenv('STORAGE_API_URL'),
                'token' => (string) getenv('STORAGE_API_TOKEN'),
            ]])
            ->setMethods(['verifyToken'])
            ->getMock();
        $tokenInfo['owner']['features'][] = 'tag-staging-files';
        $client->method('verifyToken')->willReturn($tokenInfo);
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getBasicClient')->willReturn($client);
        $writer = new TableWriter($this->getLocalStagingFactory($clientWrapper));

        $tableQueue = $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::LOCAL,
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables($this->emptyRedshiftOutputBucketId);
        self::assertCount(1, $tables);
        $table = $this->clientWrapper->getBasicClient()->getTable($this->emptyRedshiftOutputBucketId . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyRedshiftOutputBucketId . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv((string) file_get_contents($downloadedFile));
        self::assertCount(2, $table);
        self::assertContains(['Id' => 'test', 'Name' => 'test'], $table);
        self::assertContains(['Id' => 'aabb', 'Name' => 'ccdd'], $table);

        $job = $this->clientWrapper->getBasicClient()->getJob($jobIds[0]);
        $fileId = $job['operationParams']['source']['fileId'];
        $file = $this->clientWrapper->getBasicClient()->getFile($fileId);
        self::assertEquals(
            ['componentId: foo'],
            $file['tags']
        );
    }

    #[NeedsEmptyRedshiftOutputBucket]
    public function testWriteTableOutputMappingEmptySlice(): void
    {
        $root = $this->temp->getTmpFolder();
        mkdir($root . '/upload/table');
        file_put_contents($root . '/upload/table/part1', '');
        $configs = [
            [
                'source' => 'table',
                'destination' => $this->emptyRedshiftOutputBucketId . '.table',
                'columns' => ['Id', 'Name'],
            ],
        ];

        $writer = new TableWriter($this->getLocalStagingFactory());
        $tableQueue = $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::LOCAL,
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $table = $this->clientWrapper->getBasicClient()->getTable($this->emptyRedshiftOutputBucketId . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyRedshiftOutputBucketId . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv((string) file_get_contents($downloadedFile));
        self::assertCount(0, $table);
    }

    #[NeedsEmptyRedshiftOutputBucket]
    public function testWriteTableOutputMappingEmptySliceExistingTable(): void
    {
        $fileName = $this->temp->getTmpFolder() . uniqid('csv-');
        file_put_contents($fileName, "\"Id\",\"Name\"\n\"ab\",\"cd\"\n");
        $csv = new CsvFile($fileName);
        $this->clientWrapper->getBasicClient()->createTableAsync($this->emptyRedshiftOutputBucketId, 'table16', $csv);

        $root = $this->temp->getTmpFolder();
        mkdir($root . '/upload/table16');
        file_put_contents($root . '/upload/table16/part1', '');
        $configs = [
            [
                'source' => 'table16',
                'destination' => $this->emptyRedshiftOutputBucketId . '.table16',
                'columns' => ['Id', 'Name'],
            ],
        ];

        $writer = new TableWriter($this->getLocalStagingFactory());
        $tableQueue = $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::LOCAL,
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $table = $this->clientWrapper->getBasicClient()->getTable($this->emptyRedshiftOutputBucketId . '.table16');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyRedshiftOutputBucketId . '.table16', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv((string) file_get_contents($downloadedFile));
        self::assertCount(0, $table);
    }

    #[NeedsEmptyRedshiftOutputBucket]
    public function testWriteTableOutputMappingEmptyDir(): void
    {
        $root = $this->temp->getTmpFolder();
        mkdir($root . '/upload/table15');

        $configs = [
            [
                'source' => 'table15',
                'destination' => $this->emptyRedshiftOutputBucketId . '.table15',
                'columns' => ['Id', 'Name'],
            ],
        ];

        $writer = new TableWriter($this->getLocalStagingFactory());
        $tableQueue = $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::LOCAL,
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $table = $this->clientWrapper->getBasicClient()->getTable($this->emptyRedshiftOutputBucketId . '.table15');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyRedshiftOutputBucketId . '.table15', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv((string) file_get_contents($downloadedFile));
        self::assertCount(0, $table);
    }

    #[NeedsEmptyRedshiftOutputBucket]
    public function testWriteTableOutputMappingEmptyDirExistingTable(): void
    {
        $fileName = $this->temp->getTmpFolder() . uniqid('csv-');
        file_put_contents($fileName, "\"Id\",\"Name\"\n\"ab\",\"cd\"\n");
        $csv = new CsvFile($fileName);
        $this->clientWrapper->getBasicClient()->createTableAsync($this->emptyRedshiftOutputBucketId, 'table17', $csv);

        $root = $this->temp->getTmpFolder();
        mkdir($root . '/upload/table17');

        $configs = [
            [
                'source' => 'table17',
                'destination' => $this->emptyRedshiftOutputBucketId . '.table17',
                'columns' => ['Id', 'Name'],
            ],
        ];

        $writer = new TableWriter($this->getLocalStagingFactory());
        $tableQueue = $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::LOCAL,
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $table = $this->clientWrapper->getBasicClient()->getTable($this->emptyRedshiftOutputBucketId . '.table17');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyRedshiftOutputBucketId . '.table17', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv((string) file_get_contents($downloadedFile));
        self::assertCount(0, $table);
    }

    #[NeedsEmptyRedshiftOutputBucket]
    public function testWriteTableOutputMappingMissingHeaders(): void
    {
        $root = $this->temp->getTmpFolder();
        mkdir($root . '/upload/table');

        $configs = [
            [
                'source' => 'table',
                'destination' => $this->emptyRedshiftOutputBucketId . '.table',
            ],
        ];

        $writer = new TableWriter($this->getLocalStagingFactory());
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

    #[NeedsEmptyRedshiftOutputBucket]
    public function testWriteTableOutputMappingExistingTable(): void
    {
        $csvFile = new CsvFile($this->temp->createFile('header')->getPathname());
        $csvFile->writeRow(['Id', 'Name']);
        $this->clientWrapper->getBasicClient()->createTableAsync($this->emptyRedshiftOutputBucketId, 'table', $csvFile);
        $tables = $this->clientWrapper->getBasicClient()->listTables($this->emptyRedshiftOutputBucketId);
        self::assertCount(1, $tables);
        $table = $this->clientWrapper->getBasicClient()->getTable($this->emptyRedshiftOutputBucketId . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $root = $this->temp->getTmpFolder();
        mkdir($root . '/upload/table.csv');
        file_put_contents($root . '/upload/table.csv/part1', "\"test\",\"test\"\n");
        file_put_contents($root . '/upload/table.csv/part2', "\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                'source' => 'table.csv',
                'destination' => $this->emptyRedshiftOutputBucketId . '.table',
                'columns' => ['Id', 'Name'],
            ],
        ];

        $writer = new TableWriter($this->getLocalStagingFactory());

        $tableQueue = $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::LOCAL,
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables($this->emptyRedshiftOutputBucketId);
        self::assertCount(1, $tables);
        $table = $this->clientWrapper->getBasicClient()->getTable($this->emptyRedshiftOutputBucketId . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyRedshiftOutputBucketId . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv((string) file_get_contents($downloadedFile));
        self::assertCount(2, $table);
        self::assertContains(['Id' => 'test', 'Name' => 'test'], $table);
        self::assertContains(['Id' => 'aabb', 'Name' => 'ccdd'], $table);
    }

    #[NeedsEmptyRedshiftOutputBucket]
    public function testWriteTableOutputMappingDifferentDelimiterEnclosure(): void
    {
        $root = $this->temp->getTmpFolder();
        mkdir($root . '/upload/table.csv');
        file_put_contents($root . '/upload/table.csv/part1', "'test'|'test'\n");
        file_put_contents($root . '/upload/table.csv/part2', "'aabb'|'ccdd'\n");

        $configs = [
            [
                'source' => 'table.csv',
                'destination' => $this->emptyRedshiftOutputBucketId . '.table',
                'columns' => ['Id', 'Name'],
                'delimiter' => '|',
                'enclosure' => "'",
            ],
        ];

        $writer = new TableWriter($this->getLocalStagingFactory());

        $tableQueue = $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::LOCAL,
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables($this->emptyRedshiftOutputBucketId);
        self::assertCount(1, $tables);
        $table = $this->clientWrapper->getBasicClient()->getTable($this->emptyRedshiftOutputBucketId . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyRedshiftOutputBucketId . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv((string) file_get_contents($downloadedFile));
        self::assertCount(2, $table);
        self::assertContains(['Id' => 'test', 'Name' => 'test'], $table);
        self::assertContains(['Id' => 'aabb', 'Name' => 'ccdd'], $table);
    }

    #[NeedsEmptyRedshiftOutputBucket]
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
                'destination' => $this->emptyRedshiftOutputBucketId . '.table',
                'columns' => ['Id', 'Name'],
            ],
            [
                'source' => 'table2.csv',
                'destination' => $this->emptyRedshiftOutputBucketId . '.table2',
                'columns' => ['Id', 'Name'],
            ],
        ];

        $writer = new TableWriter($this->getLocalStagingFactory());
        $tableQueue = $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::LOCAL,
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(2, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables($this->emptyRedshiftOutputBucketId);
        self::assertCount(2, $tables);
        $table = $this->clientWrapper->getBasicClient()->getTable($this->emptyRedshiftOutputBucketId . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);
        $table = $this->clientWrapper->getBasicClient()->getTable($this->emptyRedshiftOutputBucketId . '.table2');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyRedshiftOutputBucketId . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv((string) file_get_contents($downloadedFile));
        self::assertCount(2, $table);
        self::assertContains(['Id' => 'test', 'Name' => 'test'], $table);
        self::assertContains(['Id' => 'aabb', 'Name' => 'ccdd'], $table);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyRedshiftOutputBucketId . '.table2', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv((string) file_get_contents($downloadedFile));
        self::assertCount(2, $table);
        self::assertContains(['Id' => 'test', 'Name' => 'test'], $table);
        self::assertContains(['Id' => 'aabb', 'Name' => 'ccdd'], $table);
    }

    #[NeedsEmptyRedshiftOutputBucket]
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
                'destination' => $this->emptyRedshiftOutputBucketId . '.table18',
                'columns' => ['Id', 'Name'],
            ],
        ];

        $writer = new TableWriter($this->getLocalStagingFactory());

        $tableQueue = $writer->uploadTables(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::LOCAL,
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables($this->emptyRedshiftOutputBucketId);
        self::assertCount(1, $tables);
        $table = $this->clientWrapper->getBasicClient()->getTable($this->emptyRedshiftOutputBucketId . '.table18');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyRedshiftOutputBucketId . '.table18', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv((string) file_get_contents($downloadedFile));
        self::assertCount(2, $table);
        self::assertContains(['Id' => 'test', 'Name' => 'test'], $table);
        self::assertContains(['Id' => 'aabb', 'Name' => 'ccdd'], $table);
    }
}
