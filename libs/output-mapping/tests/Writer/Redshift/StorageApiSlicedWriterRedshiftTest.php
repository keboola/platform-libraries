<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Redshift;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Tests\Writer\BaseWriterTest;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\TableExporter;
use Keboola\StorageApiBranch\ClientWrapper;

class StorageApiSlicedWriterRedshiftTest extends BaseWriterTest
{
    private const OUTPUT_BUCKET = 'out.c-StorageApiSlicedWriterRedshiftTest';
    private const FILE_TAG = 'StorageApiSlicedWriterRedshiftTest';

    public function setUp(): void
    {
        parent::setUp();
        $this->clearBuckets([self::OUTPUT_BUCKET]);
        $this->clearFileUploads([self::FILE_TAG]);
    }

    public function initBucket(string $backendType): void
    {
        $this->clientWrapper->getBasicClient()->createBucket(
            'StorageApiSlicedWriterRedshiftTest',
            'out',
            '',
            $backendType
        );
    }

    public function testWriteTableOutputMapping(): void
    {
        $this->initBucket('redshift');
        $root = $this->tmp->getTmpFolder();
        mkdir($root . '/upload/table.csv');
        file_put_contents($root . '/upload/table.csv/part1', "\"test\",\"test\"\n");
        file_put_contents($root . '/upload/table.csv/part2', "\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                'source' => 'table.csv',
                'destination' => self::OUTPUT_BUCKET . '.table',
                'columns' => ['Id', 'Name'],
            ],
        ];

        $writer = new TableWriter($this->getStagingFactory());
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

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        self::assertCount(1, $tables);
        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv((string) file_get_contents($downloadedFile));
        self::assertCount(2, $table);
        self::assertContains(['Id' => 'test', 'Name' => 'test'], $table);
        self::assertContains(['Id' => 'aabb', 'Name' => 'ccdd'], $table);

        $job = $this->clientWrapper->getBasicClient()->getJob($jobIds[0]);
        $fileId = $job['operationParams']['source']['fileId'];
        $file = $this->clientWrapper->getBasicClient()->getFile($fileId);
        self::assertEquals([], $file['tags']);
    }

    public function testWriteTableTagStagingFile(): void
    {
        $this->initBucket('redshift');
        $root = $this->tmp->getTmpFolder();
        mkdir($root . '/upload/table.csv');
        file_put_contents($root . '/upload/table.csv/part1', "\"test\",\"test\"\n");
        file_put_contents($root . '/upload/table.csv/part2', "\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                'source' => 'table.csv',
                'destination' => self::OUTPUT_BUCKET . '.table',
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
        $writer = new TableWriter($this->getStagingFactory($clientWrapper));

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

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        self::assertCount(1, $tables);
        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table', $downloadedFile, []);
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

    public function testWriteTableOutputMappingEmptySlice(): void
    {
        $this->initBucket('redshift');
        $root = $this->tmp->getTmpFolder();
        mkdir($root . '/upload/table');
        file_put_contents($root . '/upload/table/part1', '');
        $configs = [
            [
                'source' => 'table',
                'destination' => self::OUTPUT_BUCKET . '.table',
                'columns' => ['Id', 'Name'],
            ],
        ];

        $writer = new TableWriter($this->getStagingFactory());
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

        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv((string) file_get_contents($downloadedFile));
        self::assertCount(0, $table);
    }

    public function testWriteTableOutputMappingEmptySliceExistingTable(): void
    {
        $this->initBucket('redshift');
        $fileName = $this->tmp->getTmpFolder() . uniqid('csv-');
        file_put_contents($fileName, "\"Id\",\"Name\"\n\"ab\",\"cd\"\n");
        $csv = new CsvFile($fileName);
        $this->clientWrapper->getBasicClient()->createTable(self::OUTPUT_BUCKET, 'table16', $csv);

        $root = $this->tmp->getTmpFolder();
        mkdir($root . '/upload/table16');
        file_put_contents($root . '/upload/table16/part1', '');
        $configs = [
            [
                'source' => 'table16',
                'destination' => self::OUTPUT_BUCKET . '.table16',
                'columns' => ['Id', 'Name'],
            ],
        ];

        $writer = new TableWriter($this->getStagingFactory());
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

        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . '.table16');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table16', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv((string) file_get_contents($downloadedFile));
        self::assertCount(0, $table);
    }

    public function testWriteTableOutputMappingEmptyDir(): void
    {
        $this->initBucket('redshift');
        $root = $this->tmp->getTmpFolder();
        mkdir($root . '/upload/table15');

        $configs = [
            [
                'source' => 'table15',
                'destination' => self::OUTPUT_BUCKET . '.table15',
                'columns' => ['Id', 'Name'],
            ],
        ];

        $writer = new TableWriter($this->getStagingFactory());
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

        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . '.table15');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table15', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv((string) file_get_contents($downloadedFile));
        self::assertCount(0, $table);
    }

    public function testWriteTableOutputMappingEmptyDirExistingTable(): void
    {
        $this->initBucket('redshift');
        $fileName = $this->tmp->getTmpFolder() . uniqid('csv-');
        file_put_contents($fileName, "\"Id\",\"Name\"\n\"ab\",\"cd\"\n");
        $csv = new CsvFile($fileName);
        $this->clientWrapper->getBasicClient()->createTable(self::OUTPUT_BUCKET, 'table17', $csv);

        $root = $this->tmp->getTmpFolder();
        mkdir($root . '/upload/table17');

        $configs = [
            [
                'source' => 'table17',
                'destination' => self::OUTPUT_BUCKET . '.table17',
                'columns' => ['Id', 'Name'],
            ],
        ];

        $writer = new TableWriter($this->getStagingFactory());
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

        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . '.table17');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table17', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv((string) file_get_contents($downloadedFile));
        self::assertCount(0, $table);
    }

    public function testWriteTableOutputMappingMissingHeaders(): void
    {
        $this->initBucket('redshift');
        $root = $this->tmp->getTmpFolder();
        mkdir($root . '/upload/table');

        $configs = [
            [
                'source' => 'table',
                'destination' => self::OUTPUT_BUCKET . '.table',
            ],
        ];

        $writer = new TableWriter($this->getStagingFactory());
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

    public function testWriteTableOutputMappingExistingTable(): void
    {
        $this->initBucket('redshift');
        $csvFile = new CsvFile($this->tmp->createFile('header')->getPathname());
        $csvFile->writeRow(['Id', 'Name']);
        $this->clientWrapper->getBasicClient()->createTable(self::OUTPUT_BUCKET, 'table', $csvFile);
        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        self::assertCount(1, $tables);
        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $root = $this->tmp->getTmpFolder();
        mkdir($root . '/upload/table.csv');
        file_put_contents($root . '/upload/table.csv/part1', "\"test\",\"test\"\n");
        file_put_contents($root . '/upload/table.csv/part2', "\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                'source' => 'table.csv',
                'destination' => self::OUTPUT_BUCKET . '.table',
                'columns' => ['Id', 'Name'],
            ],
        ];

        $writer = new TableWriter($this->getStagingFactory());

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

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        self::assertCount(1, $tables);
        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv((string) file_get_contents($downloadedFile));
        self::assertCount(2, $table);
        self::assertContains(['Id' => 'test', 'Name' => 'test'], $table);
        self::assertContains(['Id' => 'aabb', 'Name' => 'ccdd'], $table);
    }

    public function testWriteTableOutputMappingDifferentDelimiterEnclosure(): void
    {
        $this->initBucket('redshift');
        $root = $this->tmp->getTmpFolder();
        mkdir($root . '/upload/table.csv');
        file_put_contents($root . '/upload/table.csv/part1', "'test'|'test'\n");
        file_put_contents($root . '/upload/table.csv/part2', "'aabb'|'ccdd'\n");

        $configs = [
            [
                'source' => 'table.csv',
                'destination' => self::OUTPUT_BUCKET . '.table',
                'columns' => ['Id', 'Name'],
                'delimiter' => '|',
                'enclosure' => "'",
            ],
        ];

        $writer = new TableWriter($this->getStagingFactory());

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

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        self::assertCount(1, $tables);
        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv((string) file_get_contents($downloadedFile));
        self::assertCount(2, $table);
        self::assertContains(['Id' => 'test', 'Name' => 'test'], $table);
        self::assertContains(['Id' => 'aabb', 'Name' => 'ccdd'], $table);
    }

    public function testWriteTableOutputMappingCombination(): void
    {
        $this->initBucket('redshift');
        $root = $this->tmp->getTmpFolder();
        mkdir($root . '/upload/table.csv');
        file_put_contents($root . '/upload/table.csv/part1', "\"test\",\"test\"\n");
        file_put_contents($root . '/upload/table.csv/part2', "\"aabb\",\"ccdd\"\n");
        file_put_contents($root . '/upload/table2.csv', "\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                'source' => 'table.csv',
                'destination' => self::OUTPUT_BUCKET . '.table',
                'columns' => ['Id', 'Name'],
            ],
            [
                'source' => 'table2.csv',
                'destination' => self::OUTPUT_BUCKET . '.table2',
                'columns' => ['Id', 'Name'],
            ],
        ];

        $writer = new TableWriter($this->getStagingFactory());
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

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        self::assertCount(2, $tables);
        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);
        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . '.table2');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv((string) file_get_contents($downloadedFile));
        self::assertCount(2, $table);
        self::assertContains(['Id' => 'test', 'Name' => 'test'], $table);
        self::assertContains(['Id' => 'aabb', 'Name' => 'ccdd'], $table);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table2', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv((string) file_get_contents($downloadedFile));
        self::assertCount(2, $table);
        self::assertContains(['Id' => 'test', 'Name' => 'test'], $table);
        self::assertContains(['Id' => 'aabb', 'Name' => 'ccdd'], $table);
    }

    public function testWriteTableOutputMappingCompression(): void
    {
        $this->initBucket('redshift');
        $root = $this->tmp->getTmpFolder();
        mkdir($root . '/upload/table18.csv');
        file_put_contents($root . '/upload/table18.csv/part1', "\"test\",\"test\"\n");
        file_put_contents($root . '/upload/table18.csv/part2', "\"aabb\",\"ccdd\"\n");
        exec('gzip ' . $root . '/upload/table18.csv/part1');
        exec('gzip ' . $root . '/upload/table18.csv/part2');

        $configs = [
            [
                'source' => 'table18.csv',
                'destination' => self::OUTPUT_BUCKET . '.table18',
                'columns' => ['Id', 'Name'],
            ],
        ];

        $writer = new TableWriter($this->getStagingFactory());

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

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        self::assertCount(1, $tables);
        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . '.table18');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table18', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv((string) file_get_contents($downloadedFile));
        self::assertCount(2, $table);
        self::assertContains(['Id' => 'test', 'Name' => 'test'], $table);
        self::assertContains(['Id' => 'aabb', 'Name' => 'ccdd'], $table);
    }
}
