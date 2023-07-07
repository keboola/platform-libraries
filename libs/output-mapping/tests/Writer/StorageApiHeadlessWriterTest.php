<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\TableExporter;

class StorageApiHeadlessWriterTest extends AbstractTestCase
{
    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMapping(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/table.csv', "\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

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
        self::assertEquals(1, $tableQueue->getTaskCount());

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->emptyOutputBucketId . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $downloadedFile = $root . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyOutputBucketId . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->parseCsv(
            (string) file_get_contents($downloadedFile)
        );
        self::assertCount(2, $table);
        self::assertContains(['Id' => 'test', 'Name' => 'test'], $table);
        self::assertContains(['Id' => 'aabb', 'Name' => 'ccdd'], $table);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingEmptyFile(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/table', '');

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
    public function testWriteTableOutputMappingAndManifest(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . 'upload/table2.csv',
            "\"test\",\"test\"\n"
        );
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . 'upload/table2.csv.manifest',
            '{"destination": "' . $this->emptyOutputBucketId . '.table2","primary_key":["Id"],"columns":["a","b"]}'
        );

        $configs = [
            [
                'source' => 'table2.csv',
                'destination' => $this->emptyOutputBucketId . '.table',
                'columns' => ['Id', 'Name'],
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
        self::assertEquals($this->emptyOutputBucketId . '.table', $tables[0]['id']);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->emptyOutputBucketId . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyOutputBucketId . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->parseCsv(
            (string) file_get_contents($downloadedFile)
        );
        self::assertCount(1, $table);
        self::assertEquals([['Id' => 'test', 'Name' => 'test']], $table);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableManifest(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . 'upload/' . $this->emptyOutputBucketId . '.table.csv',
            "\"test\",\"test\"\n"
        );
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . 'upload/' . $this->emptyOutputBucketId . '.table.csv.manifest',
            '{"destination": "' . $this->emptyOutputBucketId .
            '.table","primary_key":["Id","Name"],"columns":["Id","Name"]}'
        );

        $writer = new TableWriter($this->getWorkspaceStagingFactory());
        $tableQueue =  $writer->uploadTables(
            'upload',
            [],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::LOCAL,
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);
        self::assertEquals($this->emptyOutputBucketId . '.table', $tables[0]['id']);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->emptyOutputBucketId . '.table');
        self::assertEquals(['Id', 'Name'], $table['primaryKey']);
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $downloadedFile = $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyOutputBucketId . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->parseCsv(
            (string) file_get_contents($downloadedFile)
        );
        self::assertCount(1, $table);
        self::assertEquals([['Id' => 'test', 'Name' => 'test']], $table);
    }

    #[NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingExistingTable(): void
    {
        $csvFile = new CsvFile($this->temp->createFile('header')->getPathname());
        $csvFile->writeRow(['Id', 'Name']);
        $this->clientWrapper->getTableAndFileStorageClient()->createTable(
            $this->emptyOutputBucketId,
            'table',
            $csvFile
        );
        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);
        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->emptyOutputBucketId . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/table.csv', "\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

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
}
