<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\TableExporter;

class StorageApiHeadlessWriterTest extends BaseWriterTest
{
    private const FILE_TAG = 'StorageApiHeadlessWriterTest';
    private const OUTPUT_BUCKET = 'out.c-StorageApiHeadlessWriterTest';

    public function setUp(): void
    {
        parent::setUp();
        $this->clearBuckets([self::OUTPUT_BUCKET]);
        $this->clearFileUploads([self::FILE_TAG]);
        $this->clientWrapper->getBasicClient()->createBucket('StorageApiHeadlessWriterTest', 'out');
    }

    public function testWriteTableOutputMapping(): void
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . '/upload/table.csv', "\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                'source' => 'table.csv',
                'destination' => self::OUTPUT_BUCKET . '.table',
                'columns' => ['Id','Name'],
            ],
        ];

        $writer = new TableWriter($this->getStagingFactory());
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

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        self::assertCount(1, $tables);
        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $root . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv((string) file_get_contents($downloadedFile));
        self::assertCount(2, $table);
        self::assertContains(['Id' => 'test', 'Name' => 'test'], $table);
        self::assertContains(['Id' => 'aabb', 'Name' => 'ccdd'], $table);
    }

    public function testWriteTableOutputMappingEmptyFile(): void
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . '/upload/table', '');

        $configs = [
            [
                'source' => 'table',
                'destination' => self::OUTPUT_BUCKET . '.table',
                'columns' => ['Id','Name'],
            ],
        ];

        $writer = new TableWriter($this->getStagingFactory());
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

        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv((string) file_get_contents($downloadedFile));
        self::assertCount(0, $table);
    }

    public function testWriteTableOutputMappingAndManifest(): void
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . 'upload/table2.csv',
            "\"test\",\"test\"\n"
        );
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . 'upload/table2.csv.manifest',
            '{"destination": "' . self::OUTPUT_BUCKET . '.table2","primary_key":["Id"],"columns":["a","b"]}'
        );

        $configs = [
            [
                'source' => 'table2.csv',
                'destination' => self::OUTPUT_BUCKET . '.table',
                'columns' => ['Id', 'Name'],
            ],
        ];

        $writer = new TableWriter($this->getStagingFactory());
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

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        self::assertCount(1, $tables);
        self::assertEquals(self::OUTPUT_BUCKET . '.table', $tables[0]['id']);
        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv((string) file_get_contents($downloadedFile));
        self::assertCount(1, $table);
        self::assertEquals([['Id' => 'test', 'Name' => 'test']], $table);
    }

    public function testWriteTableManifest(): void
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . 'upload/' . self::OUTPUT_BUCKET . '.table.csv',
            "\"test\",\"test\"\n"
        );
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . 'upload/' . self::OUTPUT_BUCKET . '.table.csv.manifest',
            '{"destination": "' . self::OUTPUT_BUCKET . '.table","primary_key":["Id","Name"],"columns":["Id","Name"]}'
        );

        $writer = new TableWriter($this->getStagingFactory());
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

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        self::assertCount(1, $tables);
        self::assertEquals(self::OUTPUT_BUCKET . '.table', $tables[0]['id']);
        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . '.table');
        self::assertEquals(['Id', 'Name'], $table['primaryKey']);
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $this->tmp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv((string) file_get_contents($downloadedFile));
        self::assertCount(1, $table);
        self::assertEquals([['Id' => 'test', 'Name' => 'test']], $table);
    }

    public function testWriteTableOutputMappingExistingTable(): void
    {
        $csvFile = new CsvFile($this->tmp->createFile('header')->getPathname());
        $csvFile->writeRow(['Id', 'Name']);
        $this->clientWrapper->getBasicClient()->createTable(self::OUTPUT_BUCKET, 'table', $csvFile);
        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        self::assertCount(1, $tables);
        $table = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . '.table');
        self::assertEquals(['Id', 'Name'], $table['columns']);

        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . '/upload/table.csv', "\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                'source' => 'table.csv',
                'destination' => self::OUTPUT_BUCKET . '.table',
                'columns' => ['Id','Name'],
            ],
        ];

        $writer = new TableWriter($this->getStagingFactory());
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
}
