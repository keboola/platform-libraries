<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Redshift;

use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyRedshiftOutputBucket;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\TableExporter;

class StorageApiLocalTableWriterRedshiftTest extends AbstractTestCase
{
    #[NeedsEmptyRedshiftOutputBucket]
    public function testWriteTableManifestCsvRedshift(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . 'upload/' . $this->emptyRedshiftOutputBucketId . '.table3d.csv',
            "'Id'\t'Name'\n'test'\t'test''s'\n",
        );
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . 'upload/' . $this->emptyRedshiftOutputBucketId . '.table3d.csv.manifest',
            '{"destination": "' . $this->emptyRedshiftOutputBucketId . '.table3d","delimiter": "'
            . "\\t" . '","enclosure": "\'"}',
        );

        $writer = new TableWriter($this->getLocalStagingFactory());
        $tableQueue =  $writer->uploadTables(
            '/upload',
            [],
            ['componentId' => 'foo'],
            'local',
            false,
            false,
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyRedshiftOutputBucketId);
        self::assertCount(1, $tables);
        self::assertEquals($this->emptyRedshiftOutputBucketId . '.table3d', $tables[0]['id']);
        $exporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $downloadedFile = $root . DIRECTORY_SEPARATOR . 'download.csv';
        $exporter->exportTable($this->emptyRedshiftOutputBucketId . '.table3d', $downloadedFile, []);
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

    #[NeedsEmptyRedshiftOutputBucket]
    public function testWriteTableIncrementalWithDeleteRedshift(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/table61.csv',
            "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n",
        );

        $configs = [
            [
                'source' => 'table61.csv',
                'destination' => $this->emptyRedshiftOutputBucketId . '.table61',
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
            false,
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        // And again, check first incremental table
        $tableQueue =  $writer->uploadTables(
            '/upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'local',
            false,
            false,
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $exporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $exporter->exportTable(
            $this->emptyRedshiftOutputBucketId . '.table61',
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
}
