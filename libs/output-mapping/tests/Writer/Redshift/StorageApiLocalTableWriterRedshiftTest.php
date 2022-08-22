<?php

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\TableExporter;

class StorageApiLocalTableWriterRedshiftTest extends BaseWriterTest
{
    private const OUTPUT_BUCKET = 'out.c-StorageApiLocalTableWriterRedshiftTest';
    private const BRANCH_BUCKET = 'out.c-dev-123-StorageApiLocalTableWriterRedshiftTest';

    const DEFAULT_SYSTEM_METADATA = ['componentId' => 'foo'];

    public function setUp()
    {
        parent::setUp();
        $this->clearBuckets([
            self::OUTPUT_BUCKET,
            self::BRANCH_BUCKET
        ]);
        $this->clientWrapper->getBasicClient()->createBucket(self::class, 'out', '', 'redshift');
    }

    public function testWriteTableManifestCsvRedshift()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . 'upload/' . self::OUTPUT_BUCKET . '.table3d.csv',
            "'Id'\t'Name'\n'test'\t'test''s'\n"
        );
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . 'upload/' . self::OUTPUT_BUCKET . '.table3d.csv.manifest',
            '{"destination": "' . self::OUTPUT_BUCKET . '.table3d","delimiter": "' . "\t" . '","enclosure": "\'"}'
        );

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables('/upload', [], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        $this->assertCount(1, $tables);
        $this->assertEquals(self::OUTPUT_BUCKET . '.table3d', $tables[0]["id"]);
        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $root . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table3d', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv(file_get_contents($downloadedFile));
        $this->assertCount(1, $table);
        $this->assertCount(2, $table[0]);
        $this->assertArrayHasKey('Id', $table[0]);
        $this->assertArrayHasKey('Name', $table[0]);
        $this->assertEquals('test', $table[0]['Id']);
        $this->assertEquals('test\'s', $table[0]['Name']);
    }

    public function testWriteTableIncrementalWithDeleteRedshift()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table61.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                "source" => "table61.csv",
                "destination" => self::OUTPUT_BUCKET . ".table61",
                "delete_where_column" => "Id",
                "delete_where_values" => ["aabb"],
                "delete_where_operator" => "eq",
                "incremental" => true
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());

        $tableQueue =  $writer->uploadTables('/upload', ["mapping" => $configs], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        // And again, check first incremental table
        $tableQueue =  $writer->uploadTables('/upload', ["mapping" => $configs], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $exporter->exportTable(self::OUTPUT_BUCKET . ".table61", $root . DIRECTORY_SEPARATOR . "download.csv", []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv(file_get_contents($root . DIRECTORY_SEPARATOR . "download.csv"));
        usort($table, function ($a, $b) {
            return strcasecmp($a['Id'], $b['Id']);
        });
        $this->assertCount(3, $table);
        $this->assertCount(2, $table[0]);
        $this->assertArrayHasKey('Id', $table[0]);
        $this->assertArrayHasKey('Name', $table[0]);
        $this->assertEquals('aabb', $table[0]['Id']);
        $this->assertEquals('ccdd', $table[0]['Name']);
        $this->assertEquals('test', $table[1]['Id']);
        $this->assertEquals('test', $table[1]['Name']);
        $this->assertEquals('test', $table[2]['Id']);
        $this->assertEquals('test', $table[2]['Name']);
    }
}
