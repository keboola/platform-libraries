<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Redshift;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyRedshiftInputBucket;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyRedshiftOutputBucket;
use Keboola\OutputMapping\Tests\Writer\CreateBranchTrait;
use Keboola\OutputMapping\Writer\TableWriter;

class RedshiftWriterWorkspaceTest extends AbstractTestCase
{
    use CreateBranchTrait;

    public function setUp(): void
    {
        parent::setUp();
    }

    #[NeedsEmptyRedshiftInputBucket, NeedsEmptyRedshiftOutputBucket]
    public function testRedshiftTableOutputMapping(): void
    {
        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload.csv');
        $csv->writeRow(['Id', 'Name', 'foo', 'bar']);
        $csv->writeRow(['id1', 'name1', 'foo1', 'bar1']);
        $csv->writeRow(['id2', 'name2', 'foo2', 'bar2']);
        $csv->writeRow(['id3', 'name3', 'foo3', 'bar3']);

        $tableIds = [];
        // Create table
        for ($i = 0; $i < 2; $i++) {
            $tableIds[$i] = $this->clientWrapper->getTableAndFileStorageClient()->createTableAsync(
                $this->emptyRedshiftInputBucketId,
                'test' . ($i + 1),
                $csv,
            );
        }

        $factory = $this->getWorkspaceStagingFactory(
            null,
            'json',
            null,
            [AbstractStrategyFactory::WORKSPACE_REDSHIFT, 'redshift'],
        );
        // initialize the workspace mock
        $factory->getTableOutputStrategy(AbstractStrategyFactory::WORKSPACE_REDSHIFT)
            ->getDataStorage()->getWorkspaceId();

        $root = $this->temp->getTmpFolder();
        $this->prepareWorkspaceWithTables($this->emptyRedshiftInputBucketId);
        $configs = [
            [
                'source' => 'table1a',
                'destination' => $this->emptyRedshiftOutputBucketId . '.table1a',
            ],
            [
                'source' => 'table2a',
                'destination' => $this->emptyRedshiftOutputBucketId . '.table2a',
            ],
        ];
        file_put_contents(
            $root . '/table1a.manifest',
            json_encode(
                ['columns' => ['Id', 'Name']],
            ),
        );
        file_put_contents(
            $root . '/table2a.manifest',
            json_encode(
                ['columns' => ['Id', 'Name']],
            ),
        );
        $writer = new TableWriter($factory);

        $tableQueue = $writer->uploadTables(
            '/',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::WORKSPACE_REDSHIFT,
            false,
            'none',
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(2, $jobIds);
        self::assertNotEmpty($jobIds[0]);
        self::assertNotEmpty($jobIds[1]);

        self::assertTablesExists(
            $this->emptyRedshiftOutputBucketId,
            [
                $this->emptyRedshiftOutputBucketId . '.table1a',
                $this->emptyRedshiftOutputBucketId . '.table2a',
            ],
        );
        self::assertTableRowsEquals($this->emptyRedshiftOutputBucketId . '.table1a', [
            '"id","name","foo","bar"',
            '"id1","name1","foo1","bar1"',
            '"id2","name2","foo2","bar2"',
            '"id3","name3","foo3","bar3"',
        ]);
    }
}
