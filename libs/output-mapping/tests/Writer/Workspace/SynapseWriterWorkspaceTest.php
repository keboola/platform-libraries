<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Workspace;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\InitSynapseStorageClientTrait;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyInputBucket;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\OutputMapping\Writer\TableWriter;

class SynapseWriterWorkspaceTest extends AbstractTestCase
{
    use InitSynapseStorageClientTrait;

    protected function initClient(?string $branchId = null): void
    {
        $this->clientWrapper = $this->getSynapseClientWrapper();
    }

    #[NeedsEmptyOutputBucket]
    #[NeedsEmptyInputBucket]
    public function testSynapseTableOutputMapping(): void
    {
        // snowflake bucket does not work - https://keboola.atlassian.net/browse/KBC-228
        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload.csv');
        $csv->writeRow(['Id', 'Name', 'foo', 'bar']);
        $csv->writeRow(['id1', 'name1', 'foo1', 'bar1']);
        $csv->writeRow(['id2', 'name2', 'foo2', 'bar2']);
        $csv->writeRow(['id3', 'name3', 'foo3', 'bar3']);

        $tableIds = [];
        // Create table
        for ($i = 0; $i < 2; $i++) {
            $tableIds[$i] = $this->clientWrapper->getTableAndFileStorageClient()->createTableAsync(
                $this->emptyInputBucketId,
                'test' . ($i + 1),
                $csv,
            );
        }

        $factory = $this->getWorkspaceStagingFactory(
            null,
            'json',
            null,
            [AbstractStrategyFactory::WORKSPACE_SYNAPSE, 'synapse'],
        );
        // initialize the workspace mock
        $factory->getTableOutputStrategy(AbstractStrategyFactory::WORKSPACE_SYNAPSE)
            ->getDataStorage()->getWorkspaceId();
        $root = $this->temp->getTmpFolder();
        $this->prepareWorkspaceWithTables($this->emptyInputBucketId);
        $configs = [
            [
                'source' => 'table1a',
                'destination' => $this->emptyOutputBucketId . '.table1a',
                'distribution_key' => [],
            ],
            [
                'source' => 'table2a',
                'destination' => $this->emptyOutputBucketId . '.table2a',
                'distribution_key' => ['Id'],
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
            AbstractStrategyFactory::WORKSPACE_SYNAPSE,
            false,
            'none',
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(2, $jobIds);

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(2, $tables);
        $sortedTables = [$tables[0]['id'] => $tables[0], $tables[1]['id'] => $tables[1]];
        ksort($sortedTables);
        self::assertEquals(
            [$this->emptyOutputBucketId . '.table1a', $this->emptyOutputBucketId . '.table2a'],
            array_keys($sortedTables),
        );
        self::assertArrayHasKey('distributionKey', $sortedTables[$this->emptyOutputBucketId . '.table2a']);
        self::assertEquals(['Id'], $sortedTables[$this->emptyOutputBucketId . '.table2a']['distributionKey']);
        self::assertCount(2, $jobIds);
        self::assertNotEmpty($jobIds[0]);
        self::assertNotEmpty($jobIds[1]);
        $this->assertTableRowsEquals(
            $this->emptyOutputBucketId . '.table1a',
            [
                '"id","name","foo","bar"',
                '"id1","name1","foo1","bar1"',
                '"id2","name2","foo2","bar2"',
                '"id3","name3","foo3","bar3"',
            ],
        );
    }
}
