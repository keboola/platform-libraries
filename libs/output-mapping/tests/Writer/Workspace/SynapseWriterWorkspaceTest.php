<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Workspace;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\InitSynapseStorageClientTrait;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyInputBucket;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\OutputMapping\Tests\Needs\NeedsTestTables;
use Keboola\OutputMapping\Tests\Needs\TestSatisfyer;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\TableExporter;

class SynapseWriterWorkspaceTest extends AbstractTestCase
{
    use InitSynapseStorageClientTrait;

    public function setUp(): void
    {
        if (!$this->checkSynapseTests()) {
            self::markTestSkipped('Synapse tests disabled.');
        }
        parent::setUp();
    }

    protected function initClient(?string $branchId = null): void
    {
        $this->clientWrapper = $this->getSynapseClientWrapper();
    }

    #[NeedsEmptyOutputBucket]
    public function testSynapseTableOutputMapping(): void
    {
        // snowflake bucket does not work - https://keboola.atlassian.net/browse/KBC-228
        $bucketName = 'testSynapseTableOutputMapping';

        $outBucketId = TestSatisfyer::getBucketIdByDisplayName($this->clientWrapper, $bucketName, Client::STAGE_OUT);
        if ($outBucketId !== null) {
            $tables = $this->clientWrapper->getBasicClient()->listTables($outBucketId, ['include' => '']);
            foreach ($tables as $table) {
                $this->clientWrapper->getBasicClient()->dropTable($table['id']);
            }
        } else {
            $outBucketId  = $this->clientWrapper->getBasicClient()->createBucket(
                name: $bucketName,
                stage: Client::STAGE_OUT,
                backend: 'synapse'
            );
        }

        $bucketId = TestSatisfyer::getBucketIdByDisplayName($this->clientWrapper, $bucketName, Client::STAGE_IN);
        if ($bucketId !== null) {
            $tables = $this->clientWrapper->getBasicClient()->listTables($bucketId, ['include' => '']);
            foreach ($tables as $table) {
                $this->clientWrapper->getBasicClient()->dropTable($table['id']);
            }
        } else {
            $bucketId  = $this->clientWrapper->getBasicClient()->createBucket(
                name: $bucketName,
                stage: Client::STAGE_IN,
                backend: 'synapse'
            );
        }

        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload.csv');
        $csv->writeRow(['Id', 'Name', 'foo', 'bar']);
        $csv->writeRow(['id1', 'name1', 'foo1', 'bar1']);
        $csv->writeRow(['id2', 'name2', 'foo2', 'bar2']);
        $csv->writeRow(['id3', 'name3', 'foo3', 'bar3']);

        $tableIds = [];
        // Create table
        for ($i = 0; $i < 2; $i++) {
            $tableIds[$i] = $this->clientWrapper->getBasicClient()->createTableAsync(
                $bucketId,
                'test' . ($i + 1),
                $csv
            );
        }

        $factory = $this->getWorkspaceStagingFactory(
            null,
            'json',
            null,
            [AbstractStrategyFactory::WORKSPACE_SYNAPSE, 'synapse']
        );
        // initialize the workspace mock
        $factory->getTableOutputStrategy(AbstractStrategyFactory::WORKSPACE_SYNAPSE)
            ->getDataStorage()->getWorkspaceId();
        $root = $this->temp->getTmpFolder();
        $this->prepareWorkspaceWithTables($bucketId);
        $configs = [
            [
                'source' => 'table1a',
                'destination' => $outBucketId . '.table1a',
                'distribution_key' => [],
            ],
            [
                'source' => 'table2a',
                'destination' => $outBucketId . '.table2a',
                'distribution_key' => ['Id'],
            ],
        ];
        file_put_contents(
            $root . '/table1a.manifest',
            json_encode(
                ['columns' => ['Id', 'Name']]
            )
        );
        file_put_contents(
            $root . '/table2a.manifest',
            json_encode(
                ['columns' => ['Id', 'Name']]
            )
        );
        $writer = new TableWriter($factory);

        $tableQueue = $writer->uploadTables(
            '/',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::WORKSPACE_SYNAPSE,
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(2, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables($outBucketId);
        self::assertCount(2, $tables);
        $sortedTables = [$tables[0]['id'] => $tables[0], $tables[1]['id'] => $tables[1]];
        ksort($sortedTables);
        self::assertEquals(
            [$outBucketId . '.table1a', $outBucketId . '.table2a'],
            array_keys($sortedTables)
        );
        self::assertArrayHasKey('distributionKey', $sortedTables[$outBucketId . '.table2a']);
        self::assertEquals(['Id'], $sortedTables[$outBucketId . '.table2a']['distributionKey']);
        self::assertCount(2, $jobIds);
        self::assertNotEmpty($jobIds[0]);
        self::assertNotEmpty($jobIds[1]);
        self::assertTableRowsEquals(
            $outBucketId . '.table1a',
            [
                '"id","name","foo","bar"',
                '"id1","name1","foo1","bar1"',
                '"id2","name2","foo2","bar2"',
                '"id3","name3","foo3","bar3"',
            ]
        );
    }
}
