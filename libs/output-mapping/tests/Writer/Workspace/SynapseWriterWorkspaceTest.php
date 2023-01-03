<?php

namespace Keboola\OutputMapping\Tests\Writer\Workspace;

use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Tests\InitSynapseStorageClientTrait;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\TableExporter;

class SynapseWriterWorkspaceTest extends BaseWriterWorkspaceTest
{
    use InitSynapseStorageClientTrait;

    private const INPUT_BUCKET = 'in.c-SynapseWriterWorkspaceTest';
    private const OUTPUT_BUCKET = 'out.c-SynapseWriterWorkspaceTest';

    public function setUp(): void
    {
        if (!$this->checkSynapseTests()) {
            self::markTestSkipped('Synapse tests disabled.');
        }
        parent::setUp();
        $this->clearBuckets([
            self::INPUT_BUCKET,
            self::OUTPUT_BUCKET,
        ]);
    }

    protected function initClient($branchId = '')
    {
        $this->clientWrapper = $this->getSynapseClientWrapper();
    }

    public function testSynapseTableOutputMapping()
    {
        $factory = $this->getStagingFactory(null, 'json', null, [StrategyFactory::WORKSPACE_SYNAPSE, 'synapse']);
        // initialize the workspace mock
        $factory->getTableOutputStrategy(StrategyFactory::WORKSPACE_SYNAPSE)->getDataStorage()->getWorkspaceId();
        $root = $this->tmp->getTmpFolder();
        $this->prepareWorkspaceWithTables('synapse', 'SynapseWriterWorkspaceTest');
        // snowflake bucket does not work - https://keboola.atlassian.net/browse/KBC-228
        $this->clientWrapper->getBasicClient()->createBucket('SynapseWriterWorkspaceTest', 'out', '', 'synapse');
        $configs = [
            [
                'source' => 'table1a',
                'destination' => self::OUTPUT_BUCKET . '.table1a',
                'distribution_key' => [],
            ],
            [
                'source' => 'table2a',
                'destination' => self::OUTPUT_BUCKET . '.table2a',
                'distribution_key' => ['Id2'],
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
                ['columns' => ['Id2', 'Name2']]
            )
        );
        $writer = new TableWriter($factory);

        $tableQueue = $writer->uploadTables(
            '/',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            StrategyFactory::WORKSPACE_SYNAPSE
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(2, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        $this->assertCount(2, $tables);
        $sortedTables = [$tables[0]['id'] => $tables[0], $tables[1]['id'] => $tables[1]];
        ksort($sortedTables);
        $this->assertEquals(
            [self::OUTPUT_BUCKET . '.table1a', self::OUTPUT_BUCKET . '.table2a'],
            array_keys($sortedTables)
        );
        $this->assertArrayHasKey('distributionKey', $sortedTables[self::OUTPUT_BUCKET . '.table2a']);
        $this->assertEquals(['Id2'], $sortedTables[self::OUTPUT_BUCKET . '.table2a']['distributionKey']);
        $this->assertCount(2, $jobIds);
        $this->assertNotEmpty($jobIds[0]);
        $this->assertNotEmpty($jobIds[1]);

        $te = new TableExporter($this->clientWrapper->getBasicClient());
        $te->exportTable(self::OUTPUT_BUCKET . '.table1a', $root . DIRECTORY_SEPARATOR . 'table1a-returned.csv', []);
        $rows = explode("\n", trim(file_get_contents($root . DIRECTORY_SEPARATOR . 'table1a-returned.csv')));
        sort($rows);
        $this->assertEquals(['"Id","Name"', '"aabb","ccdd"', '"test","test"'], $rows);
    }
}
