<?php

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\InputMapping\Reader;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\OutputMapping\Tests\InitSynapseStorageClientTrait;
use Keboola\OutputMapping\Writer\TableWriter;
use Psr\Log\NullLogger;

class SynapseWriterWorkspaceTest extends BaseWriterWorkspaceTest
{
    use InitSynapseStorageClientTrait;

    public function setUp()
    {
        if (!$this->checkSynapseTests()) {
            self::markTestSkipped('Synapse tests disabled.');
        }
        parent::setUp();
        $this->clearBuckets([
            'in.c-output-mapping-test',
            'out.c-output-mapping-test',
        ]);
    }

    protected function initClient()
    {
        $this->clientWrapper = $this->getSynapseClientWrapper();
    }

    public function testSynapseTableOutputMapping()
    {
        $root = $this->tmp->getTmpFolder();
        $this->prepareWorkspaceWithTables('synapse');
        // snowflake bucket does not work - https://keboola.atlassian.net/browse/KBC-228
        $this->clientWrapper->getBasicClient()->createBucket('output-mapping-test', 'out', '', 'synapse');
        $configs = [
            [
                'source' => 'table1a',
                'destination' => 'out.c-output-mapping-test.table1a',
            ],
            [
                'source' => 'table2a',
                'destination' => 'out.c-output-mapping-test.table2a',
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
        $writer = new TableWriter($this->clientWrapper, new NullLogger(), $this->getWorkspaceProvider());

        $tableQueue = $writer->uploadTables(
            $root,
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'workspace-synapse'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(2, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables('out.c-output-mapping-test');
        $this->assertCount(2, $tables);
        $tableIds = [$tables[0]['id'], $tables[1]['id']];
        sort($tableIds);
        $this->assertEquals(['out.c-output-mapping-test.table1a', 'out.c-output-mapping-test.table2a'], $tableIds);
        $this->assertCount(2, $jobIds);
        $this->assertNotEmpty($jobIds[0]);
        $this->assertNotEmpty($jobIds[1]);

        $reader = new Reader($this->clientWrapper, new NullLogger(), $this->getWorkspaceProvider());
        $reader->downloadTables(
            new InputTableOptionsList([
                [
                    'source' => 'out.c-output-mapping-test.table1a',
                    'destination' => 'table1a-returned.csv',
                ]
            ]),
            new InputTableStateList([]),
            $root,
            Reader::STAGING_LOCAL
        );
        $rows = explode("\n", trim(file_get_contents($root . DIRECTORY_SEPARATOR . 'table1a-returned.csv')));
        sort($rows);
        $this->assertEquals(['"Id","Name"', '"aabb","ccdd"', '"test","test"'], $rows);
    }
}
