<?php

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Reader\NullWorkspaceProvider;
use Keboola\InputMapping\Reader\WorkspaceProviderInterface;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Workspaces;
use Keboola\Temp\Temp;
use Psr\Log\NullLogger;

class AbsWriterWorkspaceTest extends BaseWriterWorkspaceTest
{
    public function testAbsTableOutputMapping()
    {
        $root = $this->tmp->getTmpFolder();
        $this->prepareWorkspaceWithTables('abs');

        $configs = [
            [
                'source' => 'table1a',
                'destination' => 'out.c-output-mapping-test.table1a',
                'incremental' => true,
                'columns' => ['Id'],
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
        $writer = new TableWriter($this->client, new NullLogger(), $this->getWorkspaceProvider());

        $tableQueue = $writer->uploadTables(
            $root,
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'workspace-abs'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(2, $jobIds);

        $tables = $this->client->listTables('out.c-output-mapping-test');
        $this->assertCount(2, $tables);
        $tableIds = [$tables[0]['id'], $tables[1]['id']];
        sort($tableIds);
        $this->assertEquals(['out.c-output-mapping-test.table1a', 'out.c-output-mapping-test.table2a'], $tableIds);
        $this->assertCount(2, $jobIds);
        $this->assertNotEmpty($jobIds[0]);
        $this->assertNotEmpty($jobIds[1]);
        $job = $this->client->getJob($jobIds[0]);
        $this->assertEquals('out.c-output-mapping-test.table1a', $job['tableId']);
        $this->assertEquals(true, $job['operationParams']['params']['incremental']);
        $this->assertEquals(['Id'], $job['operationParams']['params']['columns']);
        $data = $this->client->getTableDataPreview('out.c-output-mapping-test.table1a');
        $job = $this->client->getJob($jobIds[1]);
        $this->assertEquals('out.c-output-mapping-test.table2a', $job['tableId']);
        $this->assertEquals(false, $job['operationParams']['params']['incremental']);
        $this->assertEquals(['Id2', 'Name2'], $job['operationParams']['params']['columns']);

        $rows = explode("\n", trim($data));
        sort($rows);
        // convert to lowercase because of https://keboola.atlassian.net/browse/KBC-864
        $rows = array_map(
            'strtolower',
            $rows
        );
        // Both id and name columns are present because of https://keboola.atlassian.net/browse/KBC-865
        $this->assertEquals(['"id","name"', '"aabb","ccdd"', '"test","test"'], $rows);
    }
}
