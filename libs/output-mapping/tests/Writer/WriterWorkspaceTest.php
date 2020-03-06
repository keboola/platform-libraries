<?php

namespace Keboola\OutputMapping\Tests;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Reader\NullWorkspaceProvider;
use Keboola\InputMapping\Reader\WorkspaceProviderInterface;
use Keboola\OutputMapping\Tests\Writer\BaseWriterTest;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\Workspaces;
use Keboola\Temp\Temp;
use Psr\Log\NullLogger;

class WriterWorkspaceTest extends BaseWriterTest
{
    private $workspaceId;

    public function setUp()
    {
        parent::setUp();
        $this->clearBuckets(['out.c-output-mapping-test', 'in.c-output-mapping-test']);
    }

    public function tearDown()
    {
        if ($this->workspaceId) {
            $workspaces = new Workspaces($this->client);
            $workspaces->deleteWorkspace($this->workspaceId);
            $this->workspaceId = null;
        }
        parent::tearDown();
    }

    private function getWorkspaceProvider()
    {
        $mock = self::getMockBuilder(NullWorkspaceProvider::class)
            ->setMethods(['getWorkspaceId'])
            ->getMock();
        $mock->method('getWorkspaceId')->willReturnCallback(
            function ($type) {
                if (!$this->workspaceId) {
                    $workspaces = new Workspaces($this->client);
                    $workspace = $workspaces->createWorkspace(['backend' => $type]);
                    $this->workspaceId = $workspace['id'];
                }
                return $this->workspaceId;
            }
        );
        /** @var WorkspaceProviderInterface $mock */
        return $mock;
    }

    private function prepareWorkspaceWithTables($type)
    {
        $temp = new Temp();
        $temp->initRunFolder();
        $root = $temp->getTmpFolder();
        $this->client->createBucket('output-mapping-test', 'in', '', $type);
        file_put_contents($root . "/table1a.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");
        file_put_contents($root . "/table2a.csv", "\"Id2\",\"Name2\"\n\"test2\",\"test2\"\n\"aabb2\",\"ccdd2\"\n");
        $this->client->createTable('in.c-output-mapping-test', 'table1a', new CsvFile($root . "/table1a.csv"));
        $this->client->createTable('in.c-output-mapping-test', 'table2a', new CsvFile($root . "/table2a.csv"));
        $workspaces = new Workspaces($this->client);
        $workspaceProvider = $this->getWorkspaceProvider();
        $workspaces->loadWorkspaceData(
            $workspaceProvider->getWorkspaceId($type),
            [
                'input' => [
                    [
                        'source' => 'in.c-output-mapping-test.table1a',
                        'destination' => 'table1a',
                    ],
                    [
                        'source' => 'in.c-output-mapping-test.table2a',
                        'destination' => 'table2a',
                    ]
                ]
            ]
        );
    }

    public function testSnowflakeTableOutputMapping()
    {
        $root = $this->tmp->getTmpFolder();
        $this->prepareWorkspaceWithTables('snowflake');

        $configs = [
            [
                "source" => "table1a",
                "destination" => "out.c-output-mapping-test.table1a"
            ],
            [
                "source" => "table2a",
                "destination" => "out.c-output-mapping-test.table2a"
            ]
        ];
        file_put_contents(
            $root . "/table1a.manifest",
            json_encode(
                ['columns' => ['Id', 'Name']]
            )
        );
        file_put_contents(
            $root . "/table2a.manifest",
            json_encode(
                ['columns' => ['Id2', 'Name2']]
            )
        );
        $writer = new TableWriter($this->client, new NullLogger(), $this->getWorkspaceProvider());

        $tableQueue = $writer->uploadTables($root, ["mapping" => $configs], ['componentId' => 'foo'], 'workspace-snowflake');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(2, $jobIds);

        $tables = $this->client->listTables("out.c-output-mapping-test");
        $this->assertCount(2, $tables);
        $tableIds = [$tables[0]["id"], $tables[1]["id"]];
        sort($tableIds);
        $this->assertEquals(['out.c-output-mapping-test.table1a', 'out.c-output-mapping-test.table2a'], $tableIds);
        $this->assertCount(2, $jobIds);
        $this->assertNotEmpty($jobIds[0]);
        $this->assertNotEmpty($jobIds[1]);
        $data = $this->client->getTableDataPreview('out.c-output-mapping-test.table1a');
        $rows = explode("\n", trim($data));
        sort($rows);
        $this->assertEquals(['"Id","Name"', '"aabb","ccdd"', '"test","test"'], $rows);
    }

    public function testRedshiftTableOutputMapping()
    {
        $root = $this->tmp->getTmpFolder();
        $this->prepareWorkspaceWithTables('redshift');
        // todo snowflake does not work
        $this->client->createBucket('output-mapping-test', 'out', '', 'redshift');
        $configs = [
            [
                "source" => "table1a",
                "destination" => "out.c-output-mapping-test.table1a"
            ],
            [
                "source" => "table2a",
                "destination" => "out.c-output-mapping-test.table2a"
            ]
        ];
        file_put_contents(
            $root . "/table1a.manifest",
            json_encode(
                ['columns' => ['Id', 'Name']]
            )
        );
        file_put_contents(
            $root . "/table2a.manifest",
            json_encode(
                ['columns' => ['Id2', 'Name2']]
            )
        );
        $writer = new TableWriter($this->client, new NullLogger(), $this->getWorkspaceProvider());

        $tableQueue = $writer->uploadTables($root , ["mapping" => $configs], ['componentId' => 'foo'], 'workspace-redshift');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(2, $jobIds);

        $tables = $this->client->listTables("out.c-output-mapping-test");
        $this->assertCount(2, $tables);
        $tableIds = [$tables[0]["id"], $tables[1]["id"]];
        sort($tableIds);
        $this->assertEquals(['out.c-output-mapping-test.table1a', 'out.c-output-mapping-test.table2a'], $tableIds);
        $this->assertCount(2, $jobIds);
        $this->assertNotEmpty($jobIds[0]);
        $this->assertNotEmpty($jobIds[1]);
        $data = $this->client->getTableDataPreview('out.c-output-mapping-test.table1a');
        $rows = explode("\n", trim($data));
        sort($rows);
        $this->assertEquals(['"Id","Name"', '"aabb","ccdd"', '"test","test"'], $rows);
    }
}
