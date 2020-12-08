<?php

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\NullLogger;

class WriterWorkspaceTest extends BaseWriterWorkspaceTest
{
    use CreateBranchTrait;

    public function setUp()
    {
        parent::setUp();
        $this->clearBuckets([
            'out.c-output-mapping-test',
            'in.c-output-mapping-test',
            'out.c-dev-123-output-mapping-test',
        ]);
        $this->clearFileUploads(['output-mapping-test']);
    }

    public function testSnowflakeTableOutputMapping()
    {
        $root = $this->tmp->getTmpFolder();
        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        // because of https://keboola.atlassian.net/browse/KBC-228 we need to use default backend (or create the
        // target bucket with the same backend)
        $this->prepareWorkspaceWithTables($tokenInfo['owner']['defaultBackend']);

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
        $writer = new TableWriter($this->clientWrapper, new NullLogger(), $this->getWorkspaceProvider());

        $tableQueue = $writer->uploadTables(
            $root,
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'workspace-snowflake'
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
        $job = $this->clientWrapper->getBasicClient()->getJob($jobIds[0]);
        $this->assertEquals('out.c-output-mapping-test.table1a', $job['tableId']);
        $this->assertEquals(true, $job['operationParams']['params']['incremental']);
        $this->assertEquals(['Id'], $job['operationParams']['params']['columns']);
        $data = $this->clientWrapper->getBasicClient()->getTableDataPreview('out.c-output-mapping-test.table1a');
        $job = $this->clientWrapper->getBasicClient()->getJob($jobIds[1]);
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

    public function testTableOutputMappingMissing()
    {
        self::markTestSkipped('Works, but takes ages https://keboola.atlassian.net/browse/KBC-34');
        $root = $this->tmp->getTmpFolder();
        $configs = [
            [
                'source' => 'table1a',
                'destination' => 'out.c-output-mapping-test.table1a',
            ],
        ];
        file_put_contents(
            $root . '/table1a.manifest',
            json_encode(
                ['columns' => ['Id', 'Name']]
            )
        );
        $writer = new TableWriter($this->clientWrapper, new NullLogger(), $this->getWorkspaceProvider());
        $writer->uploadTables(
            $root,
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'workspace-snowflake'
        );
        // fix exception message when https://keboola.atlassian.net/browse/KBC-34 is resolved
        // self::expectExceptionMessage('foo');
        self::expectException(InvalidOutputException::class);
    }

    public function testTableOutputMappingMissingManifest()
    {
        $root = $this->tmp->getTmpFolder();
        $configs = [
            [
                'source' => 'table1a',
                'destination' => 'out.c-output-mapping-test.table1a',
            ],
        ];
        $writer = new TableWriter($this->clientWrapper, new NullLogger(), $this->getWorkspaceProvider());
        self::expectException(InvalidOutputException::class);
        self::expectExceptionMessage('Failed to read file table1a Cannot open file table1a');
        $writer->uploadTables(
            $root,
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'workspace-snowflake'
        );
    }

    public function testMappingMerge()
    {
        $root = $this->tmp->getTmpFolder();
        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        // because of https://keboola.atlassian.net/browse/KBC-228 we need to use default backend (or create the
        // target bucket with the same backend)
        $this->prepareWorkspaceWithTables($tokenInfo['owner']['defaultBackend']);

        $configs = [
            [
                'source' => 'table1a',
                'destination' => 'out.c-output-mapping-test.table1a',
                'metadata' => [
                    [
                        'key' => 'foo',
                        'value' => 'bar',
                    ],
                ],
            ],
        ];
        file_put_contents(
            $root . '/table1a.manifest',
            json_encode(
                [
                    'columns' => ['Id', 'Name'],
                    'metadata' => [
                        [
                            'key' => 'foo',
                            'value' => 'baz',
                        ],
                        [
                            'key' => 'bar',
                            'value' => 'baz',
                        ],
                    ],
                ]
            )
        );
        $writer = new TableWriter($this->clientWrapper, new NullLogger(), $this->getWorkspaceProvider());

        $tableQueue = $writer->uploadTables(
            $root,
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'workspace-snowflake'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $metadata = new Metadata($this->clientWrapper->getBasicClient());
        $tableMetadata = $metadata->listTableMetadata('out.c-output-mapping-test.table1a');
        $tableMetadataValues = [];
        self::assertCount(4, $tableMetadata);
        foreach ($tableMetadata as $item) {
            $tableMetadataValues[$item['key']] = $item['value'];
        }
        self::assertEquals(
            [
                'foo' => 'bar',
                'bar' => 'baz',
                'KBC.createdBy.component.id' => 'foo',
                'KBC.lastUpdatedBy.component.id' => 'foo',
            ],
            $tableMetadataValues
        );
    }

    public function testRedshiftTableOutputMapping()
    {
        $root = $this->tmp->getTmpFolder();
        $this->prepareWorkspaceWithTables('redshift');
        // snowflake bucket does not work - https://keboola.atlassian.net/browse/KBC-228
        $this->clientWrapper->getBasicClient()->createBucket('output-mapping-test', 'out', '', 'redshift');
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

        $tableQueue = $writer->uploadTables($root, ['mapping' => $configs], ['componentId' => 'foo'], 'workspace-redshift');
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
        $data = (array) $this->clientWrapper->getBasicClient()->getTableDataPreview('out.c-output-mapping-test.table1a', ['format' => 'json']);
        $values = [];
        foreach ($data['rows'] as $row) {
            foreach ($row as $column) {
                $values[] = $column['value'];
            }
        }
        sort($values);
        $this->assertEquals(['aabb', 'ccdd', 'test', 'test'], $values);
    }

    public function testWriteTableOutputMappingDevMode()
    {
        $this->clientWrapper = new ClientWrapper(
            new Client([
                'url' => STORAGE_API_URL,
                'token' => STORAGE_API_TOKEN_MASTER,
                'backoffMaxTries' => 1,
                'jobPollRetryDelay' => function () {
                    return 1;
                },
            ]),
            null,
            null
        );
        $branchId = $this->createBranch($this->clientWrapper, 'dev-123');
        $this->clientWrapper->setBranchId($branchId);
        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        $this->prepareWorkspaceWithTables($tokenInfo['owner']['defaultBackend']);
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
        $root = $this->tmp->getTmpFolder();
        $this->tmp->initRunFolder();
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
            'workspace-snowflake'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(2, $jobIds);
        $tables = $this->clientWrapper->getBasicClient()->listTables(sprintf('out.c-%s-output-mapping-test', $branchId));
        $this->assertCount(2, $tables);
        $tableIds = [$tables[0]["id"], $tables[1]["id"]];
        sort($tableIds);
        $this->assertEquals(
            [
                sprintf('out.c-%s-output-mapping-test.table1a', $branchId),
                sprintf('out.c-%s-output-mapping-test.table2a', $branchId),
            ],
            $tableIds
        );
        $this->assertCount(2, $jobIds);
        $this->assertNotEmpty($jobIds[0]);
        $this->assertNotEmpty($jobIds[1]);
    }
}
