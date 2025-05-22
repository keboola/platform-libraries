<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationDeleteWhere;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Storage\TableDataModifier;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsTestTables;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\OutputMapping\Writer\Table\Strategy\SqlWorkspaceTableStrategy;
use Keboola\StorageApi\Workspaces;

class TableDataModifierTest extends AbstractTestCase
{
    #[NeedsTestTables(count: 1)]
    public function testDeleteTableRows(): void
    {
        $tableDataModifier = new TableDataModifier($this->clientWrapper);

        $destination = new MappingDestination($this->firstTableId);

        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getDeleteWhereColumn')->willReturn('Id');
        $source->method('getDeleteWhereOperator')->willReturn('eq');
        $source->method('getDeleteWhereValues')->willReturn(['id1', 'id2']);

        $runId = $this->clientWrapper->getBasicClient()->generateRunId();
        $this->clientWrapper->getTableAndFileStorageClient()->setRunId($runId);

        $tableDataModifier->updateTableData($source, $destination);

        $newTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId);

        $this->assertEquals(1, $newTable['rowsCount']);

        $jobs = array_values(
            array_filter(
                $this->clientWrapper->getBasicClient()->listJobs(),
                function (array $job) use ($runId): bool {
                    return $job['runId'] === $runId;
                },
            ),
        );

        self::assertCount(1, $jobs);

        $job = $jobs[0];

        self::assertArrayHasKey('operationParams', $job);
        self::assertSame(
            [
                'queue' => 'main',
                'request' => [
                    'changedSince' => null,
                    'changedUntil' => null,
                    'whereFilters' => [
                        [
                            'column' => 'Id',
                            'values' => ['id1', 'id2'],
                            'operator' => 'eq',
                        ],
                    ],
                ],
                'backendConfiguration' => [],
            ],
            $job['operationParams'],
        );

        self::assertArrayHasKey('results', $job);
        self::assertSame(
            [
                'deletedRows' => 2,
            ],
            $job['results'],
        );
    }

    public static function provideDeleteTableRowsFromDeleteWhereConfig(): iterable
    {
        yield 'single delete where' => [
            'deleteWhere' => [
                // single tableRowsDelete job
                new MappingFromConfigurationDeleteWhere([
                    'where_filters' => [
                        [
                            'column' => 'Id',
                            'operator' => 'eq',
                            'values_from_set' => ['id1', 'id2'],
                        ],
                    ],
                ]),
            ],
            'expectedRowsCount' => 1,
            'expectedJobsProperties' => [
                'job1' => [
                    'operationParams' => [
                        'queue' => 'main',
                        'request' => [
                            'changedSince' => null,
                            'changedUntil' => null,
                            'whereFilters' => [
                                [
                                    'column' => 'Id',
                                    'values' => ['id1', 'id2'],
                                    'operator' => 'eq',
                                ],
                            ],
                        ],
                        'backendConfiguration' => [],
                    ],
                    'results' => [
                        'deletedRows' => 2,
                    ],
                ],
            ],
        ];

        yield 'multiple delete where' => [
            'deleteWhere' => [
                // multiple tableRowsDelete jobs
                new MappingFromConfigurationDeleteWhere([
                    'where_filters' => [
                        [
                            'column' => 'Id',
                            'operator' => 'eq',
                            'values_from_set' => ['id1'],
                        ],
                    ],
                ]),
                new MappingFromConfigurationDeleteWhere([
                    'where_filters' => [
                        [
                            'column' => 'Id',
                            'operator' => 'eq',
                            'values_from_set' => ['id2'],
                        ],
                    ],
                ]),
            ],
            'expectedRowsCount' => 1,
            'expectedJobsProperties' => [
                'job2' => [
                    'operationParams' => [
                        'queue' => 'main',
                        'request' => [
                            'changedSince' => null,
                            'changedUntil' => null,
                            'whereFilters' => [
                                [
                                    'column' => 'Id',
                                    'values' => ['id2'],
                                    'operator' => 'eq',
                                ],
                            ],
                        ],
                        'backendConfiguration' => [],
                    ],
                    'results' => [
                        'deletedRows' => 1,
                    ],
                ],
                'job1' => [
                    'operationParams' => [
                        'queue' => 'main',
                        'request' => [
                            'changedSince' => null,
                            'changedUntil' => null,
                            'whereFilters' => [
                                [
                                    'column' => 'Id',
                                    'values' => ['id1'],
                                    'operator' => 'eq',
                                ],
                            ],
                        ],
                        'backendConfiguration' => [],
                    ],
                    'results' => [
                        'deletedRows' => 1,
                    ],
                ],
            ],
        ];

        yield 'multiple where_filters' => [
            'deleteWhere' => [
                // single tableRowsDelete - multiple conditions
                new MappingFromConfigurationDeleteWhere([
                    'where_filters' => [
                        [
                            'column' => 'Id',
                            'operator' => 'eq',
                            'values_from_set' => ['id1'],
                        ],
                        [
                            'column' => 'Id',
                            'operator' => 'eq',
                            'values_from_set' => ['id2'],
                        ],
                    ],
                ]),
            ],
            'expectedRowsCount' => 3, // Condition Id IN('id1') AND Id IN('id2') will never be true
            'expectedJobsProperties' => [
                'job1' => [
                    'operationParams' => [
                        'queue' => 'main',
                        'request' => [
                            'changedSince' => null,
                            'changedUntil' => null,
                            'whereFilters' => [
                                [
                                    'column' => 'Id',
                                    'values' => ['id1'],
                                    'operator' => 'eq',
                                ],
                                [
                                    'column' => 'Id',
                                    'values' => ['id2'],
                                    'operator' => 'eq',
                                ],
                            ],
                        ],
                        'backendConfiguration' => [],
                    ],
                    'results' => [
                        'deletedRows' => 0,
                    ],
                ],
            ],
        ];
    }

    /**
     * @dataProvider provideDeleteTableRowsFromDeleteWhereConfig
     */
    #[NeedsTestTables(count: 1)]
    public function testDeleteTableRowsFromDeleteWhereConfig(
        array $deleteWhere,
        int $expectedRowsCount,
        array $expectedJobsProperties,
    ): void {
        $tableDataModifier = new TableDataModifier($this->clientWrapper);

        $destination = new MappingDestination($this->firstTableId);

        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getDeleteWhere')->willReturn($deleteWhere);

        $runId = $this->clientWrapper->getBasicClient()->generateRunId();
        $this->clientWrapper->getTableAndFileStorageClient()->setRunId($runId);

        $tableDataModifier->updateTableData($source, $destination);

        $newTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId);

        $this->assertEquals($expectedRowsCount, $newTable['rowsCount']);

        $jobs = array_values(
            array_filter(
                $this->clientWrapper->getBasicClient()->listJobs(),
                function (array $job) use ($runId): bool {
                    return $job['runId'] === $runId;
                },
            ),
        );

        self::assertCount(count($expectedJobsProperties), $jobs);
        foreach (array_values($expectedJobsProperties) as $key => $expectedJobProperties) {
            self::assertArrayHasKey($key, $jobs);
            self::assertJobParams($expectedJobProperties, $jobs[$key]);
        }
    }

    #[NeedsTestTables(count: 1)]
    public function testDeleteTableRowsWithUnexistColumn(): void
    {
        $tableDataModifier = new TableDataModifier($this->clientWrapper);

        $destination = new MappingDestination($this->firstTableId);

        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getDeleteWhereColumn')->willReturn('UnexistColumn');
        $source->method('getDeleteWhereOperator')->willReturn('eq');
        $source->method('getDeleteWhereValues')->willReturn(['id1', 'id2']);

        $expectedMessage = 'Cannot delete rows ' .
            'from table "in.c-TableDataModifierTest_testDeleteTableRowsWithUnexistColumn.test1" ' .
            'in Storage: Filter validation: Cannot filter by column "UnexistColumn", column does not exist';

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage($expectedMessage);
        $tableDataModifier->updateTableData($source, $destination);
    }

    #[NeedsTestTables(count: 1)]
    public function testWhereColumnNotSet(): void
    {
        $tableDataModifier = new TableDataModifier($this->clientWrapper);

        $destination = new MappingDestination($this->firstTableId);

        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getDeleteWhereColumn')->willReturn(null);
        $source->method('getDeleteWhereOperator')->willReturn('eq');
        $source->method('getDeleteWhereValues')->willReturn(['id1', 'id2']);

        $tableDataModifier->updateTableData($source, $destination);

        $newTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId);

        $this->assertEquals(3, $newTable['rowsCount']);
    }

    public static function provideDeleteTableRowsFromDeleteWhereConfigWithWorkspace(): iterable
    {
        yield 'normal situation' => [
            'deleteFilterForTableInWorkspace' => [
                'whereColumn' => 'Id',
                'whereOperator' => 'eq',
                'whereValues' => ['id2'],
            ],
            'expectedRowsCount' => 1,
            'expectedDeletedRowsCount' => 2,
        ];
        yield 'table in workspace is empty' => [
            'deleteFilterForTableInWorkspace' => [
                'whereColumn' => 'Id',
                'whereOperator' => 'eq',
                'whereValues' => ['id1', 'id2', 'id3'],
            ],
            'expectedRowsCount' => 3, // no records will be deleted
            'expectedDeletedRowsCount' => 0,
        ];
    }

    /**
     * @dataProvider provideDeleteTableRowsFromDeleteWhereConfigWithWorkspace
     */
    #[NeedsTestTables(count: 2)]
    public function testaDeleteTableRowsFromDeleteWhereConfigWithWorkspace(
        array $deleteFilterForTableInWorkspace,
        int $expectedRowsCount,
        int $expectedDeletedRowsCount,
    ): void {
        $workspace = $this->getWorkspaceStagingFactory()->getTableOutputStrategy();
        self::assertInstanceOf(SqlWorkspaceTableStrategy::class, $workspace);

        $workspaces = new Workspaces($this->clientWrapper->getBasicClient());

        $workspaceId = $workspace->getDataStorage()->getWorkspaceId();

        $this->clientWrapper->getTableAndFileStorageClient()->deleteTableRows(
            $this->secondTableId,
            $deleteFilterForTableInWorkspace,
        );

        $workspaces->cloneIntoWorkspace((int) $workspaceId, [
            'input' => [
                [
                    'source' => $this->secondTableId,
                    'destination' => 'deleteFilter',
                ],
            ],
        ]);

        $deleteWhere = [
            new MappingFromConfigurationDeleteWhere([
                'where_filters' => [
                    [
                        'column' => 'Id',
                        'operator' => 'eq',
                        'values_from_workspace' => [
                            'workspace_id' => $workspaceId,
                            'table' => 'deleteFilter',
                        ],
                    ],
                ],
            ]),
        ];

        $tableDataModifier = new TableDataModifier($this->clientWrapper);

        $destination = new MappingDestination($this->firstTableId);

        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getDeleteWhere')->willReturn($deleteWhere);

        $runId = $this->clientWrapper->getBasicClient()->generateRunId();
        $this->clientWrapper->getTableAndFileStorageClient()->setRunId($runId);

        $tableDataModifier->updateTableData($source, $destination);

        $newTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId);
        $this->assertEquals($expectedRowsCount, $newTable['rowsCount']);

        $jobs = array_values(
            array_filter(
                $this->clientWrapper->getBasicClient()->listJobs(),
                function (array $job) use ($runId): bool {
                    return $job['runId'] === $runId;
                },
            ),
        );

        self::assertCount(1, $jobs);
        self::assertJobParams(
            [
                'operationParams' => [
                    'queue' => 'main',
                    'request' => [
                        'changedSince' => null,
                        'changedUntil' => null,
                        'whereFilters' => [
                            [
                                'column' => 'Id',
                                'operator' => 'eq',
                                'valuesByTableInWorkspace' => [
                                    'table' => 'deleteFilter',
                                    'column' => 'Id',
                                    'workspaceId' => (int) $workspaceId,
                                ],
                            ],
                        ],
                    ],
                    'backendConfiguration' => [],
                ],
                'results' => [
                    'deletedRows' => $expectedDeletedRowsCount,
                ],
            ],
            $jobs[0],
        );
    }

    private static function assertJobParams(array $expectedJobParams, array $actualJobParams): void
    {
        foreach ($expectedJobParams as $jobParam => $jobParamValues) {
            self::assertArrayHasKey($jobParam, $actualJobParams);
            self::assertSame($jobParamValues, $actualJobParams[$jobParam]);
        }
    }
}
