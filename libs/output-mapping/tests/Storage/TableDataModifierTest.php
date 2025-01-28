<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Generator;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationDeleteWhere;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Storage\TableDataModifier;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsTestTables;
use Keboola\OutputMapping\Writer\Table\MappingDestination;

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

        $tableDataModifier->updateTableData($source, $destination);

        $newTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId);

        $this->assertEquals(1, $newTable['rowsCount']);
    }

    public static function provideDeleteTableRowsFromDeleteWhereConfig(): Generator
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
                        'deletedRows' => null, //2, (bug in Storage API)
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
                        'deletedRows' => null, //1, (bug in Storage API)
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
                        'deletedRows' => null, //1, (bug in Storage API)
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
                        'deletedRows' => null, //0, (bug in Storage API)
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
            'in Storage: exceptions.storage.tables.columnNotExists';

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

    public function testaDeleteTableRowsFromDeleteWhereConfigWithWorkspace(): void
    {
        $this->markTestIncomplete('Not implemented yet on Storage API side.');
    }

    private static function assertJobParams(array $expectedJobParams, array $actualJobParams): void
    {
        foreach ($expectedJobParams as $jobParam => $jobParamValues) {
            self::assertArrayHasKey($jobParam, $actualJobParams);
            self::assertSame($jobParamValues, $actualJobParams[$jobParam]);
        }
    }
}
