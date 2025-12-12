<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Strategy;

use InvalidArgumentException;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\Strategy\AbstractWorkspaceStrategy;
use Keboola\InputMapping\Table\Strategy\WorkspaceLoadPlan;
use Keboola\InputMapping\Table\Strategy\WorkspaceLoadQueue;
use Keboola\InputMapping\Table\Strategy\WorkspaceLoadType;
use Keboola\InputMapping\Table\Strategy\WorkspaceTableLoadInstruction;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StagingProvider\Staging\StagingInterface;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStagingInterface;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\StorageApiToken;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class AbstractWorkspaceStrategyTest extends TestCase
{
    private TestHandler $testHandler;
    private Logger $testLogger;

    public function setUp(): void
    {
        parent::setUp();

        $this->testHandler = new TestHandler();
        $this->testLogger = new Logger('testLogger', [$this->testHandler]);
    }


    public function testConstructorValidatesDataStorageIsWorkspaceStagingInterface(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Data storage must be instance of WorkspaceStagingInterface');

        new class(
            $this->createMock(ClientWrapper::class),
            $this->createMock(LoggerInterface::class),
            $this->createMock(StagingInterface::class), // This should fail validation
            $this->createMock(FileStagingInterface::class),
            $this->createMock(InputTableStateList::class),
            'destination',
            FileFormat::Json,
        ) extends AbstractWorkspaceStrategy {
            public function getWorkspaceType(): string
            {
                return 'test';
            }

            public function handleExports(array $exports, bool $preserve): array
            {
                return [];
            }
        };
    }

    public function testPrepareTableLoadsToWorkspaceClone(): void
    {
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->once())
            ->method('getToken')
            ->willReturn(new StorageApiToken(
                [
                    'owner' => ['id' => 12345],
                ],
                'my-secret-token',
            ))
        ;

        $strategy = $this->createTestStrategy($clientWrapper, 'snowflake');

        // Table that can be cloned (Snowflake backend, no filters)
        $tableOptions = new RewrittenInputTableOptions(
            ['source' => 'in.c-test-bucket.table1', 'destination' => 'table1'],
            'in.c-test-bucket.table1',
            123,
            [
                'id' => 'in.c-test-bucket.table1',
                'bucket' => ['backend' => 'snowflake'],
                'isAlias' => false,
            ],
        );

        $instructions = $strategy->prepareTableLoadsToWorkspace([$tableOptions]);

        self::assertCount(1, $instructions);
        self::assertInstanceOf(WorkspaceTableLoadInstruction::class, $instructions[0]);
        self::assertSame(WorkspaceLoadType::CLONE, $instructions[0]->loadType);
        self::assertSame($tableOptions, $instructions[0]->table);
        self::assertNull($instructions[0]->loadOptions);

        self::assertTrue($this->testHandler->hasInfoThatContains('Table "in.c-test-bucket.table1" will be cloned.'));
    }

    public function testPrepareTableLoadsToWorkspaceBigQueryDefaultsCopy(): void
    {
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->exactly(2))
            ->method('getToken')
            ->willReturn(new StorageApiToken(
                [
                    'owner' => ['id' => 12345, 'features' => []],
                ],
                'my-secret-token',
            ))
        ;

        $strategy = $this->createTestStrategy($clientWrapper, 'bigquery');

        // BigQuery table in BigQuery workspace without feature flag defaults to COPY
        $tableOptions = new RewrittenInputTableOptions(
            ['source' => 'in.c-test-bucket.table1', 'destination' => 'table1'],
            'in.c-test-bucket.table1',
            123,
            [
                'id' => 'in.c-test-bucket.table1',
                'bucket' => ['backend' => 'bigquery'],
                'isAlias' => false,
            ],
        );

        $instructions = $strategy->prepareTableLoadsToWorkspace([$tableOptions]);

        self::assertCount(1, $instructions);
        self::assertEquals(WorkspaceLoadType::COPY, $instructions[0]->loadType);
        self::assertSame($tableOptions, $instructions[0]->table);
        self::assertSame(['overwrite' => false], $instructions[0]->loadOptions);

        self::assertTrue(
            $this->testHandler->hasInfoThatContains('Table "in.c-test-bucket.table1" will be copied.'),
        );
    }

    public function testPrepareTableLoadsToWorkspaceBigQueryViewWithFeatureFlag(): void
    {
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->exactly(2))
            ->method('getToken')
            ->willReturn(new StorageApiToken(
                [
                    'owner' => ['id' => 12345, 'features' => ['bigquery-default-im-view']],
                ],
                'my-secret-token',
            ))
        ;

        $strategy = $this->createTestStrategy($clientWrapper, 'bigquery');

        // BigQuery table in BigQuery workspace with feature flag uses VIEW
        $tableOptions = new RewrittenInputTableOptions(
            ['source' => 'in.c-test-bucket.table1', 'destination' => 'table1'],
            'in.c-test-bucket.table1',
            123,
            [
                'id' => 'in.c-test-bucket.table1',
                'bucket' => ['backend' => 'bigquery'],
                'isAlias' => false,
            ],
        );

        $instructions = $strategy->prepareTableLoadsToWorkspace([$tableOptions]);

        self::assertCount(1, $instructions);
        self::assertEquals(WorkspaceLoadType::VIEW, $instructions[0]->loadType);
        self::assertSame($tableOptions, $instructions[0]->table);
        self::assertSame(['overwrite' => false], $instructions[0]->loadOptions);

        self::assertTrue(
            $this->testHandler->hasInfoThatContains('Table "in.c-test-bucket.table1" will be created as view.'),
        );
    }

    public function testPrepareTableLoadsToWorkspaceCopy(): void
    {
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->once())
            ->method('getToken')
            ->willReturn(new StorageApiToken(
                [
                    'owner' => ['id' => 12345],
                ],
                'my-secret-token',
            ))
        ;

        $strategy = $this->createTestStrategy($clientWrapper, 'snowflake');

        // Table that must be copied (Different backend than workspace)
        $tableOptions = new RewrittenInputTableOptions(
            ['source' => 'in.c-test-bucket.table1', 'destination' => 'table1'],
            'in.c-test-bucket.table1',
            123,
            [
                'id' => 'in.c-test-bucket.table1',
                'bucket' => ['backend' => 'bigquery'],
                'isAlias' => false,
            ],
        );

        $instructions = $strategy->prepareTableLoadsToWorkspace([$tableOptions]);

        self::assertCount(1, $instructions);
        self::assertEquals(WorkspaceLoadType::COPY, $instructions[0]->loadType);
        self::assertSame($tableOptions, $instructions[0]->table);
        self::assertSame(['overwrite' => false], $instructions[0]->loadOptions);

        self::assertTrue($this->testHandler->hasInfoThatContains('Table "in.c-test-bucket.table1" will be copied.'));
    }

    public function testPrepareTableLoadsToWorkspaceChecksViableLoadMethod(): void
    {
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->once())
            ->method('getToken')
            ->willReturn(new StorageApiToken(
                [
                    'owner' => ['id' => 12345],
                ],
                'my-secret-token',
            ))
        ;

        $strategy = $this->createTestStrategy($clientWrapper, 'bigquery');

        // Create a table that will cause checkViableBigQueryLoadMethod to throw an exception
        // (BigQuery workspace with alias table from the same project)
        $tableOptions = new RewrittenInputTableOptions(
            ['source' => 'in.c-test-bucket.table1', 'destination' => 'table1'],
            'in.c-test-bucket.table1',
            123,
            [
                'id' => 'in.c-test-bucket.table1',
                'bucket' => ['backend' => 'bigquery'],
                'isAlias' => true,
                'sourceTable' => ['project' => ['id' => 12345]], // Same project ID
            ],
        );

        // This should throw an InvalidInputException because checkViableBigQueryLoadMethod is called
        $this->expectException(InvalidInputException::class);
        $this->expectExceptionMessage(
            'Table "in.c-test-bucket.table1" is an alias, which is not supported when loading Bigquery tables.',
        );

        $strategy->prepareTableLoadsToWorkspace([$tableOptions]);
    }

    public function testExecuteTableLoadsToWorkspaceWithCleanAndPreserveFalse(): void
    {
        $branchClient = $this->createMock(BranchAwareClient::class);

        // New unified API: empty plan with preserve=false makes single API call to clean workspace
        $branchClient->expects($this->once())
            ->method('apiPostJson')
            ->with('workspaces/456/load', [
                'input' => [], // empty input
                'preserve' => 0, // triggers cleanup
            ], false)
            ->willReturn(['id' => 123]);

        $branchClient->expects($this->never())
            ->method('handleAsyncTasks');

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->once())
            ->method('getBranchClient')
            ->willReturn($branchClient);

        $dataStorage = $this->createMock(WorkspaceStagingInterface::class);
        $dataStorage->expects($this->once())
            ->method('getWorkspaceId')
            ->willReturn('456');

        $strategy = $this->createTestStrategyWithDataStorage($clientWrapper, 'snowflake', $dataStorage);

        // Create plan with preserve=false (should trigger clean)
        $plan = new WorkspaceLoadPlan(
            [],
            false, // preserve=false should trigger clean
        );

        $result = $strategy->executeTableLoadsToWorkspace($plan);
        self::assertCount(1, $result->jobs);
        self::assertTrue($this->testHandler->hasInfoThatContains('Cleaning workspace and loading tables.'));
    }

    public function testExecuteTableLoadsToWorkspaceWithMixedOperations(): void
    {
        $branchClient = $this->createMock(BranchAwareClient::class);

        // New unified API: single call to workspaces/{id}/load with all operations and loadType parameters
        $branchClient->expects($this->once())
            ->method('apiPostJson')
            ->with(
                'workspaces/456/load',
                [
                    'input' => [
                        [
                            'source' => 'in.c-test-bucket.table1',
                            'destination' => 'table1',
                            'overwrite' => true,
                            'dropTimestampColumn' => true,
                            'sourceBranchId' => 123,
                            'loadType' => 'CLONE',
                        ],
                        [
                            'source' => 'in.c-test-bucket.table2',
                            'destination' => 'table2',
                            'overwrite' => false,
                            'dropTimestampColumn' => false,
                            'sourceBranchId' => 124,
                            'loadType' => 'CLONE',
                        ],
                        [
                            'source' => 'in.c-test-bucket.table3',
                            'destination' => 'table3',
                            'overwrite' => true,
                            'sourceBranchId' => 125,
                            'loadType' => 'COPY',
                        ],
                        [
                            'source' => 'in.c-test-bucket.table4',
                            'destination' => 'table4',
                            'overwrite' => false,
                            'sourceBranchId' => 126,
                            'loadType' => 'VIEW',
                        ],
                    ],
                    'preserve' => 0, // preserve=false triggers cleanup
                ],
                false,
            )
            ->willReturn(['id' => 456]);

        $branchClient->expects($this->never())
            ->method('handleAsyncTasks');

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->once())
            ->method('getBranchClient')
            ->willReturn($branchClient);

        $dataStorage = $this->createMock(WorkspaceStagingInterface::class);
        $dataStorage->expects(self::once())
            ->method('getWorkspaceId')
            ->willReturn('456');

        $strategy = $this->createTestStrategyWithDataStorage($clientWrapper, 'snowflake', $dataStorage);

        // Create table options for clone, copy, and view operations
        $cloneTableOptions1 = new RewrittenInputTableOptions(
            [
                'source' => 'in.c-test-bucket.table1',
                'destination' => 'table1',
                'overwrite' => true,
                'keep_internal_timestamp_column' => false, // dropTimestampColumn will be true
            ],
            'in.c-test-bucket.table1',
            123,
            [
                'id' => 'in.c-test-bucket.table1',
                'bucket' => ['backend' => 'snowflake'],
                'isAlias' => false,
            ],
        );

        $cloneTableOptions2 = new RewrittenInputTableOptions(
            [
                'source' => 'in.c-test-bucket.table2',
                'destination' => 'table2',
                'overwrite' => false,
                'keep_internal_timestamp_column' => true, // dropTimestampColumn will be false
            ],
            'in.c-test-bucket.table2',
            124,
            [
                'id' => 'in.c-test-bucket.table2',
                'bucket' => ['backend' => 'snowflake'],
                'isAlias' => false,
            ],
        );

        $copyTableOptions = new RewrittenInputTableOptions(
            ['source' => 'in.c-test-bucket.table3', 'destination' => 'table3'],
            'in.c-test-bucket.table3',
            125,
            [
                'id' => 'in.c-test-bucket.table3',
                'bucket' => ['backend' => 'bigquery'],
                'isAlias' => false,
            ],
        );

        $viewTableOptions = new RewrittenInputTableOptions(
            ['source' => 'in.c-test-bucket.table4', 'destination' => 'table4'],
            'in.c-test-bucket.table4',
            126,
            [
                'id' => 'in.c-test-bucket.table4',
                'bucket' => ['backend' => 'bigquery'],
                'isAlias' => false,
            ],
        );

        $plan = new WorkspaceLoadPlan(
            [
                new WorkspaceTableLoadInstruction(
                    WorkspaceLoadType::CLONE,
                    $cloneTableOptions1,
                    null,
                ),
                new WorkspaceTableLoadInstruction(
                    WorkspaceLoadType::COPY,
                    $copyTableOptions,
                    ['overwrite' => true],
                ),
                new WorkspaceTableLoadInstruction(
                    WorkspaceLoadType::CLONE,
                    $cloneTableOptions2,
                    null,
                ),
                new WorkspaceTableLoadInstruction(
                    WorkspaceLoadType::VIEW,
                    $viewTableOptions,
                    ['overwrite' => false],
                ),
            ],
            false, // preserve=false (trigger cleanup)
        );

        $result = $strategy->executeTableLoadsToWorkspace($plan);

        // Single job with all tables (new single-job optimization)
        self::assertCount(1, $result->jobs);

        $job = $result->jobs[0];
        self::assertSame(
            [$cloneTableOptions1, $cloneTableOptions2, $copyTableOptions, $viewTableOptions],
            $job->tables,
        );

        self::assertTrue($this->testHandler->hasInfoThatContains('Cleaning workspace and loading tables.'));
        self::assertTrue($this->testHandler->hasInfoThatContains('Cloning 2 tables to workspace.'));
        self::assertTrue($this->testHandler->hasInfoThatContains('Copying 2 tables to workspace.'));
    }

    public function testExecuteTableLoadsToWorkspaceEmptyPlanWithPreserveTrue(): void
    {
        $branchClient = $this->createMock(BranchAwareClient::class);
        $branchClient->expects(self::never())->method(self::anything());

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->once())
            ->method('getBranchClient')
            ->willReturn($branchClient)
        ;

        $strategy = $this->createTestStrategy($clientWrapper, 'snowflake');

        // Empty plan with preserve=true should not trigger any operations
        $plan = new WorkspaceLoadPlan(
            [],
            true, // preserve=true (no clean)
        );

        $result = $strategy->executeTableLoadsToWorkspace($plan);

        self::assertInstanceOf(WorkspaceLoadQueue::class, $result);
        self::assertEmpty($result->jobs);
    }

    private function createTestStrategy(
        ClientWrapper $clientWrapper,
        string $workspaceType,
    ): TestWorkspaceStrategy {
        $strategy = new TestWorkspaceStrategy(
            $clientWrapper,
            $this->testLogger,
            $this->createMock(WorkspaceStagingInterface::class),
            $this->createMock(FileStagingInterface::class),
            $this->createMock(InputTableStateList::class),
            'destination',
            FileFormat::Json,
        );
        $strategy->setWorkspaceType($workspaceType);
        return $strategy;
    }

    private function createTestStrategyWithDataStorage(
        ClientWrapper $clientWrapper,
        string $workspaceType,
        WorkspaceStagingInterface $dataStorage,
        ?FileStagingInterface $metadataStorage = null,
    ): TestWorkspaceStrategy {
        $strategy = new TestWorkspaceStrategy(
            $clientWrapper,
            $this->testLogger,
            $dataStorage,
            $metadataStorage ?? $this->createMock(FileStagingInterface::class),
            $this->createMock(InputTableStateList::class),
            'destination',
            FileFormat::Json,
        );
        $strategy->setWorkspaceType($workspaceType);
        return $strategy;
    }

    /**
     * @param string[] $columns
     * @return array<string, mixed>
     */
    private function createTableInfo(string $tableId, string $tableName, array $columns): array
    {
        return [
            'id' => $tableId,
            'uri' => 'https://connection.keboola.com/v2/storage/tables/' . $tableId,
            'name' => $tableName,
            'primaryKey' => [],
            'distributionKey' => [],
            'created' => '2022-06-03T01:31:43+0200',
            'lastChangeDate' => '2022-06-03T02:31:43+0200',
            'lastImportDate' => '2022-06-03T03:31:43+0200',
            'columns' => $columns,
            'metadata' => [],
            'columnMetadata' => [],
        ];
    }

    public function testPrepareAndExecuteTableLoadsEmpty(): void
    {
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $strategy = $this->createTestStrategy($clientWrapper, 'snowflake');

        $result = $strategy->prepareAndExecuteTableLoads([], true);

        self::assertInstanceOf(WorkspaceLoadQueue::class, $result);
        self::assertEmpty($result->jobs);
    }

    public function testPrepareAndExecuteTableLoadsWithCloneAndCopy(): void
    {
        $branchClient = $this->createMock(BranchAwareClient::class);

        // Mock single API call with unified format (new single-job optimization)
        $branchClient->expects($this->once())
            ->method('apiPostJson')
            ->with(
                'workspaces/456/load',
                [
                    'input' => [
                        [
                            'source' => 'in.c-test-bucket.table1',
                            'destination' => 'table1',
                            'overwrite' => false,
                            'dropTimestampColumn' => false,
                            'sourceBranchId' => 123,
                            'loadType' => 'CLONE',
                        ],
                        [
                            'source' => 'in.c-test-bucket.table2',
                            'destination' => 'table2',
                            'overwrite' => false,
                            'sourceBranchId' => 124,
                            'loadType' => 'COPY',
                        ],
                    ],
                    'preserve' => 1,
                ],
                false,
            )
            ->willReturn(['id' => 789]);

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->exactly(2))
            ->method('getToken')
            ->willReturn(new StorageApiToken(
                [
                    'owner' => ['id' => 12345],
                ],
                'my-secret-token',
            ));
        $clientWrapper->expects($this->once())
            ->method('getBranchClient')
            ->willReturn($branchClient);

        $dataStorage = $this->createMock(WorkspaceStagingInterface::class);
        $dataStorage->expects(self::once())
            ->method('getWorkspaceId')
            ->willReturn('456');

        $strategy = $this->createTestStrategyWithDataStorage($clientWrapper, 'snowflake', $dataStorage);

        // Create tables: one clone, one copy
        $cloneTable = new RewrittenInputTableOptions(
            [
                'source' => 'in.c-test-bucket.table1',
                'destination' => 'table1',
                'keep_internal_timestamp_column' => true, // dropTimestampColumn will be false
            ],
            'in.c-test-bucket.table1',
            123,
            [
                'id' => 'in.c-test-bucket.table1',
                'bucket' => ['backend' => 'snowflake'],
                'isAlias' => false,
            ],
        );

        $copyTable = new RewrittenInputTableOptions(
            ['source' => 'in.c-test-bucket.table2', 'destination' => 'table2'],
            'in.c-test-bucket.table2',
            124,
            [
                'id' => 'in.c-test-bucket.table2',
                'bucket' => ['backend' => 'bigquery'],
                'isAlias' => false,
            ],
        );

        $result = $strategy->prepareAndExecuteTableLoads([$cloneTable, $copyTable], true);

        self::assertInstanceOf(WorkspaceLoadQueue::class, $result);
        self::assertCount(1, $result->jobs);

        // Verify single job with both tables
        $job = $result->jobs[0];
        self::assertSame([$cloneTable, $copyTable], $job->tables);
    }

    public function testPrepareAndExecuteTableLoadsWithCleanWorkspace(): void
    {
        $branchClient = $this->createMock(BranchAwareClient::class);

        // New unified API: single call with loadType parameter
        $branchClient->expects($this->once())
            ->method('apiPostJson')
            ->with(
                'workspaces/456/load',
                [
                    'input' => [
                        [
                            'source' => 'in.c-test-bucket.table1',
                            'destination' => 'table1',
                            'overwrite' => false,
                            'dropTimestampColumn' => false,
                            'sourceBranchId' => 123,
                            'loadType' => 'CLONE',
                        ],
                    ],
                    'preserve' => 0, // triggers cleanup
                ],
                false,
            )
            ->willReturn(['id' => 789]);

        $branchClient->expects($this->never())
            ->method('handleAsyncTasks');

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->once())
            ->method('getToken')
            ->willReturn(new StorageApiToken(
                [
                    'owner' => ['id' => 12345],
                ],
                'my-secret-token',
            ));
        $clientWrapper->expects($this->once())
            ->method('getBranchClient')
            ->willReturn($branchClient);

        $dataStorage = $this->createMock(WorkspaceStagingInterface::class);
        $dataStorage->expects(self::atLeastOnce())
            ->method('getWorkspaceId')
            ->willReturn('456');

        $strategy = $this->createTestStrategyWithDataStorage($clientWrapper, 'snowflake', $dataStorage);

        $cloneTable = new RewrittenInputTableOptions(
            [
                'source' => 'in.c-test-bucket.table1',
                'destination' => 'table1',
                'keep_internal_timestamp_column' => true, // dropTimestampColumn will be false
            ],
            'in.c-test-bucket.table1',
            123,
            [
                'id' => 'in.c-test-bucket.table1',
                'bucket' => ['backend' => 'snowflake'],
                'isAlias' => false,
            ],
        );

        $result = $strategy->prepareAndExecuteTableLoads([$cloneTable], false); // preserve=false

        self::assertInstanceOf(WorkspaceLoadQueue::class, $result);
        self::assertCount(1, $result->jobs);
        self::assertTrue($this->testHandler->hasInfoThatContains('Cleaning workspace and loading tables.'));
        self::assertTrue($this->testHandler->hasInfoThatContains('Cloning 1 tables to workspace.'));
    }

    public function testHandleExportsOnlyClone(): void
    {
        $branchClient = $this->createMock(BranchAwareClient::class);
        $branchClient->expects($this->once())
            ->method('apiPostJson')
            ->with('workspaces/456/load-clone', [
                'input' => [
                    [
                        'source' => 'in.c-test-bucket.table1',
                        'destination' => 'table1',
                        'sourceBranchId' => 123,
                        'overwrite' => false,
                        'dropTimestampColumn' => false,
                    ],
                    [
                        'source' => 'in.c-test-bucket.table2',
                        'destination' => 'table2',
                        'sourceBranchId' => 124,
                        'overwrite' => true,
                        'dropTimestampColumn' => true,
                    ],
                ],
                'preserve' => 1,
            ], false)
            ->willReturn(['id' => 100]);

        $branchClient->expects($this->once())
            ->method('handleAsyncTasks')
            ->with([100])
            ->willReturn([['id' => 100, 'status' => 'success']]);

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->exactly(2))
            ->method('getBranchClient')
            ->willReturn($branchClient);

        $dataStorage = $this->createMock(WorkspaceStagingInterface::class);
        $dataStorage->expects($this->once())
            ->method('getWorkspaceId')
            ->willReturn('456');

        $metadataStorage = $this->createMock(FileStagingInterface::class);
        $metadataStorage->expects($this->exactly(2))
            ->method('getPath')
            ->willReturn('/tmp/data');

        $strategy = $this->createTestStrategyWithDataStorage(
            $clientWrapper,
            'snowflake',
            $dataStorage,
            $metadataStorage,
        );

        $table1 = new RewrittenInputTableOptions(
            [
                'source' => 'in.c-test-bucket.table1',
                'destination' => 'table1',
                'keep_internal_timestamp_column' => true,
            ],
            'in.c-test-bucket.table1',
            123,
            $this->createTableInfo('in.c-test-bucket.table1', 'table1', ['col1', 'col2']),
        );

        $table2 = new RewrittenInputTableOptions(
            [
                'source' => 'in.c-test-bucket.table2',
                'destination' => 'table2',
                'overwrite' => true,
                'keep_internal_timestamp_column' => false,
            ],
            'in.c-test-bucket.table2',
            124,
            $this->createTableInfo('in.c-test-bucket.table2', 'table2', ['col3']),
        );

        $exports = [
            ['type' => 'CLONE', 'table' => $table1],
            ['type' => 'CLONE', 'table' => $table2],
        ];

        $result = $strategy->handleExports($exports, true);

        self::assertCount(1, $result);
        self::assertTrue($this->testHandler->hasInfoThatContains('Cloning 2 tables to workspace.'));
        self::assertTrue($this->testHandler->hasInfoThatContains('Processed 1 workspace exports.'));
    }

    public function testHandleExportsOnlyCopy(): void
    {
        $branchClient = $this->createMock(BranchAwareClient::class);
        $branchClient->expects($this->once())
            ->method('apiPostJson')
            ->with('workspaces/456/load', [
                'input' => [
                    [
                        'source' => 'in.c-test-bucket.table1',
                        'destination' => 'table1',
                        'sourceBranchId' => 123,
                        'overwrite' => false,
                    ],
                    [
                        'source' => 'in.c-test-bucket.table2',
                        'destination' => 'table2',
                        'sourceBranchId' => 124,
                        'overwrite' => true,
                        'columns' => ['col1', 'col2'],
                    ],
                ],
                'preserve' => 1,
            ], false)
            ->willReturn(['id' => 200]);

        $branchClient->expects($this->once())
            ->method('handleAsyncTasks')
            ->with([200])
            ->willReturn([['id' => 200, 'status' => 'success']]);

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->exactly(2))
            ->method('getBranchClient')
            ->willReturn($branchClient);

        $dataStorage = $this->createMock(WorkspaceStagingInterface::class);
        $dataStorage->expects($this->once())
            ->method('getWorkspaceId')
            ->willReturn('456');

        $metadataStorage = $this->createMock(FileStagingInterface::class);
        $metadataStorage->expects($this->exactly(2))
            ->method('getPath')
            ->willReturn('/tmp/data');

        $strategy = $this->createTestStrategyWithDataStorage(
            $clientWrapper,
            'snowflake',
            $dataStorage,
            $metadataStorage,
        );

        $table1 = new RewrittenInputTableOptions(
            [
                'source' => 'in.c-test-bucket.table1',
                'destination' => 'table1',
            ],
            'in.c-test-bucket.table1',
            123,
            $this->createTableInfo('in.c-test-bucket.table1', 'table1', ['col1']),
        );

        $table2 = new RewrittenInputTableOptions(
            [
                'source' => 'in.c-test-bucket.table2',
                'destination' => 'table2',
            ],
            'in.c-test-bucket.table2',
            124,
            $this->createTableInfo('in.c-test-bucket.table2', 'table2', ['col2']),
        );

        $exports = [
            ['type' => 'COPY', 'table' => [$table1, ['overwrite' => false]]],
            ['type' => 'COPY', 'table' => [$table2, ['overwrite' => true, 'columns' => ['col1', 'col2']]]],
        ];

        $result = $strategy->handleExports($exports, true);

        self::assertCount(1, $result);
        self::assertTrue($this->testHandler->hasInfoThatContains('Copying 2 tables to workspace.'));
        self::assertTrue($this->testHandler->hasInfoThatContains('Processed 1 workspace exports.'));
    }

    public function testHandleExportsMixedCloneAndCopy(): void
    {
        $branchClient = $this->createMock(BranchAwareClient::class);

        $expectedApiCalls = [
            [
                'endpoint' => 'workspaces/456/load-clone',
                'data' => [
                    'input' => [
                        [
                            'source' => 'in.c-test-bucket.table1',
                            'destination' => 'table1',
                            'sourceBranchId' => 123,
                            'overwrite' => false,
                            'dropTimestampColumn' => false,
                        ],
                    ],
                    'preserve' => 1,
                ],
                'async' => false,
                'returnValue' => ['id' => 100],
            ],
            [
                'endpoint' => 'workspaces/456/load',
                'data' => [
                    'input' => [
                        [
                            'source' => 'in.c-test-bucket.table2',
                            'destination' => 'table2',
                            'sourceBranchId' => 124,
                            'overwrite' => true,
                        ],
                    ],
                    'preserve' => 1,
                ],
                'async' => false,
                'returnValue' => ['id' => 200],
            ],
        ];

        $branchClient->expects($this->exactly(2))
            ->method('apiPostJson')
            ->willReturnCallback(function (string $endpoint, array $data, bool $async) use (&$expectedApiCalls) {
                $expectedCall = array_shift($expectedApiCalls);
                self::assertNotNull($expectedCall);
                self::assertSame($expectedCall['endpoint'], $endpoint);
                self::assertEquals($expectedCall['data'], $data);
                self::assertSame($expectedCall['async'], $async);
                return $expectedCall['returnValue'];
            });

        $expectedAsyncCalls = [
            ['jobIds' => [100], 'returnValue' => [['id' => 100, 'status' => 'success']]],
            ['jobIds' => [200], 'returnValue' => [['id' => 200, 'status' => 'success']]],
        ];

        $branchClient->expects($this->exactly(2))
            ->method('handleAsyncTasks')
            ->willReturnCallback(function (array $jobIds) use (&$expectedAsyncCalls) {
                $expectedCall = array_shift($expectedAsyncCalls);
                self::assertNotNull($expectedCall);
                self::assertSame($expectedCall['jobIds'], $jobIds);
                return $expectedCall['returnValue'];
            });

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->exactly(3))
            ->method('getBranchClient')
            ->willReturn($branchClient);

        $dataStorage = $this->createMock(WorkspaceStagingInterface::class);
        $dataStorage->expects($this->exactly(2))
            ->method('getWorkspaceId')
            ->willReturn('456');

        $metadataStorage = $this->createMock(FileStagingInterface::class);
        $metadataStorage->expects($this->exactly(2))
            ->method('getPath')
            ->willReturn('/tmp/data');

        $strategy = $this->createTestStrategyWithDataStorage(
            $clientWrapper,
            'snowflake',
            $dataStorage,
            $metadataStorage,
        );

        $table1 = new RewrittenInputTableOptions(
            [
                'source' => 'in.c-test-bucket.table1',
                'destination' => 'table1',
                'keep_internal_timestamp_column' => true,
            ],
            'in.c-test-bucket.table1',
            123,
            $this->createTableInfo('in.c-test-bucket.table1', 'table1', ['col1']),
        );

        $table2 = new RewrittenInputTableOptions(
            [
                'source' => 'in.c-test-bucket.table2',
                'destination' => 'table2',
            ],
            'in.c-test-bucket.table2',
            124,
            $this->createTableInfo('in.c-test-bucket.table2', 'table2', ['col2']),
        );

        $exports = [
            ['type' => 'CLONE', 'table' => $table1],
            ['type' => 'COPY', 'table' => [$table2, ['overwrite' => true]]],
        ];

        $result = $strategy->handleExports($exports, true);

        self::assertCount(2, $result);
        self::assertTrue($this->testHandler->hasInfoThatContains('Cloning 1 tables to workspace.'));
        self::assertTrue($this->testHandler->hasInfoThatContains('Copying 1 tables to workspace.'));
        self::assertTrue($this->testHandler->hasInfoThatContains('Processed 2 workspace exports.'));
    }

    public function testHandleExportsPreserveFalseWithClones(): void
    {
        $branchClient = $this->createMock(BranchAwareClient::class);
        $branchClient->expects($this->once())
            ->method('apiPostJson')
            ->with('workspaces/456/load-clone', [
                'input' => [
                    [
                        'source' => 'in.c-test-bucket.table1',
                        'destination' => 'table1',
                        'sourceBranchId' => 123,
                        'overwrite' => false,
                        'dropTimestampColumn' => false,
                    ],
                ],
                'preserve' => 0, // preserve=false => clean workspace
            ], false)
            ->willReturn(['id' => 100]);

        $branchClient->expects($this->once())
            ->method('handleAsyncTasks')
            ->with([100])
            ->willReturn([['id' => 100, 'status' => 'success']]);

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->exactly(2))
            ->method('getBranchClient')
            ->willReturn($branchClient);

        $dataStorage = $this->createMock(WorkspaceStagingInterface::class);
        $dataStorage->expects($this->once())
            ->method('getWorkspaceId')
            ->willReturn('456');

        $metadataStorage = $this->createMock(FileStagingInterface::class);
        $metadataStorage->expects($this->once())
            ->method('getPath')
            ->willReturn('/tmp/data');

        $strategy = $this->createTestStrategyWithDataStorage(
            $clientWrapper,
            'snowflake',
            $dataStorage,
            $metadataStorage,
        );

        $table1 = new RewrittenInputTableOptions(
            [
                'source' => 'in.c-test-bucket.table1',
                'destination' => 'table1',
                'keep_internal_timestamp_column' => true,
            ],
            'in.c-test-bucket.table1',
            123,
            $this->createTableInfo('in.c-test-bucket.table1', 'table1', ['col1']),
        );

        $exports = [
            ['type' => 'CLONE', 'table' => $table1],
        ];

        $result = $strategy->handleExports($exports, false); // preserve=false

        self::assertCount(1, $result);
        self::assertTrue($this->testHandler->hasInfoThatContains('Cloning 1 tables to workspace.'));
    }

    public function testHandleExportsPreserveFalseWithClonesThenCopies(): void
    {
        // This is the CRITICAL test for the hasBeenCleaned logic (line 119 in AbstractWorkspaceStrategy)
        $branchClient = $this->createMock(BranchAwareClient::class);

        $expectedApiCalls = [
            [
                'endpoint' => 'workspaces/456/load-clone',
                'data' => [
                    'input' => [
                        [
                            'source' => 'in.c-test-bucket.table1',
                            'destination' => 'table1',
                            'sourceBranchId' => 123,
                            'overwrite' => false,
                            'dropTimestampColumn' => false,
                        ],
                    ],
                    'preserve' => 0, // Clone operation clears workspace (preserve=false, line 102)
                ],
                'async' => false,
                'returnValue' => ['id' => 100],
            ],
            [
                'endpoint' => 'workspaces/456/load',
                'data' => [
                    'input' => [
                        [
                            'source' => 'in.c-test-bucket.table2',
                            'destination' => 'table2',
                            'sourceBranchId' => 124,
                            'overwrite' => true,
                        ],
                    ],
                    // Line 119: preserve = !hasBeenCleaned && !preserve ? 0 : 1
                    // !true && !false = false, so preserve = 1 (doesn't clean again!)
                    'preserve' => 1,
                ],
                'async' => false,
                'returnValue' => ['id' => 200],
            ],
        ];

        $branchClient->expects($this->exactly(2))
            ->method('apiPostJson')
            ->willReturnCallback(function (string $endpoint, array $data, bool $async) use (&$expectedApiCalls) {
                $expectedCall = array_shift($expectedApiCalls);
                self::assertNotNull($expectedCall);
                self::assertSame($expectedCall['endpoint'], $endpoint);
                self::assertEquals($expectedCall['data'], $data);
                self::assertSame($expectedCall['async'], $async);
                return $expectedCall['returnValue'];
            });

        $expectedAsyncCalls = [
            ['jobIds' => [100], 'returnValue' => [['id' => 100, 'status' => 'success']]],
            ['jobIds' => [200], 'returnValue' => [['id' => 200, 'status' => 'success']]],
        ];

        $branchClient->expects($this->exactly(2))
            ->method('handleAsyncTasks')
            ->willReturnCallback(function (array $jobIds) use (&$expectedAsyncCalls) {
                $expectedCall = array_shift($expectedAsyncCalls);
                self::assertNotNull($expectedCall);
                self::assertSame($expectedCall['jobIds'], $jobIds);
                return $expectedCall['returnValue'];
            });

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->exactly(3))
            ->method('getBranchClient')
            ->willReturn($branchClient);

        $dataStorage = $this->createMock(WorkspaceStagingInterface::class);
        $dataStorage->expects($this->exactly(2))
            ->method('getWorkspaceId')
            ->willReturn('456');

        $metadataStorage = $this->createMock(FileStagingInterface::class);
        $metadataStorage->expects($this->exactly(2))
            ->method('getPath')
            ->willReturn('/tmp/data');

        $strategy = $this->createTestStrategyWithDataStorage(
            $clientWrapper,
            'snowflake',
            $dataStorage,
            $metadataStorage,
        );

        $table1 = new RewrittenInputTableOptions(
            [
                'source' => 'in.c-test-bucket.table1',
                'destination' => 'table1',
                'keep_internal_timestamp_column' => true,
            ],
            'in.c-test-bucket.table1',
            123,
            $this->createTableInfo('in.c-test-bucket.table1', 'table1', ['col1']),
        );

        $table2 = new RewrittenInputTableOptions(
            [
                'source' => 'in.c-test-bucket.table2',
                'destination' => 'table2',
            ],
            'in.c-test-bucket.table2',
            124,
            $this->createTableInfo('in.c-test-bucket.table2', 'table2', ['col2']),
        );

        $exports = [
            ['type' => 'CLONE', 'table' => $table1],
            ['type' => 'COPY', 'table' => [$table2, ['overwrite' => true]]],
        ];

        $result = $strategy->handleExports($exports, false); // preserve=false

        self::assertCount(2, $result);
        self::assertTrue($this->testHandler->hasInfoThatContains('Cloning 1 tables to workspace.'));
        self::assertTrue($this->testHandler->hasInfoThatContains('Copying 1 tables to workspace.'));
    }

    public function testHandleExportsPreserveFalseWithCopiesOnly(): void
    {
        $branchClient = $this->createMock(BranchAwareClient::class);
        $branchClient->expects($this->once())
            ->method('apiPostJson')
            ->with('workspaces/456/load', [
                'input' => [
                    [
                        'source' => 'in.c-test-bucket.table1',
                        'destination' => 'table1',
                        'sourceBranchId' => 123,
                        'overwrite' => false,
                    ],
                ],
                // Line 119: preserve = !hasBeenCleaned && !preserve ? 0 : 1
                // !false && !false = true, so preserve = 0 (clean workspace!)
                'preserve' => 0,
            ], false)
            ->willReturn(['id' => 200]);

        $branchClient->expects($this->once())
            ->method('handleAsyncTasks')
            ->with([200])
            ->willReturn([['id' => 200, 'status' => 'success']]);

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->exactly(2))
            ->method('getBranchClient')
            ->willReturn($branchClient);

        $dataStorage = $this->createMock(WorkspaceStagingInterface::class);
        $dataStorage->expects($this->once())
            ->method('getWorkspaceId')
            ->willReturn('456');

        $metadataStorage = $this->createMock(FileStagingInterface::class);
        $metadataStorage->expects($this->once())
            ->method('getPath')
            ->willReturn('/tmp/data');

        $strategy = $this->createTestStrategyWithDataStorage(
            $clientWrapper,
            'snowflake',
            $dataStorage,
            $metadataStorage,
        );

        $table1 = new RewrittenInputTableOptions(
            [
                'source' => 'in.c-test-bucket.table1',
                'destination' => 'table1',
            ],
            'in.c-test-bucket.table1',
            123,
            $this->createTableInfo('in.c-test-bucket.table1', 'table1', ['col1']),
        );

        $exports = [
            ['type' => 'COPY', 'table' => [$table1, ['overwrite' => false]]],
        ];

        $result = $strategy->handleExports($exports, false); // preserve=false

        self::assertCount(1, $result);
        self::assertTrue($this->testHandler->hasInfoThatContains('Copying 1 tables to workspace.'));
    }

    public function testHandleExportsPreserveTrueWithCopies(): void
    {
        $branchClient = $this->createMock(BranchAwareClient::class);
        $branchClient->expects($this->once())
            ->method('apiPostJson')
            ->with('workspaces/456/load', [
                'input' => [
                    [
                        'source' => 'in.c-test-bucket.table1',
                        'destination' => 'table1',
                        'sourceBranchId' => 123,
                        'overwrite' => false,
                    ],
                ],
                // Line 119: preserve = !hasBeenCleaned && !preserve ? 0 : 1
                // !false && !true = false, so preserve = 1 (keep workspace!)
                'preserve' => 1,
            ], false)
            ->willReturn(['id' => 200]);

        $branchClient->expects($this->once())
            ->method('handleAsyncTasks')
            ->with([200])
            ->willReturn([['id' => 200, 'status' => 'success']]);

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->exactly(2))
            ->method('getBranchClient')
            ->willReturn($branchClient);

        $dataStorage = $this->createMock(WorkspaceStagingInterface::class);
        $dataStorage->expects($this->once())
            ->method('getWorkspaceId')
            ->willReturn('456');

        $metadataStorage = $this->createMock(FileStagingInterface::class);
        $metadataStorage->expects($this->once())
            ->method('getPath')
            ->willReturn('/tmp/data');

        $strategy = $this->createTestStrategyWithDataStorage(
            $clientWrapper,
            'snowflake',
            $dataStorage,
            $metadataStorage,
        );

        $table1 = new RewrittenInputTableOptions(
            [
                'source' => 'in.c-test-bucket.table1',
                'destination' => 'table1',
            ],
            'in.c-test-bucket.table1',
            123,
            $this->createTableInfo('in.c-test-bucket.table1', 'table1', ['col1']),
        );

        $exports = [
            ['type' => 'COPY', 'table' => [$table1, ['overwrite' => false]]],
        ];

        $result = $strategy->handleExports($exports, true); // preserve=true

        self::assertCount(1, $result);
        self::assertTrue($this->testHandler->hasInfoThatContains('Copying 1 tables to workspace.'));
    }

    public function testHandleExportsOnlyView(): void
    {
        $branchClient = $this->createMock(BranchAwareClient::class);
        $branchClient->expects($this->once())
            ->method('apiPostJson')
            ->with('workspaces/456/load', [
                'input' => [
                    [
                        'source' => 'in.c-test-bucket.table1',
                        'destination' => 'table1',
                        'sourceBranchId' => 123,
                        'overwrite' => false,
                        'useView' => true, // VIEW type sets useView=true
                    ],
                ],
                'preserve' => 1,
            ], false)
            ->willReturn(['id' => 300]);

        $branchClient->expects($this->once())
            ->method('handleAsyncTasks')
            ->with([300])
            ->willReturn([['id' => 300, 'status' => 'success']]);

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->exactly(2))
            ->method('getBranchClient')
            ->willReturn($branchClient);

        $dataStorage = $this->createMock(WorkspaceStagingInterface::class);
        $dataStorage->expects($this->once())
            ->method('getWorkspaceId')
            ->willReturn('456');

        $metadataStorage = $this->createMock(FileStagingInterface::class);
        $metadataStorage->expects($this->once())
            ->method('getPath')
            ->willReturn('/tmp/data');

        $strategy = $this->createTestStrategyWithDataStorage(
            $clientWrapper,
            'bigquery',
            $dataStorage,
            $metadataStorage,
        );

        $table1 = new RewrittenInputTableOptions(
            [
                'source' => 'in.c-test-bucket.table1',
                'destination' => 'table1',
            ],
            'in.c-test-bucket.table1',
            123,
            $this->createTableInfo('in.c-test-bucket.table1', 'table1', ['col1']),
        );

        $exports = [
            ['type' => 'VIEW', 'table' => [$table1, ['overwrite' => false]]],
        ];

        $result = $strategy->handleExports($exports, true);

        self::assertCount(1, $result);
        self::assertTrue($this->testHandler->hasInfoThatContains('Copying 1 tables to workspace.'));
    }

    public function testHandleExportsMixedAllThreeTypes(): void
    {
        $branchClient = $this->createMock(BranchAwareClient::class);

        $expectedApiCalls = [
            [
                'endpoint' => 'workspaces/456/load-clone',
                'data' => [
                    'input' => [
                        [
                            'source' => 'in.c-test-bucket.table1',
                            'destination' => 'table1',
                            'sourceBranchId' => 123,
                            'overwrite' => false,
                            'dropTimestampColumn' => false,
                        ],
                    ],
                    'preserve' => 1,
                ],
                'async' => false,
                'returnValue' => ['id' => 100],
            ],
            [
                'endpoint' => 'workspaces/456/load',
                'data' => [
                    'input' => [
                        [
                            'source' => 'in.c-test-bucket.table2',
                            'destination' => 'table2',
                            'sourceBranchId' => 124,
                            'overwrite' => true,
                        ],
                        [
                            'source' => 'in.c-test-bucket.table3',
                            'destination' => 'table3',
                            'sourceBranchId' => 125,
                            'overwrite' => false,
                            'useView' => true, // VIEW type includes useView
                        ],
                    ],
                    'preserve' => 1,
                ],
                'async' => false,
                'returnValue' => ['id' => 200],
            ],
        ];

        $branchClient->expects($this->exactly(2))
            ->method('apiPostJson')
            ->willReturnCallback(function (string $endpoint, array $data, bool $async) use (&$expectedApiCalls) {
                $expectedCall = array_shift($expectedApiCalls);
                self::assertNotNull($expectedCall);
                self::assertSame($expectedCall['endpoint'], $endpoint);
                self::assertEquals($expectedCall['data'], $data);
                self::assertSame($expectedCall['async'], $async);
                return $expectedCall['returnValue'];
            });

        $expectedAsyncCalls = [
            ['jobIds' => [100], 'returnValue' => [['id' => 100, 'status' => 'success']]],
            ['jobIds' => [200], 'returnValue' => [['id' => 200, 'status' => 'success']]],
        ];

        $branchClient->expects($this->exactly(2))
            ->method('handleAsyncTasks')
            ->willReturnCallback(function (array $jobIds) use (&$expectedAsyncCalls) {
                $expectedCall = array_shift($expectedAsyncCalls);
                self::assertNotNull($expectedCall);
                self::assertSame($expectedCall['jobIds'], $jobIds);
                return $expectedCall['returnValue'];
            });

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->exactly(3))
            ->method('getBranchClient')
            ->willReturn($branchClient);

        $dataStorage = $this->createMock(WorkspaceStagingInterface::class);
        $dataStorage->expects($this->exactly(2))
            ->method('getWorkspaceId')
            ->willReturn('456');

        $metadataStorage = $this->createMock(FileStagingInterface::class);
        $metadataStorage->expects($this->exactly(3))
            ->method('getPath')
            ->willReturn('/tmp/data');

        $strategy = $this->createTestStrategyWithDataStorage(
            $clientWrapper,
            'snowflake',
            $dataStorage,
            $metadataStorage,
        );

        $table1 = new RewrittenInputTableOptions(
            [
                'source' => 'in.c-test-bucket.table1',
                'destination' => 'table1',
                'keep_internal_timestamp_column' => true,
            ],
            'in.c-test-bucket.table1',
            123,
            $this->createTableInfo('in.c-test-bucket.table1', 'table1', ['col1']),
        );

        $table2 = new RewrittenInputTableOptions(
            [
                'source' => 'in.c-test-bucket.table2',
                'destination' => 'table2',
            ],
            'in.c-test-bucket.table2',
            124,
            $this->createTableInfo('in.c-test-bucket.table2', 'table2', ['col2']),
        );

        $table3 = new RewrittenInputTableOptions(
            [
                'source' => 'in.c-test-bucket.table3',
                'destination' => 'table3',
            ],
            'in.c-test-bucket.table3',
            125,
            $this->createTableInfo('in.c-test-bucket.table3', 'table3', ['col3']),
        );

        $exports = [
            ['type' => 'CLONE', 'table' => $table1],
            ['type' => 'COPY', 'table' => [$table2, ['overwrite' => true]]],
            ['type' => 'VIEW', 'table' => [$table3, ['overwrite' => false]]],
        ];

        $result = $strategy->handleExports($exports, true);

        self::assertCount(2, $result);
        self::assertTrue($this->testHandler->hasInfoThatContains('Cloning 1 tables to workspace.'));
        self::assertTrue($this->testHandler->hasInfoThatContains('Copying 2 tables to workspace.'));
        self::assertTrue($this->testHandler->hasInfoThatContains('Processed 2 workspace exports.'));
    }

    public function testHandleExportsEmptyArray(): void
    {
        $branchClient = $this->createMock(BranchAwareClient::class);
        $branchClient->expects($this->never())->method('apiPostJson');
        $branchClient->expects($this->never())->method('handleAsyncTasks');

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->once())
            ->method('getBranchClient')
            ->willReturn($branchClient);

        $dataStorage = $this->createMock(WorkspaceStagingInterface::class);
        $dataStorage->expects($this->never())
            ->method('getWorkspaceId');

        $strategy = $this->createTestStrategyWithDataStorage($clientWrapper, 'snowflake', $dataStorage);

        $result = $strategy->handleExports([], true);

        self::assertEmpty($result);
        self::assertTrue($this->testHandler->hasInfoThatContains('Processed 0 workspace exports.'));
    }

    public function testHandleExportsVerifiesJobResultsMerging(): void
    {
        $branchClient = $this->createMock(BranchAwareClient::class);

        $branchClient->expects($this->exactly(2))
            ->method('apiPostJson')
            ->willReturnOnConsecutiveCalls(
                ['id' => 100], // Clone job
                ['id' => 200], // Copy job
            );

        $branchClient->expects($this->exactly(2))
            ->method('handleAsyncTasks')
            ->willReturnOnConsecutiveCalls(
                [['id' => 100, 'status' => 'success', 'result' => 'clone_data']], // Clone result
                [['id' => 200, 'status' => 'success', 'result' => 'copy_data']], // Copy result
            );

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->exactly(3))
            ->method('getBranchClient')
            ->willReturn($branchClient);

        $dataStorage = $this->createMock(WorkspaceStagingInterface::class);
        $dataStorage->expects($this->exactly(2))
            ->method('getWorkspaceId')
            ->willReturn('456');

        $metadataStorage = $this->createMock(FileStagingInterface::class);
        $metadataStorage->expects($this->exactly(2))
            ->method('getPath')
            ->willReturn('/tmp/data');

        $strategy = $this->createTestStrategyWithDataStorage(
            $clientWrapper,
            'snowflake',
            $dataStorage,
            $metadataStorage,
        );

        $table1 = new RewrittenInputTableOptions(
            ['source' => 'in.c-test-bucket.table1', 'destination' => 'table1'],
            'in.c-test-bucket.table1',
            123,
            $this->createTableInfo('in.c-test-bucket.table1', 'table1', ['col1']),
        );

        $table2 = new RewrittenInputTableOptions(
            ['source' => 'in.c-test-bucket.table2', 'destination' => 'table2'],
            'in.c-test-bucket.table2',
            124,
            $this->createTableInfo('in.c-test-bucket.table2', 'table2', ['col2']),
        );

        $exports = [
            ['type' => 'CLONE', 'table' => $table1],
            ['type' => 'COPY', 'table' => [$table2, ['overwrite' => false]]],
        ];

        $result = $strategy->handleExports($exports, true);

        // Verify job results are properly merged (line 124 in AbstractWorkspaceStrategy)
        self::assertCount(2, $result);
        self::assertSame('clone_data', $result[0]['result']);
        self::assertSame('copy_data', $result[1]['result']);
    }
}
