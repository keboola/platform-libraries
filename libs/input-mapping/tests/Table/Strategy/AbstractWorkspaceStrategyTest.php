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

    public function testPrepareTableLoadsToWorkspaceView(): void
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

        // Table that can use view (BigQuery backend)
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

        // Create a table that will cause checkViableLoadMethod to throw an exception
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

        // This should throw an InvalidInputException because checkViableLoadMethod is called
        $this->expectException(InvalidInputException::class);
        $this->expectExceptionMessage(
            'Table "in.c-test-bucket.table1" is an alias, which is not supported when loading Bigquery tables.',
        );

        $strategy->prepareTableLoadsToWorkspace([$tableOptions]);
    }

    public function testExecuteTableLoadsToWorkspaceWithCleanAndPreserveFalse(): void
    {
        $branchClient = $this->createMock(BranchAwareClient::class);
        $branchClient->expects(self::never())->method(self::anything());

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->once())
            ->method('getBranchClient')
            ->willReturn($branchClient);

        $dataStorage = $this->createMock(WorkspaceStagingInterface::class);

        $strategy = $this->createTestStrategyWithDataStorage($clientWrapper, 'snowflake', $dataStorage);

        $plan = new WorkspaceLoadPlan(
            [],
            false,
        );

        $result = $strategy->executeTableLoadsToWorkspace($plan);
        self::assertEmpty($result->jobs);
    }

    public function testExecuteTableLoadsToWorkspaceWithMixedOperations(): void
    {
        $branchClient = $this->createMock(BranchAwareClient::class);

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
                            'loadType' => 'clone',
                        ],
                        [
                            'source' => 'in.c-test-bucket.table2',
                            'destination' => 'table2',
                            'overwrite' => false,
                            'dropTimestampColumn' => false,
                            'sourceBranchId' => 124,
                            'loadType' => 'clone',
                        ],
                        [
                            'source' => 'in.c-test-bucket.table3',
                            'destination' => 'table3',
                            'overwrite' => true,
                            'sourceBranchId' => 125,
                            'loadType' => 'copy',
                        ],
                        [
                            'source' => 'in.c-test-bucket.table4',
                            'destination' => 'table4',
                            'overwrite' => false,
                            'sourceBranchId' => 126,
                            'useView' => true,
                            'loadType' => 'view',
                        ],
                    ],
                    'preserve' => 0,
                ],
                false,
            )
            ->willReturn(['id' => 456]);

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->once())
            ->method('getBranchClient')
            ->willReturn($branchClient);

        $dataStorage = $this->createMock(WorkspaceStagingInterface::class);
        $dataStorage->expects(self::once())
            ->method('getWorkspaceId')
            ->willReturn('456');

        $strategy = $this->createTestStrategyWithDataStorage($clientWrapper, 'snowflake', $dataStorage);

        $cloneTableOptions1 = new RewrittenInputTableOptions(
            [
                'source' => 'in.c-test-bucket.table1',
                'destination' => 'table1',
                'overwrite' => true,
                'keep_internal_timestamp_column' => false,
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
                'keep_internal_timestamp_column' => true,
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
                    WorkspaceLoadType::CLONE,
                    $cloneTableOptions2,
                    null,
                ),
                new WorkspaceTableLoadInstruction(
                    WorkspaceLoadType::COPY,
                    $copyTableOptions,
                    ['overwrite' => true],
                ),
                new WorkspaceTableLoadInstruction(
                    WorkspaceLoadType::VIEW,
                    $viewTableOptions,
                    ['overwrite' => false],
                ),
            ],
            false,
        );

        $result = $strategy->executeTableLoadsToWorkspace($plan);

        self::assertCount(1, $result->jobs);

        $job = $result->jobs[0];
        self::assertSame(WorkspaceLoadType::COPY, $job->jobType);
        self::assertSame(
            [$cloneTableOptions1, $cloneTableOptions2, $copyTableOptions, $viewTableOptions],
            $job->tables,
        );

        self::assertTrue($this->testHandler->hasInfoThatContains('Loading 4 tables to workspace.'));
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
    ): TestWorkspaceStrategy {
        $strategy = new TestWorkspaceStrategy(
            $clientWrapper,
            $this->testLogger,
            $dataStorage,
            $this->createMock(FileStagingInterface::class),
            $this->createMock(InputTableStateList::class),
            'destination',
            FileFormat::Json,
        );
        $strategy->setWorkspaceType($workspaceType);
        return $strategy;
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
                            'loadType' => 'clone',
                        ],
                        [
                            'source' => 'in.c-test-bucket.table2',
                            'destination' => 'table2',
                            'overwrite' => false,
                            'sourceBranchId' => 124,
                            'loadType' => 'copy',
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

        $cloneTable = new RewrittenInputTableOptions(
            [
                'source' => 'in.c-test-bucket.table1',
                'destination' => 'table1',
                'keep_internal_timestamp_column' => true,
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

        $job = $result->jobs[0];
        self::assertSame(WorkspaceLoadType::COPY, $job->jobType);
        self::assertSame([$cloneTable, $copyTable], $job->tables);
    }

    public function testPrepareAndExecuteTableLoadsWithCleanWorkspace(): void
    {
        $branchClient = $this->createMock(BranchAwareClient::class);

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
                            'loadType' => 'clone',
                        ],
                    ],
                    'preserve' => 0,
                ],
                false,
            )
            ->willReturn(['id' => 789]);

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
        $dataStorage->expects(self::once())
            ->method('getWorkspaceId')
            ->willReturn('456');

        $strategy = $this->createTestStrategyWithDataStorage($clientWrapper, 'snowflake', $dataStorage);

        $cloneTable = new RewrittenInputTableOptions(
            [
                'source' => 'in.c-test-bucket.table1',
                'destination' => 'table1',
                'keep_internal_timestamp_column' => true,
            ],
            'in.c-test-bucket.table1',
            123,
            [
                'id' => 'in.c-test-bucket.table1',
                'bucket' => ['backend' => 'snowflake'],
                'isAlias' => false,
            ],
        );

        $result = $strategy->prepareAndExecuteTableLoads([$cloneTable], false);

        self::assertInstanceOf(WorkspaceLoadQueue::class, $result);
        self::assertCount(1, $result->jobs);
        self::assertTrue($this->testHandler->hasInfoThatContains('Loading 1 tables to workspace.'));
    }
}
