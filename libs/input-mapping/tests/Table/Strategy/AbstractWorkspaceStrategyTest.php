<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Strategy;

use InvalidArgumentException;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\Strategy\AbstractWorkspaceStrategy;
use Keboola\InputMapping\Table\Strategy\WorkspaceJobType;
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
use ReflectionClass;

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

    public function testGetters(): void
    {
        $metadataStorage = $this->createMock(FileStagingInterface::class);
        $destination = 'test-destination';

        $strategy = new class(
            $this->createMock(ClientWrapper::class),
            $this->createMock(LoggerInterface::class),
            $this->createMock(WorkspaceStagingInterface::class),
            $metadataStorage,
            $this->createMock(InputTableStateList::class),
            $destination,
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

        $reflection = new ReflectionClass($strategy);

        $getMetadataStorageMethod = $reflection->getMethod('getMetadataStorage');
        $getMetadataStorageMethod->setAccessible(true);
        self::assertSame($metadataStorage, $getMetadataStorageMethod->invoke($strategy));

        $getDestinationMethod = $reflection->getMethod('getDestination');
        $getDestinationMethod->setAccessible(true);
        self::assertSame($destination, $getDestinationMethod->invoke($strategy));
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
        $branchClient->expects($this->once())
            ->method('apiPostJson')
            ->with('workspaces/456/load-clone', [
                'input' => [],
                'preserve' => 0,
            ], false)
            ->willReturn(['id' => 123]);

        $branchClient->expects($this->once())
            ->method('handleAsyncTasks')
            ->with([123])
            ->willReturn([]);

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->exactly(2))
            ->method('getBranchClient')
            ->willReturn($branchClient);

        $dataStorage = $this->createMock(WorkspaceStagingInterface::class);
        $dataStorage->expects($this->atLeastOnce())
            ->method('getWorkspaceId')
            ->willReturn('456');

        $strategy = $this->createTestStrategyWithDataStorage($clientWrapper, 'snowflake', $dataStorage);

        // Create plan with preserve=false (should trigger clean)
        $plan = new WorkspaceLoadPlan(
            [],
            false, // preserve=false should trigger clean
        );

        $result = $strategy->executeTableLoadsToWorkspace($plan);
        self::assertEmpty($result->jobs);
        self::assertTrue($this->testHandler->hasInfoThatContains('Cleaning workspace before loading tables.'));
    }

    public function testExecuteTableLoadsToWorkspaceWithMixedOperations(): void
    {
        $branchClient = $this->createMock(BranchAwareClient::class);

        // Set up expected API calls in execution order:
        // 1. Cleanup: workspaces/{id}/load-clone with empty input (preserve=false trigger)
        // 2. Clone: workspaces/{id}/load-clone with clone instructions
        // 3. Load: workspaces/{id}/load with copy + view instructions batched together
        $expectedApiCalls = [
            [
                // Step 1: Cleanup operation (executed synchronously, not returned in queue)
                'endpoint' => 'workspaces/456/load-clone',
                'data' => [
                    'input' => [], // workspace will be only cleaned
                    'preserve' => 0,
                ],
                'async' => false,
                'returnValue' => ['id' => 100], // cleanup job ID
            ],
            [
                // Step 2: Clone instruction group - both clone operations batched together
                'endpoint' => 'workspaces/456/load-clone',
                'data' => [
                    'input' => [
                        [
                            'source' => 'in.c-test-bucket.table1',
                            'destination' => 'table1',
                            'overwrite' => true,
                            'dropTimestampColumn' => true,
                            'sourceBranchId' => 123,
                        ],
                        [
                            'source' => 'in.c-test-bucket.table2',
                            'destination' => 'table2',
                            'overwrite' => false,
                            'dropTimestampColumn' => false,
                            'sourceBranchId' => 124,
                        ],
                    ],
                    'preserve' => 1,
                ],
                'async' => false,
                'returnValue' => ['id' => 456],
            ],
            [
                // Step 3: Load instruction group - copy + view operations batched together
                'endpoint' => 'workspaces/456/load',
                'data' => [
                    'input' => [
                        [
                            // Copy instruction
                            'source' => 'in.c-test-bucket.table3',
                            'destination' => 'table3',
                            'overwrite' => true,
                            'sourceBranchId' => 125,
                        ],
                        [
                            // View instruction (note: useView=true parameter)
                            'source' => 'in.c-test-bucket.table4',
                            'destination' => 'table4',
                            'overwrite' => false,
                            'sourceBranchId' => 126,
                            'useView' => true,
                        ],
                    ],
                    'preserve' => 1,
                ],
                'async' => false,
                'returnValue' => ['id' => 789],
            ],
        ];

        // Mock API calls with callback verification
        $branchClient->expects($this->exactly(3))
            ->method('apiPostJson')
            ->willReturnCallback(function (string $endpoint, array $data, bool $async) use (&$expectedApiCalls) {
                $expectedCall = array_shift($expectedApiCalls);
                self::assertNotNull($expectedCall);

                self::assertSame($expectedCall['endpoint'], $endpoint);
                self::assertEquals($expectedCall['data'], $data);
                self::assertSame($expectedCall['async'], $async);

                return $expectedCall['returnValue'];
            });

        // Mock handleAsyncTasks for cleanup job completion
        $branchClient->expects($this->once())
            ->method('handleAsyncTasks')
            ->with([100]) // cleanup job ID
            ->willReturn([]);

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->exactly(2))
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

        self::assertCount(2, $result->jobs);

        $cloneJob = $result->jobs[0];
        self::assertSame(WorkspaceJobType::CLONE, $cloneJob->jobType);
        self::assertSame([$cloneTableOptions1, $cloneTableOptions2], $cloneJob->tables);

        $loadJob = $result->jobs[1];
        self::assertSame(WorkspaceJobType::LOAD, $loadJob->jobType);
        self::assertSame([$copyTableOptions, $viewTableOptions], $loadJob->tables);

        self::assertTrue($this->testHandler->hasInfoThatContains('Cleaning workspace before loading tables.'));
        self::assertTrue($this->testHandler->hasInfoThatContains('Cloning 2 tables to workspace.'));
        self::assertTrue($this->testHandler->hasInfoThatContains('Copying 2 tables to workspace.'));
    }

    public function testExecuteTableLoadsToWorkspaceEmptyPlanWithPreserveTrue(): void
    {
        $branchClient = $this->createMock(BranchAwareClient::class);
        $branchClient->expects(self::never())->method(self::anything());

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects(self::once())
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
        $stragegy = new TestWorkspaceStrategy(
            $clientWrapper,
            $this->testLogger,
            $this->createMock(WorkspaceStagingInterface::class),
            $this->createMock(FileStagingInterface::class),
            $this->createMock(InputTableStateList::class),
            'destination',
            FileFormat::Json,
        );
        $stragegy->setWorkspaceType($workspaceType);
        return $stragegy;
    }

    private function createTestStrategyWithDataStorage(
        ClientWrapper $clientWrapper,
        string $workspaceType,
        WorkspaceStagingInterface $dataStorage,
    ): TestWorkspaceStrategy {
        $stragegy = new TestWorkspaceStrategy(
            $clientWrapper,
            $this->testLogger,
            $dataStorage,
            $this->createMock(FileStagingInterface::class),
            $this->createMock(InputTableStateList::class),
            'destination',
            FileFormat::Json,
        );
        $stragegy->setWorkspaceType($workspaceType);
        return $stragegy;
    }
}
