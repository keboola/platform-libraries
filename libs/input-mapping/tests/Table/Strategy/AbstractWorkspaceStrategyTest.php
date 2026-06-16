<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Strategy;

use InvalidArgumentException;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\Strategy\AbstractWorkspaceStrategy;
use Keboola\InputMapping\Table\Strategy\WorkspaceLoadJob;
use Keboola\InputMapping\Table\Strategy\WorkspaceLoadQueue;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StagingProvider\Staging\StagingInterface;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStagingInterface;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApiBranch\ClientWrapper;
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
        };
    }

    public function testPrepareAndExecuteTableLoadsEmptyWithPreserveDoesNothing(): void
    {
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->never())->method('getBranchClient');

        $strategy = $this->createTestStrategy($clientWrapper, 'snowflake');

        // No tables and the workspace is preserved: nothing to do, no job submitted.
        $result = $strategy->prepareAndExecuteTableLoads([], true);

        self::assertInstanceOf(WorkspaceLoadQueue::class, $result);
        self::assertEmpty($result->jobs);
        self::assertSame(TestWorkspaceStrategy::class, $result->getStrategyClass());
        self::assertSame('destination', $result->getDestination());
    }

    public function testPrepareAndExecuteTableLoadsCleansWorkspaceWhenNotPreserving(): void
    {
        $branchClient = $this->createMock(BranchAwareClient::class);

        // Empty input + preserve=false still submits a job that purges the workspace.
        $branchClient->expects($this->once())
            ->method('apiPostJson')
            ->with('workspaces/456/input-mapping-load', ['input' => [], 'preserve' => 0], false)
            ->willReturn(['id' => 123]);
        $branchClient->expects($this->never())->method('handleAsyncTasks');

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->once())->method('getBranchClient')->willReturn($branchClient);

        $dataStorage = $this->createMock(WorkspaceStagingInterface::class);
        $dataStorage->expects($this->once())->method('getWorkspaceId')->willReturn('456');

        $strategy = $this->createTestStrategyWithDataStorage($clientWrapper, 'snowflake', $dataStorage);

        $result = $strategy->prepareAndExecuteTableLoads([], false);

        self::assertCount(1, $result->jobs);
        self::assertTrue($this->testHandler->hasInfoThatContains('Cleaning workspace and loading tables.'));
    }

    public function testPrepareAndExecuteTableLoadsSubmitsConfigToInputMappingLoadEndpoint(): void
    {
        $cloneTable = new RewrittenInputTableOptions(
            ['source' => 'in.c-test-bucket.table1', 'destination' => 'table1'],
            'in.c-test-bucket.table1',
            123,
            ['id' => 'in.c-test-bucket.table1', 'bucket' => ['backend' => 'snowflake'], 'isAlias' => false],
        );
        // An explicit load_type is passed through verbatim; the endpoint resolves the actual method.
        $viewTable = new RewrittenInputTableOptions(
            ['source' => 'in.c-test-bucket.table2', 'destination' => 'table2', 'load_type' => 'VIEW'],
            'in.c-test-bucket.table2',
            124,
            ['id' => 'in.c-test-bucket.table2', 'bucket' => ['backend' => 'snowflake'], 'isAlias' => false],
        );

        $branchClient = $this->createMock(BranchAwareClient::class);
        $branchClient->expects($this->once())
            ->method('apiPostJson')
            ->with(
                'workspaces/456/input-mapping-load',
                self::callback(function (array $options): bool {
                    self::assertSame(0, $options['preserve']);
                    self::assertCount(2, $options['input']);

                    // The parsed snake_case configuration is forwarded unchanged - no camelCase
                    // translation and no client-side loadType.
                    self::assertSame('in.c-test-bucket.table1', $options['input'][0]['source']);
                    self::assertSame('table1', $options['input'][0]['destination']);
                    self::assertSame(123, $options['input'][0]['source_branch_id']);
                    self::assertArrayNotHasKey('load_type', $options['input'][0]);

                    self::assertSame('in.c-test-bucket.table2', $options['input'][1]['source']);
                    self::assertSame('table2', $options['input'][1]['destination']);
                    self::assertSame('VIEW', $options['input'][1]['load_type']);
                    return true;
                }),
                false,
            )
            ->willReturn(['id' => 789]);
        $branchClient->expects($this->never())->method('handleAsyncTasks');

        $clientWrapper = $this->createMock(ClientWrapper::class);
        // The load type is decided server-side now, so the token is never inspected client-side.
        $clientWrapper->expects($this->never())->method('getToken');
        $clientWrapper->expects($this->once())->method('getBranchClient')->willReturn($branchClient);

        $dataStorage = $this->createMock(WorkspaceStagingInterface::class);
        $dataStorage->expects($this->once())->method('getWorkspaceId')->willReturn('456');

        $strategy = $this->createTestStrategyWithDataStorage($clientWrapper, 'snowflake', $dataStorage);

        $result = $strategy->prepareAndExecuteTableLoads([$cloneTable, $viewTable], false);

        // Single job carrying all tables, with the creating strategy's identity.
        self::assertCount(1, $result->jobs);
        self::assertSame([$cloneTable, $viewTable], $result->jobs[0]->tables);
        self::assertSame(TestWorkspaceStrategy::class, $result->getStrategyClass());
        self::assertSame('destination', $result->getDestination());
        self::assertTrue($this->testHandler->hasInfoThatContains('Loading 2 tables to workspace.'));
    }

    public function testPrepareAndExecuteTableLoadsPreservesWorkspace(): void
    {
        $table = new RewrittenInputTableOptions(
            ['source' => 'in.c-test-bucket.table1', 'destination' => 'table1'],
            'in.c-test-bucket.table1',
            123,
            ['id' => 'in.c-test-bucket.table1', 'bucket' => ['backend' => 'snowflake'], 'isAlias' => false],
        );

        $branchClient = $this->createMock(BranchAwareClient::class);
        $branchClient->expects($this->once())
            ->method('apiPostJson')
            ->with(
                'workspaces/456/input-mapping-load',
                self::callback(function (array $options): bool {
                    self::assertSame(1, $options['preserve']);
                    return true;
                }),
                false,
            )
            ->willReturn(['id' => 789]);

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->once())->method('getBranchClient')->willReturn($branchClient);

        $dataStorage = $this->createMock(WorkspaceStagingInterface::class);
        $dataStorage->expects($this->once())->method('getWorkspaceId')->willReturn('456');

        $strategy = $this->createTestStrategyWithDataStorage($clientWrapper, 'snowflake', $dataStorage);

        $result = $strategy->prepareAndExecuteTableLoads([$table], true);

        self::assertCount(1, $result->jobs);
        // preserve=true keeps the workspace, so the cleanup message is not logged.
        self::assertFalse($this->testHandler->hasInfoThatContains('Cleaning workspace and loading tables.'));
    }

    public function testWaitForTableLoadCompletionWritesManifestsAndBuildsResult(): void
    {
        $tmpDir = sys_get_temp_dir() . '/' . uniqid('im-test-', true);
        mkdir($tmpDir, 0777, true);

        try {
            $jobResults = [
                [
                    'id' => 100,
                    'status' => 'success',
                    'tableId' => 'in.c-test-bucket.table1',
                    'metrics' => ['outBytes' => 1024, 'outBytesUncompressed' => 2048],
                ],
            ];

            $branchClient = $this->createMock(BranchAwareClient::class);
            $branchClient->expects($this->once())
                ->method('handleAsyncTasks')
                // the queue's job id ('100') is what waitForTableLoadCompletion passes to handleAsyncTasks
                ->with(['100'])
                ->willReturn($jobResults);

            $clientWrapper = $this->createMock(ClientWrapper::class);
            $clientWrapper->expects($this->once())
                ->method('getBranchClient')
                ->willReturn($branchClient);

            $metadataStorage = $this->createMock(FileStagingInterface::class);
            $metadataStorage->expects($this->exactly(2))
                ->method('getPath')
                ->willReturn($tmpDir);

            $strategy = $this->createTestStrategyWithDataStorage(
                $clientWrapper,
                'snowflake',
                $this->createMock(WorkspaceStagingInterface::class),
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

            $queue = new WorkspaceLoadQueue([
                new WorkspaceLoadJob('100', [$table1, $table2]),
            ], TestWorkspaceStrategy::class, 'destination');

            $result = $strategy->waitForTableLoadCompletion($queue);

            self::assertCount(2, $result->getTables());

            $metrics = $result->getMetrics();
            self::assertNotNull($metrics);
            self::assertCount(1, $metrics->getTableMetrics());
            self::assertSame('in.c-test-bucket.table1', $metrics->getTableMetrics()[0]->getTableId());

            // state list carries lastImportDate from the loaded tables (used for incremental loads)
            $stateList = $result->getInputTableStateList();
            self::assertSame(
                '2022-06-03T03:31:43+0200',
                $stateList->getTable('in.c-test-bucket.table1')->getLastImportDate(),
            );

            // manifest is actually written to disk, not just triggered
            $manifest = json_decode(
                (string) file_get_contents($tmpDir . '/destination/table1.manifest'),
                true,
                flags: JSON_THROW_ON_ERROR,
            );
            self::assertIsArray($manifest);
            self::assertSame('in.c-test-bucket.table1', $manifest['id']);
            self::assertSame('2022-06-03T03:31:43+0200', $manifest['last_import_date']);

            self::assertTrue($this->testHandler->hasInfoThatContains('Processed 1 workspace exports.'));
            self::assertTrue($this->testHandler->hasInfoThatContains('Fetched table in.c-test-bucket.table1.'));
            self::assertTrue($this->testHandler->hasInfoThatContains('Fetched table in.c-test-bucket.table2.'));
            self::assertTrue($this->testHandler->hasInfoThatContains('All tables were fetched.'));
        } finally {
            array_map('unlink', glob($tmpDir . '/destination/*.manifest') ?: []);
            @rmdir($tmpDir . '/destination');
            rmdir($tmpDir);
        }
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
            'displayName' => $tableName,
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
}
