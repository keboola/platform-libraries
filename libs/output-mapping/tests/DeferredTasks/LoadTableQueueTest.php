<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\DeferredTasks;

use Generator;
use Keboola\InputMapping\Table\Result\TableInfo;
use Keboola\OutputMapping\DeferredTasks\DeferredTaskInterface;
use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\DeferredTasks\TableWriter\LoadTableTask;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Storage\TableDescription;
use Keboola\OutputMapping\Table\Result;
use Keboola\OutputMapping\Table\Result\Metrics;
use Keboola\OutputMapping\Table\Result\TableMetrics;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApiBranch\ClientWrapper;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class LoadTableQueueTest extends TestCase
{
    public function testGetLoadTableTasks(): void
    {
        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getTableAndFileStorageClient')
            ->willReturn($this->createMock(Client::class));

        $loadTask1 = $this->createMock(LoadTableTask::class);
        $loadTask2 = $this->createMock(LoadTableTask::class);

        $loadQueue = new LoadTableQueue(
            $clientWrapperMock,
            new NullLogger(),
            [
                $loadTask1,
                $loadTask2,
            ],
        );

        self::assertSame([$loadTask1, $loadTask2], $loadQueue->getLoadTableTasks());
    }

    public function testTaskCount(): void
    {
        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getTableAndFileStorageClient')
            ->willReturn($this->createMock(Client::class));

        $loadTask1 = $this->createMock(LoadTableTask::class);
        $loadTask1->method('getStorageJobIds')->willReturn(['123']);
        $loadTask2 = $this->createMock(LoadTableTask::class);
        $loadTask2->method('getStorageJobIds')->willReturn(['456']);

        $loadQueue = new LoadTableQueue($clientWrapperMock, new NullLogger(), [$loadTask1, $loadTask2]);

        self::assertSame(2, $loadQueue->getTaskCount());
    }

    public function testTaskCountCountsEveryStorageJobOfATask(): void
    {
        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getTableAndFileStorageClient')
            ->willReturn($this->createMock(Client::class));

        $loadTask = $this->createMock(LoadTableTask::class);
        $loadTask->method('getStorageJobIds')->willReturn(['123']);

        $refreshTask = $this->createMock(DeferredTaskInterface::class);
        $refreshTask->method('getStorageJobIds')->willReturn(['456', '789']);

        $loadQueue = new LoadTableQueue($clientWrapperMock, new NullLogger(), [$loadTask, $refreshTask]);

        self::assertSame(3, $loadQueue->getTaskCount());
    }

    public function testWaitForAllWaitsForTaskWithoutDestinationTable(): void
    {
        $expectedTableId = 'in.c-myBucket.myTable';
        $awaitedJobIds = [];

        $branchClientMock = $this->createMock(BranchAwareClient::class);
        $branchClientMock->expects(self::exactly(2))
            ->method('waitForJob')
            ->willReturnCallback(
                function ($jobId) use (&$awaitedJobIds, $expectedTableId): array {
                    $awaitedJobIds[] = $jobId;

                    if ($jobId === '456') {
                        return [
                            'operationName' => 'refreshStorageBuckets',
                            'status' => 'success',
                        ];
                    }

                    return [
                        'operationName' => 'tableImport',
                        'status' => 'success',
                        'tableId' => $expectedTableId,
                        'metrics' => [
                            'inBytes' => 123,
                            'inBytesUncompressed' => 456,
                        ],
                    ];
                },
            )
        ;

        $clientMock = $this->createMock(Client::class);
        $clientMock->expects(self::once())
            ->method('getTable')
            ->with($expectedTableId)
            ->willReturn([
                'id' => $expectedTableId,
                'displayName' => 'my-name',
                'name' => 'my-name',
                'columns' => [],
                'lastImportDate' => null,
                'lastChangeDate' => null,
            ])
        ;

        $loadTask = $this->createMock(LoadTableTask::class);
        $loadTask->expects(self::once())
            ->method('getStorageJobIds')
            ->willReturn(['123'])
        ;
        $loadTask->expects(self::once())
            ->method('applyMetadata')
        ;

        $refreshTask = $this->createMock(DeferredTaskInterface::class);
        $refreshTask->expects(self::once())
            ->method('getStorageJobIds')
            ->willReturn(['456'])
        ;

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getTableAndFileStorageClient')
            ->willReturn($clientMock);
        $clientWrapperMock->method('getBranchClient')
            ->willReturn($branchClientMock);

        $loadQueue = new LoadTableQueue($clientWrapperMock, new NullLogger(), [$loadTask, $refreshTask]);
        $jobIds = $loadQueue->waitForAll();

        self::assertSame(['123', '456'], $awaitedJobIds);
        self::assertSame(['123', '456'], $jobIds);

        // a job without a destination table contributes no table metrics
        $tablesMetrics = $loadQueue->getTableResult()->getMetrics()?->getTableMetrics();
        self::assertNotNull($tablesMetrics);
        self::assertCount(1, $tablesMetrics);
    }

    public function testWaitForAllWithFailedTaskWithoutDestinationTableThrowsInvalidOutputException(): void
    {
        $jobResult = [
            'operationName' => 'refreshStorageBuckets',
            'status' => 'error',
            'error' => [
                'message' => 'Workspace "1234" not found.',
            ],
        ];

        $branchClientMock = $this->createMock(BranchAwareClient::class);
        $branchClientMock->expects(self::once())
            ->method('waitForJob')
            ->with('456')
            ->willReturn($jobResult)
        ;

        $refreshTask = $this->createMock(DeferredTaskInterface::class);
        $refreshTask->expects(self::once())
            ->method('getStorageJobIds')
            ->willReturn(['456'])
        ;
        $refreshTask->expects(self::once())
            ->method('getFailedJobError')
            ->with('456', $jobResult)
            ->willReturn('Failed to refresh metadata of direct-grant tables (Storage job "456"): '
                . 'Workspace "1234" not found.')
        ;

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getTableAndFileStorageClient')
            ->willReturn($this->createMock(Client::class));
        $clientWrapperMock->method('getBranchClient')
            ->willReturn($branchClientMock);

        $loadQueue = new LoadTableQueue($clientWrapperMock, new NullLogger(), [$refreshTask]);

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage(
            'Failed to refresh metadata of direct-grant tables (Storage job "456"): Workspace "1234" not found.',
        );
        $loadQueue->waitForAll();
    }

    public function testStart(): void
    {
        $clientWrapperMock = $this->createMock(ClientWrapper::class);

        $loadTask = $this->createMock(LoadTableTask::class);
        $loadTask->expects(self::once())
            ->method('start')
            ->with($clientWrapperMock)
        ;

        $loadQueue = new LoadTableQueue($clientWrapperMock, new NullLogger(), [$loadTask]);
        $loadQueue->start();
    }

    public function testStartPropagatesFailureOfTask(): void
    {
        $clientException = new ClientException('Hi', 444);

        $loadTask = $this->createMock(LoadTableTask::class);
        $loadTask->expects(self::once())
            ->method('start')
            ->willThrowException($clientException)
        ;

        $loadQueue = new LoadTableQueue($this->createMock(ClientWrapper::class), new NullLogger(), [$loadTask]);

        try {
            $loadQueue->start();
            self::fail('LoadTableQueue should fail with ClientException');
        } catch (ClientException $e) {
            self::assertSame($clientException, $e);
        }
    }

    public function testStartEnqueuesTasksInOrderUntilOneFails(): void
    {
        $startedTasks = [];

        $firstTask = $this->createMock(DeferredTaskInterface::class);
        $firstTask->expects(self::once())
            ->method('start')
            ->willReturnCallback(function () use (&$startedTasks): void {
                $startedTasks[] = 'first';
            })
        ;

        $failingTask = $this->createMock(LoadTableTask::class);
        $failingTask->expects(self::once())
            ->method('start')
            ->willReturnCallback(function () use (&$startedTasks): void {
                $startedTasks[] = 'failing';
                throw new ClientException('Hi', 444);
            })
        ;

        $lastTask = $this->createMock(LoadTableTask::class);
        $lastTask->expects(self::never())
            ->method('start')
        ;

        $loadQueue = new LoadTableQueue(
            $this->createMock(ClientWrapper::class),
            new NullLogger(),
            [$firstTask, $failingTask, $lastTask],
        );

        try {
            $loadQueue->start();
            self::fail('LoadTableQueue should fail with ClientException');
        } catch (ClientException) {
            self::assertSame(['first', 'failing'], $startedTasks);
        }
    }

    public function testWaitForAllWithErrorThrowsInvalidOutputException(): void
    {
        $clientMock = $this->createMock(BranchAwareClient::class);
        $clientMock->expects(self::once())
            ->method('waitForJob')
            ->with(123)
            ->willReturn([
                'status' => 'error',
                'error' => [
                    'message' => 'Table with displayName "test" already exists.',
                ],
            ])
        ;
        $clientMock->expects(self::once())
            ->method('getTable')
            ->willReturn(['rowsCount' => 0, 'metadata' => [], 'isTyped' => false]);

        $loadTask = $this->createMock(LoadTableTask::class);
        $loadTask->expects(self::never())
            ->method('start')
        ;
        $loadTask->expects(self::exactly(2))
            ->method('getDestinationTableName')
            ->willReturn('myTable')
        ;
        $loadTask->expects($this->atLeastOnce())
            ->method('getStorageJobIds')
            ->willReturn(['123'])
        ;
        $loadTask->expects(self::once())
            ->method('getFailedJobError')
            ->with('123', $this->anything())
            ->willReturn('Failed to load table "myTable": Table with displayName "test" already exists.')
        ;

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getTableAndFileStorageClient')
            ->willReturn($clientMock);
        $clientWrapperMock->method('getBranchClient')
            ->willReturn($clientMock);

        $loadQueue = new LoadTableQueue($clientWrapperMock, new NullLogger(), [$loadTask]);

        try {
            $loadQueue->waitForAll();
            self::fail('WaitForAll shoud fail with InvalidOutputException.');
        } catch (InvalidOutputException $e) {
            self::assertSame(
                'Failed to load table "myTable": Table with displayName "test" already exists.',
                $e->getMessage(),
            );
        }

        $tablesResult = $loadQueue->getTableResult();
        self::assertInstanceOf(Result::class, $tablesResult);

        $tables = $tablesResult->getTables();
        self::assertCount(0, iterator_to_array($tables));

        $tablesMetrics = $tablesResult->getMetrics()?->getTableMetrics();
        self::assertNotNull($tablesMetrics);
        self::assertCount(0, iterator_to_array($tablesMetrics));
    }

    public function testWaitForAllWithSapiUserErrorOnMetadataApplyThrowsInvalidOutputException(): void
    {
        $tableName = 'myTable';
        $expectedTableId = 'in.c-myBucket.' . $tableName;

        $branchClientMock = $this->createMock(BranchAwareClient::class);
        $branchClientMock->expects(self::once())
            ->method('waitForJob')
            ->with(123)
            ->willReturn([
                'operationName' => 'tableImport',
                'status' => 'success',
                'tableId' => $expectedTableId,
                'metrics' => [
                    'inBytes' => 123,
                    'inBytesUncompressed' => 456,
                ],
            ])
        ;

        $clientMock = $this->createMock(Client::class);
        $clientMock->expects(self::once())
            ->method('getTable')
            ->with($expectedTableId)
            ->willReturn([
                'id' => $expectedTableId,
                'displayName' => 'my-name',
                'name' => 'my-name',
                'columns' => [],
                'lastImportDate' => null,
                'lastChangeDate' => null,
            ])
        ;

        $loadTask = $this->createMock(LoadTableTask::class);
        $loadTask->expects(self::never())
            ->method('start')
        ;
        $loadTask->expects(self::once())
            ->method('getDestinationTableName')
            ->willReturn('myTable')
        ;
        $loadTask->expects(self::once())
            ->method('getStorageJobIds')
            ->willReturn(['123'])
        ;

        $clientException = new ClientException('Hi', 444, null, null, ['errors' => ['bar' => 'Kochba']]);

        $loadTask->expects(self::once())
            ->method('applyMetadata')
            ->willThrowException($clientException)
        ;

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getTableAndFileStorageClient')
            ->willReturn($clientMock);
        $clientWrapperMock->method('getBranchClient')
            ->willReturn($branchClientMock);

        $loadQueue = new LoadTableQueue($clientWrapperMock, new NullLogger(), [$loadTask]);

        try {
            $loadQueue->waitForAll();
            self::fail('WaitForAll shoud fail with InvalidOutputException.');
        } catch (InvalidOutputException $e) {
            self::assertSame(
                'Failed to update metadata for table "myTable": Hi ({"bar":"Kochba"})',
                $e->getMessage(),
            );
        }

        $tablesResult = $loadQueue->getTableResult();

        $tables = iterator_to_array($tablesResult->getTables());
        self::assertCount(1, $tables);

        /** @var TableInfo $table */
        $table = reset($tables);
        self::assertSame($expectedTableId, $table->getId());

        $metrics = $tablesResult->getMetrics();
        self::assertInstanceOf(Metrics::class, $metrics);
        $tablesMetrics = iterator_to_array($metrics->getTableMetrics());
        self::assertCount(1, $tablesMetrics);

        /** @var TableMetrics $tableMetric */
        $tableMetric = reset($tablesMetrics);
        self::assertSame($expectedTableId, $tableMetric->getTableId());
        self::assertSame(123, $tableMetric->getCompressedBytes());
        self::assertSame(456, $tableMetric->getUncompressedBytes());
    }

    /**
     * @dataProvider waitForAllData
     */
    public function testWaitForAll(
        array $jobResult,
        string $expectedTableId,
        int $expectedCompressedBytes,
        int $expectedUncompressedBytes,
    ): void {
        $clientMock = $this->createMock(Client::class);
        $clientMock->expects(self::once())
            ->method('getTable')
            ->with($expectedTableId)
            ->willReturn([
                'id' => $expectedTableId,
                'displayName' => 'my-name',
                'name' => 'my-name',
                'columns' => [],
                'lastImportDate' => null,
                'lastChangeDate' => null,
            ])
        ;

        $branchClientMock = $this->createMock(BranchAwareClient::class);
        $branchClientMock->expects(self::once())
            ->method('waitForJob')
            ->with(123)
            ->willReturn($jobResult)
        ;

        $loadTask = $this->createMock(LoadTableTask::class);
        $loadTask->expects(self::never())
            ->method('start')
        ;
        $loadTask->expects(self::once())
            ->method('getStorageJobIds')
            ->willReturn(['123'])
        ;
        $loadTask->expects(self::once())
            ->method('applyMetadata')
            ->with($this->callback(function ($client): bool {
                self::assertInstanceOf(Metadata::class, $client);
                return true;
            }))
        ;

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getTableAndFileStorageClient')
            ->willReturn($clientMock);
        $clientWrapperMock->method('getBranchClient')
            ->willReturn($branchClientMock);

        $loadQueue = new LoadTableQueue($clientWrapperMock, new NullLogger(), [$loadTask]);
        $loadQueue->waitForAll();

        $tablesResult = $loadQueue->getTableResult();

        $tables = iterator_to_array($tablesResult->getTables());
        self::assertCount(1, $tables);

        /** @var TableInfo $table */
        $table = reset($tables);
        self::assertSame($expectedTableId, $table->getId());

        self::assertNotNull($tablesResult->getMetrics());
        $tablesMetrics = iterator_to_array($tablesResult->getMetrics()->getTableMetrics());
        self::assertCount(1, $tablesMetrics);

        /** @var TableMetrics $tableMetric */
        $tableMetric = reset($tablesMetrics);
        self::assertSame($expectedTableId, $tableMetric->getTableId());
        self::assertSame($expectedCompressedBytes, $tableMetric->getCompressedBytes());
        self::assertSame($expectedUncompressedBytes, $tableMetric->getUncompressedBytes());
    }

    /**
     * @dataProvider waitForAllData
     */
    public function testWaitForAllWithSapiAppErrorOnMetadataApplyPropagatesErrorFromClient(
        array $jobResult,
        string $expectedTableId,
        int $expectedCompressedBytes,
        int $expectedUncompressedBytes,
    ): void {
        $clientMock = $this->createMock(Client::class);
        $branchClientMock = $this->createMock(BranchAwareClient::class);
        $branchClientMock->expects(self::once())
            ->method('waitForJob')
            ->with(123)
            ->willReturn($jobResult)
        ;

        $loadTask = $this->createMock(LoadTableTask::class);
        $loadTask->expects(self::never())
            ->method('start')
        ;
        $loadTask->expects(self::once())
            ->method('getStorageJobIds')
            ->willReturn(['123'])
        ;

        $clientException = new ClientException('Hi', 500);

        $loadTask->expects(self::once())
            ->method('applyMetadata')
            ->willThrowException($clientException)
        ;

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getTableAndFileStorageClient')
            ->willReturn($clientMock);
        $clientWrapperMock->method('getBranchClient')
            ->willReturn($branchClientMock);

        $loadQueue = new LoadTableQueue($clientWrapperMock, new NullLogger(), [$loadTask]);
        try {
            $loadQueue->waitForAll();
            self::fail('WaitForAll shoud fail with ClientException.');
        } catch (ClientException $e) {
            self::assertSame($clientException, $e);
        }

        $tablesResult = $loadQueue->getTableResult();
        self::assertCount(0, iterator_to_array($tablesResult->getTables()));
        self::assertNull($tablesResult->getMetrics());
    }

    public function waitForAllData(): Generator
    {
        yield [
            [
                'operationName' => 'tableImport',
                'status' => 'success',
                'tableId' => 'in.c-myBucket.tableImported',
                'metrics' => [
                    'inBytes' => 123,
                    'inBytesUncompressed' => 0,
                ],
            ],
            'in.c-myBucket.tableImported',
            123,
            0,
        ];

        yield [
            [
                'operationName' => 'tableCreate',
                'tableId' => null,
                'status' => 'success',
                'results' => [
                    'id' => 'in.c-myBucket.tableCreated',
                ],
                'metrics' => [
                    'inBytes' => 0,
                    'inBytesUncompressed' => 5,
                ],
            ],
            'in.c-myBucket.tableCreated',
            0,
            5,
        ];
    }

    /**
     * @dataProvider genericVariablesData
     * @param array<string, mixed> $jobResult
     * @param array<string, array<string, int|string>> $expectedGenericVariables
     * @param string[] $getTableColumns
     */
    public function testWaitForAllExtractsGenericVariables(
        array $jobResult,
        string $expectedTableId,
        array $expectedGenericVariables,
        array $getTableColumns = [],
        string $getTableName = 'my-name',
    ): void {
        $clientMock = $this->createMock(Client::class);
        $clientMock->expects(self::once())
            ->method('getTable')
            ->with($expectedTableId)
            ->willReturn([
                'id' => $expectedTableId,
                'displayName' => $getTableName,
                'name' => $getTableName,
                'columns' => $getTableColumns,
                'lastImportDate' => null,
                'lastChangeDate' => null,
            ])
        ;

        $branchClientMock = $this->createMock(BranchAwareClient::class);
        $branchClientMock->expects(self::once())
            ->method('waitForJob')
            ->with(123)
            ->willReturn($jobResult)
        ;

        $loadTask = $this->createMock(LoadTableTask::class);
        $loadTask->expects(self::never())
            ->method('start')
        ;
        $loadTask->expects(self::once())
            ->method('getStorageJobIds')
            ->willReturn(['123'])
        ;
        $loadTask->expects(self::once())
            ->method('applyMetadata')
            ->with($this->callback(function ($client): bool {
                self::assertInstanceOf(Metadata::class, $client);
                return true;
            }))
        ;

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getTableAndFileStorageClient')
            ->willReturn($clientMock);
        $clientWrapperMock->method('getBranchClient')
            ->willReturn($branchClientMock);

        $loadQueue = new LoadTableQueue($clientWrapperMock, new NullLogger(), [$loadTask]);
        $loadQueue->waitForAll();

        self::assertSame($expectedGenericVariables, $loadQueue->getTableResult()->getGenericVariables());
    }

    public function genericVariablesData(): Generator
    {
        yield 'tableImport with importedRowsCount' => [
            'jobResult' => [
                'operationName' => 'tableImport',
                'status' => 'success',
                'tableId' => 'in.c-myBucket.tableImported',
                'results' => [
                    'importedRowsCount' => 42,
                ],
                'metrics' => [
                    'inBytes' => 10,
                    'inBytesUncompressed' => 20,
                ],
            ],
            'expectedTableId' => 'in.c-myBucket.tableImported',
            'expectedGenericVariables' => [
                'in.c-myBucket.tableImported' => ['importedRowsCount' => 42],
            ],
            'getTableColumns' => [],
            'getTableName' => 'tableImported',
        ];

        yield 'tableCreate does not add generic variables' => [
            'jobResult' => [
                'operationName' => 'tableCreate',
                'tableId' => null,
                'status' => 'success',
                'results' => [
                    'id' => 'in.c-myBucket.tableCreated',
                    'name' => 'tableCreated',
                    'columns' => ['id', 'name', 'value'],
                ],
                'metrics' => [
                    'inBytes' => 0,
                    'inBytesUncompressed' => 5,
                ],
            ],
            'expectedTableId' => 'in.c-myBucket.tableCreated',
            'expectedGenericVariables' => [],
            'getTableColumns' => [],
        ];

        yield 'tableImport without importedRowsCount defaults to zero' => [
            'jobResult' => [
                'operationName' => 'tableImport',
                'status' => 'success',
                'tableId' => 'in.c-myBucket.tableImported',
                'metrics' => [
                    'inBytes' => 5,
                    'inBytesUncompressed' => 10,
                ],
            ],
            'expectedTableId' => 'in.c-myBucket.tableImported',
            'expectedGenericVariables' => [
                'in.c-myBucket.tableImported' => ['importedRowsCount' => 0],
            ],
            'getTableColumns' => [],
            'getTableName' => 'tableImported',
        ];

        yield 'tableCreate missing results.name and results.columns still adds no generic variables' => [
            'jobResult' => [
                'operationName' => 'tableCreate',
                'tableId' => null,
                'status' => 'success',
                'results' => [
                    'id' => 'in.c-myBucket.tableCreated',
                ],
                'metrics' => [
                    'inBytes' => 0,
                    'inBytesUncompressed' => 5,
                ],
            ],
            'expectedTableId' => 'in.c-myBucket.tableCreated',
            'expectedGenericVariables' => [],
            'getTableColumns' => [],
        ];
    }

    public function testWaitForAllDeleteTableAfterFailedLoad(): void
    {
        $branchClientMock = $this->createMock(BranchAwareClient::class);
        $branchClientMock->method('waitForJob')
            ->willReturn(['status' => 'error', 'error' => ['message' => 'Hi']]);

        $clientMock = $this->createMock(Client::class);
        $clientMock->expects(self::once())
            ->method('dropTable')
            ->with('my-table', ['force' => true]);
        $clientMock->method('getTable')
            ->willReturn(['rowsCount' => 0, 'metadata' => [], 'isTyped' => false]);

        $loadTask = $this->createMock(LoadTableTask::class);
        $loadTask->method('isUsingFreshlyCreatedTable')
            ->willReturn(true);
        $loadTask->method('getDestinationTableName')
            ->willReturn('my-table');
        $loadTask->method('getStorageJobIds')
            ->willReturn(['123']);
        $loadTask->method('getFailedJobError')
            ->willReturn('Failed to load table "my-table": Hi');

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getTableAndFileStorageClient')
            ->willReturn($clientMock);
        $clientWrapperMock->method('getBranchClient')
            ->willReturn($branchClientMock);

        $loadQueue = new LoadTableQueue($clientWrapperMock, new NullLogger(), [$loadTask]);
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Failed to load table "my-table": Hi');
        $loadQueue->waitForAll();
    }

    public function testLoadCustomVariablesSetsVariablesFromValidJson(): void
    {
        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getTableAndFileStorageClient')
            ->willReturn($this->createMock(Client::class));

        $tmpFile = tempnam(sys_get_temp_dir(), 'variables') . '.json';
        file_put_contents($tmpFile, (string) json_encode(['variables' => ['my_var' => 'hello', 'count' => 42]]));

        $loadQueue = new LoadTableQueue($clientWrapperMock, new NullLogger(), []);
        $loadQueue->loadCustomVariables($tmpFile);

        self::assertSame(['my_var' => 'hello', 'count' => 42], $loadQueue->getTableResult()->getCustomVariables());

        unlink($tmpFile);
    }

    /**
     * The deferred path is only reachable for a table created by the load job itself (CreateAndLoadTableTask),
     * whose job operation is `tableCreate`. Every other created table carries its description already in the
     * create-table-definition payload.
     */
    public function testWaitForAllStoresDescriptionsOfCreatedTable(): void
    {
        $tableId = 'in.c-myBucket.tableCreated';

        $clientMock = $this->createMock(Client::class);
        $clientMock->expects(self::once())
            ->method('getTable')
            ->with($tableId)
            ->willReturn([
                'id' => $tableId,
                'displayName' => 'my-name',
                'name' => 'my-name',
                'columns' => ['col1'],
                'lastImportDate' => null,
                'lastChangeDate' => null,
            ])
        ;
        $clientMock->expects(self::once())
            ->method('updateTableDefinition')
            ->with($tableId, [
                'description' => 'table desc',
                'columns' => [
                    ['name' => 'col1', 'description' => 'col1 desc'],
                ],
            ])
            ->willReturn([])
        ;

        $branchClientMock = $this->createMock(BranchAwareClient::class);
        $branchClientMock->expects(self::once())
            ->method('waitForJob')
            ->with(123)
            ->willReturn([
                'operationName' => 'tableCreate',
                'status' => 'success',
                'tableId' => null,
                'results' => [
                    'id' => $tableId,
                ],
                'metrics' => [
                    'inBytes' => 0,
                    'inBytesUncompressed' => 0,
                ],
            ])
        ;

        $loadTask = $this->createMock(LoadTableTask::class);
        $loadTask->expects(self::once())
            ->method('getStorageJobIds')
            ->willReturn(['123'])
        ;
        $loadTask->expects(self::once())
            ->method('applyMetadata')
        ;
        $loadTask->expects(self::once())
            ->method('getDestinationTableName')
            ->willReturn($tableId)
        ;

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getTableAndFileStorageClient')
            ->willReturn($clientMock);
        $clientWrapperMock->method('getBranchClient')
            ->willReturn($branchClientMock);

        $loadQueue = new LoadTableQueue(
            $clientWrapperMock,
            new NullLogger(),
            [$loadTask],
            [$tableId => new TableDescription($tableId, 'table desc', ['col1' => 'col1 desc'])],
        );
        $loadQueue->waitForAll();
    }

    public function testWaitForAllReportsFailedDescriptionUpdateAsError(): void
    {
        $tableId = 'in.c-myBucket.tableCreated';

        $clientMock = $this->createMock(Client::class);
        $clientMock->expects(self::once())
            ->method('getTable')
            ->with($tableId)
            ->willReturn([
                'id' => $tableId,
                'displayName' => 'my-name',
                'name' => 'my-name',
                'columns' => ['col1'],
                'lastImportDate' => null,
                'lastChangeDate' => null,
            ])
        ;
        $clientMock->expects(self::once())
            ->method('updateTableDefinition')
            ->willThrowException(new ClientException('Backend does not support definition update', 400))
        ;

        $branchClientMock = $this->createMock(BranchAwareClient::class);
        $branchClientMock->expects(self::once())
            ->method('waitForJob')
            ->with(123)
            ->willReturn([
                'operationName' => 'tableCreate',
                'status' => 'success',
                'tableId' => null,
                'results' => [
                    'id' => $tableId,
                ],
                'metrics' => [
                    'inBytes' => 0,
                    'inBytesUncompressed' => 0,
                ],
            ])
        ;

        $loadTask = $this->createMock(LoadTableTask::class);
        $loadTask->method('getStorageJobIds')->willReturn(['123']);
        $loadTask->method('getDestinationTableName')->willReturn($tableId);
        $loadTask->expects(self::once())->method('applyMetadata');

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getTableAndFileStorageClient')
            ->willReturn($clientMock);
        $clientWrapperMock->method('getBranchClient')
            ->willReturn($branchClientMock);

        $loadQueue = new LoadTableQueue(
            $clientWrapperMock,
            new NullLogger(),
            [$loadTask],
            [$tableId => new TableDescription($tableId, 'table desc', [])],
        );

        try {
            $loadQueue->waitForAll();
            self::fail('WaitForAll should fail with InvalidOutputException.');
        } catch (InvalidOutputException $e) {
            self::assertSame(
                sprintf(
                    'Cannot update description of table "%s": Backend does not support definition update',
                    $tableId,
                ),
                $e->getMessage(),
            );
        }
    }

    /**
     * A leftover description means it was never handed over to Storage. It cannot happen today, but it must
     * never be dropped silently.
     */
    public function testWaitForAllWarnsAboutDescriptionsWhichWereNotStored(): void
    {
        $tableId = 'in.c-myBucket.tableImported';

        $clientMock = $this->createMock(Client::class);
        $clientMock->expects(self::once())
            ->method('getTable')
            ->with($tableId)
            ->willReturn([
                'id' => $tableId,
                'displayName' => 'my-name',
                'name' => 'my-name',
                'columns' => ['col1'],
                'lastImportDate' => null,
                'lastChangeDate' => null,
            ])
        ;
        $clientMock->expects(self::never())
            ->method('updateTableDefinition')
        ;

        $branchClientMock = $this->createMock(BranchAwareClient::class);
        $branchClientMock->expects(self::once())
            ->method('waitForJob')
            ->with(123)
            ->willReturn([
                'operationName' => 'tableImport',
                'status' => 'success',
                'tableId' => $tableId,
                'metrics' => [
                    'inBytes' => 0,
                    'inBytesUncompressed' => 0,
                ],
            ])
        ;

        $loadTask = $this->createMock(LoadTableTask::class);
        $loadTask->expects(self::once())->method('getStorageJobIds')->willReturn(['123']);
        $loadTask->expects(self::once())->method('applyMetadata');

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getTableAndFileStorageClient')
            ->willReturn($clientMock);
        $clientWrapperMock->method('getBranchClient')
            ->willReturn($branchClientMock);

        $logHandler = new TestHandler();

        $loadQueue = new LoadTableQueue(
            $clientWrapperMock,
            new Logger('test', [$logHandler]),
            [$loadTask],
            ['in.c-myBucket.leftover' => new TableDescription('in.c-myBucket.leftover', 'table desc', [])],
        );
        $loadQueue->waitForAll();

        self::assertTrue($logHandler->hasWarningThatContains(
            'Description of table(s) "in.c-myBucket.leftover" was not stored.',
        ));
    }

    /**
     * A failed load leaves nothing to describe, so the pending description is expected to disappear without
     * the "was not stored" warning - that warning is about descriptions lost by mistake.
     */
    public function testWaitForAllDoesNotWarnAboutDescriptionsOfFailedLoad(): void
    {
        $tableId = 'in.c-myBucket.tableFailed';

        $branchClientMock = $this->createMock(BranchAwareClient::class);
        $branchClientMock->expects(self::once())
            ->method('waitForJob')
            ->with(123)
            ->willReturn(['status' => 'error', 'error' => ['message' => 'Hi']])
        ;

        $clientMock = $this->createMock(Client::class);
        $clientMock->expects(self::once())
            ->method('getTable')
            ->with($tableId)
            ->willReturn(['rowsCount' => 0, 'metadata' => [], 'isTyped' => false])
        ;
        $clientMock->expects(self::once())
            ->method('dropTable')
            ->with($tableId, ['force' => true])
        ;
        $clientMock->expects(self::never())
            ->method('updateTableDefinition')
        ;

        $loadTask = $this->createMock(LoadTableTask::class);
        $loadTask->expects(self::once())->method('getStorageJobIds')->willReturn(['123']);
        $loadTask->expects(self::once())->method('isUsingFreshlyCreatedTable')->willReturn(true);
        $loadTask->method('getDestinationTableName')->willReturn($tableId);
        $loadTask->method('getFailedJobError')->willReturn(sprintf('Failed to load table "%s": Hi', $tableId));

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getTableAndFileStorageClient')
            ->willReturn($clientMock);
        $clientWrapperMock->method('getBranchClient')
            ->willReturn($branchClientMock);

        $logHandler = new TestHandler();

        $loadQueue = new LoadTableQueue(
            $clientWrapperMock,
            new Logger('test', [$logHandler]),
            [$loadTask],
            [$tableId => new TableDescription($tableId, 'table desc', ['col1' => 'col1 desc'])],
        );

        try {
            $loadQueue->waitForAll();
            self::fail('WaitForAll should fail with InvalidOutputException.');
        } catch (InvalidOutputException $e) {
            self::assertSame(sprintf('Failed to load table "%s": Hi', $tableId), $e->getMessage());
        }

        self::assertFalse($logHandler->hasWarningThatContains('was not stored'));
    }

    /**
     * Several sources may be mapped to the same destination table. The descriptions belong to the table, not
     * to the mapping, so they are stored exactly once and nothing is left over afterwards.
     */
    public function testWaitForAllStoresDescriptionOnceForTwoTasksWithSameDestination(): void
    {
        $tableId = 'in.c-myBucket.tableCreated';
        $tableData = [
            'id' => $tableId,
            'displayName' => 'my-name',
            'name' => 'my-name',
            'columns' => ['col1'],
            'lastImportDate' => null,
            'lastChangeDate' => null,
        ];

        $clientMock = $this->createMock(Client::class);
        $clientMock->expects(self::exactly(2))
            ->method('getTable')
            ->with($tableId)
            ->willReturn($tableData)
        ;
        $clientMock->expects(self::once())
            ->method('updateTableDefinition')
            ->with($tableId, [
                'description' => 'table desc',
                'columns' => [
                    ['name' => 'col1', 'description' => 'col1 desc'],
                ],
            ])
            ->willReturn([])
        ;

        $jobResults = [
            [
                'operationName' => 'tableCreate',
                'status' => 'success',
                'tableId' => null,
                'results' => ['id' => $tableId],
                'metrics' => ['inBytes' => 0, 'inBytesUncompressed' => 0],
            ],
            [
                'operationName' => 'tableImport',
                'status' => 'success',
                'tableId' => $tableId,
                'metrics' => ['inBytes' => 0, 'inBytesUncompressed' => 0],
            ],
        ];

        $branchClientMock = $this->createMock(BranchAwareClient::class);
        $branchClientMock->expects(self::exactly(count($jobResults)))
            ->method('waitForJob')
            ->willReturnCallback(function () use (&$jobResults) {
                return array_shift($jobResults);
            })
        ;

        $loadTasks = [];
        foreach (['123', '456'] as $jobId) {
            $loadTask = $this->createMock(LoadTableTask::class);
            $loadTask->expects(self::once())->method('getStorageJobIds')->willReturn([$jobId]);
            $loadTask->expects(self::once())->method('applyMetadata');
            $loadTask->method('getDestinationTableName')->willReturn($tableId);
            $loadTasks[] = $loadTask;
        }

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getTableAndFileStorageClient')
            ->willReturn($clientMock);
        $clientWrapperMock->method('getBranchClient')
            ->willReturn($branchClientMock);

        $logHandler = new TestHandler();

        $loadQueue = new LoadTableQueue(
            $clientWrapperMock,
            new Logger('test', [$logHandler]),
            $loadTasks,
            [$tableId => new TableDescription($tableId, 'table desc', ['col1' => 'col1 desc'])],
        );
        $loadQueue->waitForAll();

        self::assertFalse($logHandler->hasWarningThatContains('was not stored'));
    }

    public function testLoadCustomVariablesDoesNothingWhenFileMissing(): void
    {
        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getTableAndFileStorageClient')
            ->willReturn($this->createMock(Client::class));

        $loadQueue = new LoadTableQueue($clientWrapperMock, new NullLogger(), []);
        $loadQueue->loadCustomVariables('/nonexistent/path/variables.json');

        self::assertSame([], $loadQueue->getTableResult()->getCustomVariables());
    }

    public function testLoadCustomVariablesIgnoresInvalidJson(): void
    {
        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getTableAndFileStorageClient')
            ->willReturn($this->createMock(Client::class));

        $tmpFile = tempnam(sys_get_temp_dir(), 'result') . '.json';
        file_put_contents($tmpFile, 'not valid json {{{');

        $loadQueue = new LoadTableQueue($clientWrapperMock, new NullLogger(), []);
        $loadQueue->loadCustomVariables($tmpFile);

        self::assertSame([], $loadQueue->getTableResult()->getCustomVariables());

        unlink($tmpFile);
    }
}
