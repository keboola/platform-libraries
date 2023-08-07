<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\DeferredTasks;

use Generator;
use Keboola\InputMapping\Table\Result\TableInfo;
use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\DeferredTasks\TableWriter\LoadTableTask;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Table\Result;
use Keboola\OutputMapping\Table\Result\Metrics;
use Keboola\OutputMapping\Table\Result\TableMetrics;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApiBranch\ClientWrapper;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class LoadTableQueueTest extends TestCase
{
    public function testTaskCount(): void
    {
        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getTableAndFileStorageClient')
            ->willReturn($this->createMock(Client::class));

        $loadQueue = new LoadTableQueue(
            $clientWrapperMock,
            new NullLogger(),
            [
                $this->createMock(LoadTableTask::class),
                $this->createMock(LoadTableTask::class),
            ]
        );

        self::assertSame(2, $loadQueue->getTaskCount());
    }

    public function testStart(): void
    {
        $loadTask = $this->createMock(LoadTableTask::class);
        $loadTask->expects(self::once())
            ->method('startImport')
            ->with($this->callback(function ($client) {
                self::assertInstanceOf(Client::class, $client);
                return true;
            }))
        ;
        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getTableAndFileStorageClient')
            ->willReturn($this->createMock(Client::class));

        $loadQueue = new LoadTableQueue($clientWrapperMock, new NullLogger(), [$loadTask]);
        $loadQueue->start();
    }

    public function testStartFailureWithSapiUserErrorThrowsInvalidOutputException(): void
    {
        $clientException = new ClientException('Hi', 444);

        $loadTask = $this->createMock(LoadTableTask::class);
        $loadTask->expects(self::once())
            ->method('startImport')
            ->with($this->callback(function ($client) {
                self::assertInstanceOf(Client::class, $client);
                return true;
            }))
            ->willThrowException($clientException)
        ;
        $loadTask->expects(self::once())
            ->method('getDestinationTableName')
            ->willReturn('out.c-test.test-table')
        ;

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getTableAndFileStorageClient')
            ->willReturn($this->createMock(Client::class));

        try {
            $loadQueue = new LoadTableQueue($clientWrapperMock, new NullLogger(), [$loadTask]);
            $loadQueue->start();
            self::fail('LoadTableQueue should fail with InvalidOutputException');
        } catch (InvalidOutputException $e) {
            self::assertSame('Hi [out.c-test.test-table]', $e->getMessage());
            self::assertSame(444, $e->getCode());
            self::assertSame($clientException, $e->getPrevious());
        }
    }

    public function testStartFailureWithSapiAppErrorPropagatesErrorFromClient(): void
    {
        $clientException = new ClientException('Hi', 500);

        $loadTask = $this->createMock(LoadTableTask::class);
        $loadTask->expects(self::once())
            ->method('startImport')
            ->with($this->callback(function ($client) {
                self::assertInstanceOf(Client::class, $client);
                return true;
            }))
            ->willThrowException($clientException)
        ;

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getTableAndFileStorageClient')
            ->willReturn($this->createMock(Client::class));

        try {
            $loadQueue = new LoadTableQueue($clientWrapperMock, new NullLogger(), [$loadTask]);
            $loadQueue->start();
            self::fail('LoadTableQueue should fail with ClientException');
        } catch (ClientException $e) {
            self::assertSame($clientException, $e);
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

        $loadTask = $this->createMock(LoadTableTask::class);
        $loadTask->expects(self::never())
            ->method('startImport')
        ;
        $loadTask->expects(self::exactly(2))
            ->method('getDestinationTableName')
            ->willReturn('myTable')
        ;
        $loadTask->expects($this->atLeastOnce())
            ->method('getStorageJobId')
            ->willReturn('123')
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
                $e->getMessage()
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
            ->method('startImport')
        ;
        $loadTask->expects(self::once())
            ->method('getDestinationTableName')
            ->willReturn('myTable')
        ;
        $loadTask->expects(self::once())
            ->method('getStorageJobId')
            ->willReturn('123')
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
                $e->getMessage()
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
        int $expectedUncompressedBytes
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
            ->method('startImport')
        ;
        $loadTask->expects(self::once())
            ->method('getStorageJobId')
            ->willReturn('123')
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
        int $expectedUncompressedBytes
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
            ->method('startImport')
        ;
        $loadTask->expects(self::once())
            ->method('getStorageJobId')
            ->willReturn('123')
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
            ->willReturn(['rowsCount' => 0, 'metadata' => []]);

        $loadTask = $this->createMock(LoadTableTask::class);
        $loadTask->method('isUsingFreshlyCreatedTable')
            ->willReturn(true);
        $loadTask->method('getDestinationTableName')
            ->willReturn('my-table');

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
}
