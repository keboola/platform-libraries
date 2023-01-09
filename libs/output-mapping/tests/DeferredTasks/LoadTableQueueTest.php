<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\DeferredTasks;

use Generator;
use Keboola\InputMapping\Table\Result\TableInfo;
use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\DeferredTasks\TableWriter\LoadTableTask;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Table\Result\TableMetrics;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use PHPUnit\Framework\TestCase;

class LoadTableQueueTest extends TestCase
{
    public function testTaskCount(): void
    {
        $loadQueue = new LoadTableQueue(
            $this->createMock(Client::class),
            [
                $this->createMock(LoadTableTask::class),
                $this->createMock(LoadTableTask::class)
            ]
        );

        self::assertSame(2, $loadQueue->getTaskCount());
    }

    public function testStart(): void
    {
        $storageApiMock = $this->createMock(Client::class);

        $loadTask = $this->createMock(LoadTableTask::class);
        $loadTask->expects($this->once())
            ->method('startImport')
            ->with($this->callback(function ($client) {
                self::assertInstanceOf(Client::class, $client);
                return true;
            }))
        ;

        $loadQueue = new LoadTableQueue($storageApiMock, [$loadTask]);
        $loadQueue->start();
    }

    public function testStartFailureWithSapiUserErrorThrowsInvalidOutputException(): void
    {
        $storageApiMock = $this->createMock(Client::class);

        $clientException = new ClientException('Hi', 444);

        $loadTask = $this->createMock(LoadTableTask::class);
        $loadTask->expects($this->once())
            ->method('startImport')
            ->with($this->callback(function ($client) {
                self::assertInstanceOf(Client::class, $client);
                return true;
            }))
            ->willThrowException($clientException)
        ;
        $loadTask->expects($this->once())
            ->method('getDestinationTableName')
            ->willReturn('out.c-test.test-table')
        ;

        try {
            $loadQueue = new LoadTableQueue($storageApiMock, [$loadTask]);
            $loadQueue->start();
            $this->fail('LoadTableQueue should fail with InvalidOutputException');
        } catch (InvalidOutputException $e) {
            self::assertSame('Hi [out.c-test.test-table]', $e->getMessage());
            self::assertSame(444, $e->getCode());
            self::assertSame($clientException, $e->getPrevious());
        }
    }

    public function testStartFailureWithSapiAppErrorPropagatesErrorFromClient(): void
    {
        $storageApiMock = $this->createMock(Client::class);

        $clientException = new ClientException('Hi', 500);

        $loadTask = $this->createMock(LoadTableTask::class);
        $loadTask->expects($this->once())
            ->method('startImport')
            ->with($this->callback(function ($client) {
                self::assertInstanceOf(Client::class, $client);
                return true;
            }))
            ->willThrowException($clientException)
        ;

        try {
            $loadQueue = new LoadTableQueue($storageApiMock, [$loadTask]);
            $loadQueue->start();
            $this->fail('LoadTableQueue should fail with ClientException');
        } catch (ClientException $e) {
            self::assertSame($clientException, $e);
        }
    }

    public function testWaitForAllWithErrorThrowsInvalidOutputException(): void
    {
        $storageApiMock = $this->createMock(Client::class);

        $storageApiMock->expects($this->once())
            ->method('waitForJob')
            ->with(123)
            ->willReturn([
                'status' => 'error',
                'error' => [
                    'message' => 'Table with displayName "test" already exists.'
                ]
            ])
        ;

        $loadTask = $this->createMock(LoadTableTask::class);
        $loadTask->expects($this->never())
            ->method('startImport')
        ;

        $loadTask->expects($this->once())
            ->method('getDestinationTableName')
            ->willReturn('myTable');

        $loadTask->expects($this->atLeastOnce())
            ->method('getStorageJobId')
            ->willReturn(123)
        ;

        $loadQueue = new LoadTableQueue($storageApiMock, [$loadTask]);

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

        $tables = $tablesResult->getTables();
        self::assertCount(0, iterator_to_array($tables));

        $tablesMetrics = $tablesResult->getMetrics()->getTableMetrics();
        self::assertCount(0, iterator_to_array($tablesMetrics));
    }

    public function testWaitForAllWithSapiUserErrorOnMetadataApplyThrowsInvalidOutputException(): void
    {
        $storageApiMock = $this->createMock(Client::class);

        $storageApiMock->expects($this->once())
            ->method('waitForJob')
            ->with(123)
            ->willReturn([
                'status' => 'error',
                'error' => [
                    'message' => 'Table with displayName "test" already exists.'
                ]
            ])
        ;

        $loadTask = $this->createMock(LoadTableTask::class);
        $loadTask->expects($this->never())
            ->method('startImport')
        ;

        $loadTask->expects($this->once())
            ->method('getDestinationTableName')
            ->willReturn('myTable');

        $loadTask->expects($this->atLeastOnce())
            ->method('getStorageJobId')
            ->willReturn(123)
        ;

        $loadQueue = new LoadTableQueue($storageApiMock, [$loadTask]);

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

        $tables = $tablesResult->getTables();
        self::assertCount(0, iterator_to_array($tables));

        $tablesMetrics = $tablesResult->getMetrics()->getTableMetrics();
        self::assertCount(0, iterator_to_array($tablesMetrics));
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
        $storageApiMock = $this->createMock(Client::class);

        $storageApiMock->expects($this->once())
            ->method('getTable')
            ->with($expectedTableId)
            ->willReturn([
                'id' => $expectedTableId,
                'displayName' => 'my-name',
                'name' => 'my-name',
                'columns' => [],
                'lastImportDate' => null,
                'lastChangeDate' => null,
            ]);

        $storageApiMock->expects($this->once())
            ->method('waitForJob')
            ->with(123)
            ->willReturn($jobResult)
        ;

        $loadTask = $this->createMock(LoadTableTask::class);
        $loadTask->expects($this->never())
            ->method('startImport')
        ;

        $loadTask->expects($this->once())
            ->method('getStorageJobId')
            ->willReturn(123)
        ;

        $loadTask->expects($this->once())
            ->method('applyMetadata')
            ->with($this->callback(function ($client) {
                self::assertInstanceOf(Metadata::class, $client);
                return true;
            }))
        ;

        $loadQueue = new LoadTableQueue($storageApiMock, [$loadTask]);
        $loadQueue->waitForAll();

        $tablesResult = $loadQueue->getTableResult();

        $tables = iterator_to_array($tablesResult->getTables());
        self::assertCount(1, $tables);

        /** @var TableInfo $table */
        $table = reset($tables);
        self::assertSame($expectedTableId, $table->getId());

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
    public function testWaitForAllWithSapiAppErrorPropagatesErrorFromClient(
        array $jobResult,
        string $expectedTableId,
        int $expectedCompressedBytes,
        int $expectedUncompressedBytes
    ): void {
        $storageApiMock = $this->createMock(Client::class);

        $storageApiMock->expects($this->once())
            ->method('waitForJob')
            ->with(123)
            ->willReturn($jobResult)
        ;

        $loadTask = $this->createMock(LoadTableTask::class);
        $loadTask->expects($this->never())
            ->method('startImport')
        ;
        $loadTask->expects($this->once())
            ->method('getStorageJobId')
            ->willReturn(123)
        ;

        $clientException = new ClientException('Hi', 500);

        $loadTask->expects($this->once())
            ->method('applyMetadata')
            ->willThrowException($clientException)
        ;

        $loadQueue = new LoadTableQueue($storageApiMock, [$loadTask]);
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
                ]
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
                ]
            ],
            'in.c-myBucket.tableCreated',
            0,
            5,
        ];
    }
}
