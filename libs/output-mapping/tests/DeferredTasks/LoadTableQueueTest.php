<?php

namespace Keboola\OutputMapping\Tests\DeferredTasks;

use Keboola\InputMapping\Table\Result\TableInfo;
use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\DeferredTasks\TableWriterV1\LoadTableTask;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Metadata;
use PHPUnit\Framework\TestCase;

class LoadTableQueueTest extends TestCase
{
    public function testTaskCount()
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

    public function testStart()
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

    public function testWaitForAllWithError()
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

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Failed to load table "myTable": Table with displayName "test" already exists.');
        $loadQueue->waitForAll();

        $tables = $loadQueue->getTableResult()->getTables();
        self::assertCount(0, iterator_to_array($tables));
    }

    public function testWaitForAll()
    {
        $expectedTableId = 'in.c-myBucket.table';
        $storageApiMock = $this->createMock(Client::class);

        $storageApiMock->expects($this->once())
            ->method('getTable')
            ->with($expectedTableId)
            ->willReturn([
                'id' => $expectedTableId,
                'columns' => [],
            ]);

        $storageApiMock->expects($this->once())
            ->method('waitForJob')
            ->with(123)
            ->willReturn([
                'status' => 'success',
                'tableId' => $expectedTableId,
            ])
        ;

        $loadTask = $this->createMock(LoadTableTask::class);
        $loadTask->expects($this->never())
            ->method('startImport')
        ;

        $loadTask->expects($this->atLeastOnce())
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

        /** @var TableInfo $table */
        $tables = iterator_to_array($loadQueue->getTableResult()->getTables());
        self::assertCount(1, $tables);

        $table = reset($tables);
        self::assertSame($expectedTableId, $table->getId());
    }
}
