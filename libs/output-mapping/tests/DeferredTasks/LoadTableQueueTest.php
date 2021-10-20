<?php

namespace Keboola\OutputMapping\Tests\DeferredTasks;

use Keboola\InputMapping\Table\Result\TableInfo;
use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\DeferredTasks\TableWriter\LoadTableTask;
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

        try {
            $loadQueue->waitForAll();
            self::fail('waitForAll shoud fail with InvalidOutputException-');
        } catch (InvalidOutputException $e) {
            self::assertSame(
                'Failed to load table "myTable": Table with displayName "test" already exists.',
                $e->getMessage()
            );
        }

        $tables = $loadQueue->getTableResult()->getTables();
        self::assertCount(0, iterator_to_array($tables));
    }

    /**
     * @dataProvider waitForAllData
     */
    public function testWaitForAll($expectedTableId, $jobResult)
    {
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
            ->willReturn($jobResult)
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

    public function waitForAllData()
    {
        yield [
            'in.c-myBucket.tableImported',
            [
                'operationName' => 'tableImport',
                'status' => 'success',
                'tableId' => 'in.c-myBucket.tableImported',
            ],
        ];

        yield [
            'in.c-myBucket.tableCreated',
            [
                'operationName' => 'tableCreate',
                'status' => 'success',
                'results' => [
                    'id' => 'in.c-myBucket.tableCreated',
                ]
            ],
        ];
    }
}
