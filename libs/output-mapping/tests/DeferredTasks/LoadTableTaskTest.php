<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\DeferredTasks;

use Keboola\OutputMapping\DeferredTasks\TableWriter\LoadTableTask;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\StorageApi\Client;
use PHPUnit\Framework\TestCase;

class LoadTableTaskTest extends TestCase
{
    public function testStartImport(): void
    {
        $destinationMock = $this->createMock(MappingDestination::class);
        $destinationMock->expects(self::once())
            ->method('getTableId')
            ->willReturn('out.c-test.test-table');

        $storageApiMock = $this->createMock(Client::class);
        $storageApiMock->expects($this->once())
            ->method('queueTableImport')
            ->with('out.c-test.test-table', ['foo' => 'bar'])
            ->willReturn('123456')
        ;

        $loadTableTask = new LoadTableTask($destinationMock, ['foo' => 'bar']);
        $loadTableTask->startImport($storageApiMock);

        self::assertSame('123456', $loadTableTask->getStorageJobId());
    }
}
