<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\DeferredTasks;

use Keboola\OutputMapping\DeferredTasks\TableWriter\CreateAndLoadTableTask;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\StorageApi\Client;
use PHPUnit\Framework\TestCase;

class CreateAndLoadTableTaskTest extends TestCase
{
    public function testStartImport(): void
    {
        $destinationMock = $this->createMock(MappingDestination::class);
        $destinationMock->expects(self::once())
            ->method('getTableName')
            ->willReturn('test-table');
        $destinationMock->expects(self::once())
            ->method('getBucketId')
            ->willReturn('out.c-test');

        $storageApiMock = $this->createMock(Client::class);
        $storageApiMock->expects($this->once())
            ->method('queueTableCreate')
            ->with(
                'out.c-test',
                [
                    'foo' => 'bar',
                    'name' => 'test-table',
                ],
            )
            ->willReturn('123456')
        ;

        $loadTableTask = new CreateAndLoadTableTask($destinationMock, ['foo' => 'bar'], true);
        $loadTableTask->startImport($storageApiMock);

        self::assertSame('123456', $loadTableTask->getStorageJobId());
        self::assertTrue($loadTableTask->isUsingFreshlyCreatedTable());
    }
}
