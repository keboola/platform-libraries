<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\DeferredTasks;

use Keboola\OutputMapping\DeferredTasks\TableWriter\CreateAndLoadTableTask;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\StorageApi\Client;
use Keboola\StorageApiBranch\ClientWrapper;
use PHPUnit\Framework\TestCase;

class CreateAndLoadTableTaskTest extends TestCase
{
    public function testStart(): void
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

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->expects(self::once())
            ->method('getTableAndFileStorageClient')
            ->willReturn($storageApiMock);

        $loadTableTask = new CreateAndLoadTableTask($destinationMock, ['foo' => 'bar'], true);
        $loadTableTask->start($clientWrapperMock);

        self::assertSame(['123456'], $loadTableTask->getStorageJobIds());
        self::assertTrue($loadTableTask->isUsingFreshlyCreatedTable());
    }
}
