<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\DeferredTasks;

use Keboola\OutputMapping\DeferredTasks\TableWriter\DirectGrantMetadataRefreshTask;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApiBranch\ClientWrapper;
use LogicException;
use PHPUnit\Framework\TestCase;

class DirectGrantMetadataRefreshTaskTest extends TestCase
{
    public function testGetStorageJobIdsBeforeStartThrowsLogicException(): void
    {
        $task = new DirectGrantMetadataRefreshTask(1234);

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('Metadata refresh of direct-grant tables has not been started yet.');
        $task->getStorageJobIds();
    }

    public function testStartWithNothingToRefreshReturnsNoJobIds(): void
    {
        $branchClient = $this->createMock(BranchAwareClient::class);
        $branchClient->expects(self::once())
            ->method('apiPostJson')
            ->with('workspaces/1234/unload?only-direct-grants=1', [], false)
            ->willReturn([])
        ;

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects(self::once())
            ->method('getBranchClient')
            ->willReturn($branchClient)
        ;

        $task = new DirectGrantMetadataRefreshTask(1234);
        $task->start($clientWrapper);

        self::assertSame([], $task->getStorageJobIds());
    }
}
