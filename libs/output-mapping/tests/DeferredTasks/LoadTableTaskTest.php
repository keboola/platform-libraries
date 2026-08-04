<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\DeferredTasks;

use Keboola\OutputMapping\DeferredTasks\TableWriter\LoadTableTask;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use LogicException;
use PHPUnit\Framework\TestCase;

class LoadTableTaskTest extends TestCase
{
    public function testStart(): void
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

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->expects(self::once())
            ->method('getTableAndFileStorageClient')
            ->willReturn($storageApiMock);

        $loadTableTask = new LoadTableTask($destinationMock, ['foo' => 'bar'], false);
        $loadTableTask->start($clientWrapperMock);

        self::assertFalse($loadTableTask->isUsingFreshlyCreatedTable());
        self::assertSame(['123456'], $loadTableTask->getStorageJobIds());
    }

    public function testStartFailureWithSapiUserErrorThrowsInvalidOutputException(): void
    {
        $clientException = new ClientException('Hi', 444);

        try {
            $this->startFailingTask($clientException);
            self::fail('Start should fail with InvalidOutputException');
        } catch (InvalidOutputException $e) {
            self::assertSame('Hi [out.c-test.test-table]', $e->getMessage());
            self::assertSame(444, $e->getCode());
            self::assertSame($clientException, $e->getPrevious());
        }
    }

    public function testStartFailureWithSapiAppErrorPropagatesErrorFromClient(): void
    {
        $clientException = new ClientException('Hi', 500);

        try {
            $this->startFailingTask($clientException);
            self::fail('Start should fail with ClientException');
        } catch (ClientException $e) {
            self::assertSame($clientException, $e);
        }
    }

    public function testGetStorageJobIdsBeforeStartThrowsLogicException(): void
    {
        $destinationMock = $this->createMock(MappingDestination::class);
        $destinationMock->method('getTableId')->willReturn('out.c-test.test-table');

        $loadTableTask = new LoadTableTask($destinationMock, [], false);

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('Load of table "out.c-test.test-table" has not been started yet.');
        $loadTableTask->getStorageJobIds();
    }

    public function testGetFailedJobError(): void
    {
        $destinationMock = $this->createMock(MappingDestination::class);
        $destinationMock->method('getTableId')->willReturn('out.c-test.test-table');

        $loadTableTask = new LoadTableTask($destinationMock, [], false);

        self::assertSame(
            'Failed to load table "out.c-test.test-table": Table already exists.',
            $loadTableTask->getFailedJobError('123456', [
                'status' => 'error',
                'error' => ['message' => 'Table already exists.'],
            ]),
        );
    }

    private function startFailingTask(ClientException $clientException): void
    {
        $destinationMock = $this->createMock(MappingDestination::class);
        $destinationMock->method('getTableId')->willReturn('out.c-test.test-table');

        $storageApiMock = $this->createMock(Client::class);
        $storageApiMock->expects(self::once())
            ->method('queueTableImport')
            ->willThrowException($clientException)
        ;

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->expects(self::once())
            ->method('getTableAndFileStorageClient')
            ->willReturn($storageApiMock);

        (new LoadTableTask($destinationMock, [], false))->start($clientWrapperMock);
    }
}
