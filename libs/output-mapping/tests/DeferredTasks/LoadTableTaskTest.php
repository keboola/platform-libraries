<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\DeferredTasks;

use Keboola\OutputMapping\DeferredTasks\TableWriter\LoadTableTask;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use PHPUnit\Framework\TestCase;

class LoadTableTaskTest extends TestCase
{
    public function testClientErrorsArePropagated(): void
    {
        $mappingDestination = new MappingDestination('out.c-test.test-table');
        $loadTableTask = new LoadTableTask($mappingDestination, []);
        $storageApiMock = $this->createMock(Client::class);

        $storageApiMock->expects($this->once())
            ->method('queueTableImport')
            ->willThrowException(
                new ClientException("Bad Params.", 400)
            );
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage("Bad Params.");
        $this->expectExceptionCode(400);
        $loadTableTask->startImport($storageApiMock);
    }

    public function testAccessPermissionErrorIsConvertedToOutputOperationException(): void
    {
        $mappingDestination = new MappingDestination('out.c-test.test-table');
        $loadTableTask = new LoadTableTask($mappingDestination, []);
        $storageApiMock = $this->createMock(Client::class);

        $storageApiMock->expects($this->once())
            ->method('queueTableImport')
            ->willThrowException(
                new ClientException("You don't have access to the resource.", 403)
            );
        $this->expectException(OutputOperationException::class);
        $this->expectExceptionMessage("You don't have access to the resource. [{$mappingDestination->getTableId()}]");
        $loadTableTask->startImport($storageApiMock);
    }
}
