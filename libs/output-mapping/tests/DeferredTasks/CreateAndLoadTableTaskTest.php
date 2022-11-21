<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\DeferredTasks;

use Keboola\OutputMapping\DeferredTasks\TableWriter\CreateAndLoadTableTask;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use PHPUnit\Framework\TestCase;

class CreateAndLoadTableTaskTest extends TestCase
{
    public function testClientErrorsArePropagated(): void
    {
        $mappingDestination = new MappingDestination('out.c-test.test-table');
        $createAndLoadTableTask = new CreateAndLoadTableTask($mappingDestination, []);
        $storageApiMock = $this->createMock(Client::class);

        $storageApiMock->expects($this->once())
            ->method('queueTableCreate')
            ->willThrowException(
                new ClientException("Bad Params.", 400)
            );
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage("Bad Params.");
        $this->expectExceptionCode(400);
        $createAndLoadTableTask->startImport($storageApiMock);
    }

    public function testAccessPermissionErrorIsConvertedToOutputOperationException(): void
    {
        $mappingDestination = new MappingDestination('out.c-test.test-table');
        $createAndLoadTableTask = new CreateAndLoadTableTask($mappingDestination, []);
        $storageApiMock = $this->createMock(Client::class);

        $storageApiMock->expects($this->once())
            ->method('queueTableCreate')
            ->willThrowException(
                new ClientException("You don't have access to the resource.", 403)
            );
        $this->expectException(OutputOperationException::class);
        $this->expectExceptionMessage("You don't have access to the resource. [{$mappingDestination->getBucketId()}]");
        $createAndLoadTableTask->startImport($storageApiMock);
    }
}
