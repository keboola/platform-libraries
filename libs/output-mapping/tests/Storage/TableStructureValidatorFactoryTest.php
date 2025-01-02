<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\TableNotFoundException;
use Keboola\OutputMapping\Storage\TableStructureValidator;
use Keboola\OutputMapping\Storage\TableStructureValidatorFactory;
use Keboola\OutputMapping\Storage\TypedTableStructureValidator;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class TableStructureValidatorFactoryTest extends TestCase
{
    public function testEnsureStructureValidator(): void
    {
        $client = $this->createMock(Client::class);
        $client->method('getTable')->willReturn(['isTyped' => false]);

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getTableAndFileStorageClient')->willReturn($client);

        $factory = new TableStructureValidatorFactory(new NullLogger(), $clientWrapper);
        $validator = $factory->ensureStructureValidator('in.c-main.table');
        self::assertInstanceOf(TableStructureValidator::class, $validator);
    }

    public function testEnsureTypedStructureValidator(): void
    {
        $client = $this->createMock(Client::class);
        $client->method('getTable')->willReturn(['isTyped' => true]);

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getTableAndFileStorageClient')->willReturn($client);

        $factory = new TableStructureValidatorFactory(new NullLogger(), $clientWrapper);
        $validator = $factory->ensureStructureValidator('in.c-main.table');
        self::assertInstanceOf(TypedTableStructureValidator::class, $validator);
    }

    public function testEnsureStructureValidatorTableNotFound(): void
    {
        $client = $this->createMock(Client::class);
        $client->method('getTable')->willThrowException(new ClientException('Table not found', 404));

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getTableAndFileStorageClient')->willReturn($client);

        $factory = new TableStructureValidatorFactory(new NullLogger(), $clientWrapper);
        $this->expectException(TableNotFoundException::class);
        $factory->ensureStructureValidator('in.c-main.table');
    }

    public function testEnsureStructureValidatorInvalidOutput(): void
    {
        $client = $this->createMock(Client::class);
        $client->method('getTable')->willThrowException(new ClientException('Invalid output', 400));

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getTableAndFileStorageClient')->willReturn($client);

        $factory = new TableStructureValidatorFactory(new NullLogger(), $clientWrapper);
        $this->expectException(InvalidOutputException::class);
        $factory->ensureStructureValidator('in.c-main.table');
    }
}
