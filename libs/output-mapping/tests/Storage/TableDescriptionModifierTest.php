<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Storage\TableDescription;
use Keboola\OutputMapping\Storage\TableDescriptionModifier;
use Keboola\OutputMapping\Storage\TableInfo;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

class TableDescriptionModifierTest extends TestCase
{
    private const TABLE_ID = 'in.c-main.table';

    public function testUpdateExistingTableDescriptions(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects($this->once())
            ->method('updateTableDefinition')
            ->with(
                self::TABLE_ID,
                [
                    'description' => 'table desc',
                    'columns' => [
                        ['name' => 'col1', 'description' => 'col1 desc'],
                        ['name' => 'col2', 'description' => 'col2 desc'],
                    ],
                ],
            )
            ->willReturn([]);

        $logHandler = new TestHandler();
        $logger = new Logger('test', [$logHandler]);
        $modifier = new TableDescriptionModifier($this->createClientWrapper($client), $logger);
        $modifier->updateExistingTableDescriptions(
            $this->createTableInfo(true, ['col1', 'col2']),
            new TableDescription(self::TABLE_ID, 'table desc', ['col1' => 'col1 desc', 'col2' => 'col2 desc']),
        );

        self::assertFalse($logHandler->hasWarningRecords());
    }

    public function testUpdateExistingTableDescriptionsIsSkippedForUserManagedDescription(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects($this->never())
            ->method('updateTableDefinition');

        $logHandler = new TestHandler();
        $logger = new Logger('test', [$logHandler]);
        $modifier = new TableDescriptionModifier($this->createClientWrapper($client), $logger);
        $modifier->updateExistingTableDescriptions(
            $this->createTableInfo(false, ['col1']),
            new TableDescription(self::TABLE_ID, 'table desc', ['col1' => 'col1 desc']),
        );

        self::assertTrue($logHandler->hasInfoThatContains(sprintf(
            'Description of table "%s" is managed by the user, keeping the current value.',
            self::TABLE_ID,
        )));
    }

    public function testUpdateExistingTableDescriptionsSkipsMissingColumns(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects($this->once())
            ->method('updateTableDefinition')
            ->with(
                self::TABLE_ID,
                [
                    'columns' => [
                        ['name' => 'col1', 'description' => 'col1 desc'],
                    ],
                ],
            )
            ->willReturn([]);

        $logHandler = new TestHandler();
        $logger = new Logger('test', [$logHandler]);
        $modifier = new TableDescriptionModifier($this->createClientWrapper($client), $logger);
        $modifier->updateExistingTableDescriptions(
            $this->createTableInfo(true, ['col1']),
            new TableDescription(self::TABLE_ID, null, ['col1' => 'col1 desc', 'col2' => 'col2 desc']),
        );

        self::assertTrue($logHandler->hasWarningThatContains(sprintf(
            'Cannot store description of column(s) "col2" of table "%s", the column(s) do not exist.',
            self::TABLE_ID,
        )));
    }

    public function testUpdateExistingTableDescriptionsWithNothingToStoreDoesNotCallStorage(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects($this->never())
            ->method('updateTableDefinition');

        $modifier = new TableDescriptionModifier($this->createClientWrapper($client), new Logger('test'));
        $modifier->updateExistingTableDescriptions(
            $this->createTableInfo(true, ['col1']),
            new TableDescription(self::TABLE_ID, null, []),
        );
    }

    public function testSetCreatedTableDescriptions(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects($this->once())
            ->method('updateTableDefinition')
            ->with(
                self::TABLE_ID,
                [
                    'description' => 'table desc',
                    'columns' => [
                        ['name' => 'col1', 'description' => 'col1 desc'],
                    ],
                ],
            )
            ->willReturn([]);

        $modifier = new TableDescriptionModifier($this->createClientWrapper($client), new Logger('test'));
        $modifier->setCreatedTableDescriptions(
            new TableDescription(self::TABLE_ID, 'table desc', ['col1' => 'col1 desc']),
            ['col1'],
        );
    }

    public function testSetCreatedTableDescriptionsWithUnknownColumnList(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects($this->once())
            ->method('updateTableDefinition')
            ->with(
                self::TABLE_ID,
                [
                    'columns' => [
                        ['name' => 'col1', 'description' => 'col1 desc'],
                    ],
                ],
            )
            ->willReturn([]);

        $logHandler = new TestHandler();
        $logger = new Logger('test', [$logHandler]);
        $modifier = new TableDescriptionModifier($this->createClientWrapper($client), $logger);
        $modifier->setCreatedTableDescriptions(
            new TableDescription(self::TABLE_ID, null, ['col1' => 'col1 desc']),
            null,
        );

        self::assertFalse($logHandler->hasWarningRecords());
    }

    public function testStorageErrorIsWrappedInInvalidOutputException(): void
    {
        $clientException = new ClientException('Table definition update failed', 400);

        $client = $this->createMock(Client::class);
        $client->expects($this->once())
            ->method('updateTableDefinition')
            ->willThrowException($clientException);

        $modifier = new TableDescriptionModifier($this->createClientWrapper($client), new Logger('test'));

        try {
            $modifier->setCreatedTableDescriptions(
                new TableDescription(self::TABLE_ID, 'table desc', []),
                null,
            );
            self::fail('Storing the description should fail with InvalidOutputException.');
        } catch (InvalidOutputException $e) {
            self::assertSame(
                'Cannot update description of table "in.c-main.table": Table definition update failed',
                $e->getMessage(),
            );
            self::assertSame(400, $e->getCode());
            self::assertSame($clientException, $e->getPrevious());
        }
    }

    private function createClientWrapper(Client&MockObject $client): ClientWrapper
    {
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getTableAndFileStorageClient')->willReturn($client);

        return $clientWrapper;
    }

    /**
     * @param string[] $columns
     */
    private function createTableInfo(bool $isDescriptionSystemManaged, array $columns): TableInfo
    {
        return new TableInfo([
            'id' => self::TABLE_ID,
            'columns' => $columns,
            'isTyped' => true,
            'primaryKey' => [],
            'isDescriptionSystemManaged' => $isDescriptionSystemManaged,
        ]);
    }
}
