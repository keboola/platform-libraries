<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Generator;
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

    private TestHandler $logHandler;
    private Logger $logger;

    protected function setUp(): void
    {
        parent::setUp();

        $this->logHandler = new TestHandler();
        $this->logger = new Logger('test', [$this->logHandler]);
    }

    public function testDescriptionsAreStored(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects(self::once())
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

        $this->createModifier($client)->updateDescriptions(
            $this->createTableInfo(columns: ['col1', 'col2']),
            new TableDescription(self::TABLE_ID, 'table desc', ['col1' => 'col1 desc', 'col2' => 'col2 desc']),
        );

        self::assertFalse($this->logHandler->hasWarningRecords());
    }

    public function testStoringIsSkippedForUserManagedDescription(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects(self::never())
            ->method('updateTableDefinition');

        $this->createModifier($client)->updateDescriptions(
            $this->createTableInfo(columns: ['col1'], isDescriptionSystemManaged: false),
            new TableDescription(self::TABLE_ID, 'table desc', ['col1' => 'col1 desc']),
        );

        self::assertTrue($this->logHandler->hasInfoThatContains(sprintf(
            'Description of table "%s" is managed by the user, keeping the current value.',
            self::TABLE_ID,
        )));
    }

    /** @dataProvider unsupportedBackendProvider */
    public function testStoringIsSkippedOnBackendWithoutDefinitionUpdate(?string $backend): void
    {
        $client = $this->createMock(Client::class);
        $client->expects(self::never())
            ->method('updateTableDefinition');

        $this->createModifier($client)->updateDescriptions(
            $this->createTableInfo(columns: ['col1'], backend: $backend),
            new TableDescription(self::TABLE_ID, 'table desc', ['col1' => 'col1 desc']),
        );

        self::assertTrue($this->logHandler->hasInfoThatContains(sprintf(
            'Storing description of table "%s" is not supported on the',
            self::TABLE_ID,
        )));
    }

    public static function unsupportedBackendProvider(): Generator
    {
        // postgres is a bucket backend Storage allows (Model_Buckets::availableBackends()) but the
        // table-definition update endpoint does not support it
        yield 'postgres' => ['backend' => 'postgres'];
        yield 'backend missing in the response' => ['backend' => null];
    }

    /** @dataProvider supportedBackendProvider */
    public function testStoringIsPerformedOnSupportedBackend(string $backend): void
    {
        $client = $this->createMock(Client::class);
        $client->expects(self::once())
            ->method('updateTableDefinition')
            ->willReturn([]);

        $this->createModifier($client)->updateDescriptions(
            $this->createTableInfo(columns: [], backend: $backend),
            new TableDescription(self::TABLE_ID, 'table desc', []),
        );
    }

    public static function supportedBackendProvider(): Generator
    {
        yield 'snowflake' => ['backend' => 'snowflake'];
        yield 'bigquery' => ['backend' => 'bigquery'];
    }

    /**
     * Storage rejects a patch that carries no effective change with 400 "No table definition changes were
     * provided.", so an unchanged description must not be sent at all.
     */
    public function testUnchangedDescriptionsAreNotSent(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects(self::never())
            ->method('updateTableDefinition');

        $this->createModifier($client)->updateDescriptions(
            $this->createTableInfo(
                columns: ['col1'],
                storedTableDescription: 'table desc',
                storedColumnDescriptions: ['col1' => 'col1 desc'],
            ),
            new TableDescription(self::TABLE_ID, 'table desc', ['col1' => 'col1 desc']),
        );
    }

    public function testOnlyChangedDescriptionsAreSent(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects(self::once())
            ->method('updateTableDefinition')
            ->with(
                self::TABLE_ID,
                [
                    'columns' => [
                        ['name' => 'col2', 'description' => 'new col2 desc'],
                    ],
                ],
            )
            ->willReturn([]);

        $this->createModifier($client)->updateDescriptions(
            $this->createTableInfo(
                columns: ['col1', 'col2'],
                storedTableDescription: 'table desc',
                storedColumnDescriptions: ['col1' => 'col1 desc', 'col2' => 'old col2 desc'],
            ),
            new TableDescription(
                self::TABLE_ID,
                'table desc',
                ['col1' => 'col1 desc', 'col2' => 'new col2 desc'],
            ),
        );
    }

    public function testMissingColumnsAreSkippedWithWarning(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects(self::once())
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

        $this->createModifier($client)->updateDescriptions(
            $this->createTableInfo(columns: ['col1']),
            new TableDescription(self::TABLE_ID, null, ['col1' => 'col1 desc', 'col2' => 'col2 desc']),
        );

        self::assertTrue($this->logHandler->hasWarningThatContains(sprintf(
            'Cannot store description of column(s) "col2" of table "%s", the column(s) do not exist.',
            self::TABLE_ID,
        )));
    }

    public function testNothingToStoreDoesNotCallStorage(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects(self::never())
            ->method('updateTableDefinition');

        $this->createModifier($client)->updateDescriptions(
            $this->createTableInfo(columns: ['col1']),
            new TableDescription(self::TABLE_ID, null, []),
        );
    }

    public function testUserErrorIsWrappedInInvalidOutputException(): void
    {
        $clientException = new ClientException('Table definition update failed', 400);

        $client = $this->createMock(Client::class);
        $client->expects(self::once())
            ->method('updateTableDefinition')
            ->willThrowException($clientException);

        try {
            $this->createModifier($client)->updateDescriptions(
                $this->createTableInfo(columns: []),
                new TableDescription(self::TABLE_ID, 'table desc', []),
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

    public function testApplicationErrorIsPropagated(): void
    {
        $clientException = new ClientException('Internal Server Error', 500);

        $client = $this->createMock(Client::class);
        $client->expects(self::once())
            ->method('updateTableDefinition')
            ->willThrowException($clientException);

        try {
            $this->createModifier($client)->updateDescriptions(
                $this->createTableInfo(columns: []),
                new TableDescription(self::TABLE_ID, 'table desc', []),
            );
            self::fail('Storing the description should fail with ClientException.');
        } catch (ClientException $e) {
            self::assertSame($clientException, $e);
        }
    }

    private function createModifier(Client&MockObject $client): TableDescriptionModifier
    {
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getTableAndFileStorageClient')->willReturn($client);

        return new TableDescriptionModifier($clientWrapper, $this->logger);
    }

    /**
     * @param string[] $columns
     * @param array<string, string> $storedColumnDescriptions
     */
    private function createTableInfo(
        array $columns = [],
        bool $isDescriptionSystemManaged = true,
        ?string $backend = 'snowflake',
        ?string $storedTableDescription = null,
        array $storedColumnDescriptions = [],
    ): TableInfo {
        $definitionColumns = [];
        foreach ($columns as $columnName) {
            $definitionColumn = ['name' => $columnName];
            if (isset($storedColumnDescriptions[$columnName])) {
                $definitionColumn['definition'] = ['description' => $storedColumnDescriptions[$columnName]];
            }
            $definitionColumns[] = $definitionColumn;
        }

        return new TableInfo([
            'id' => self::TABLE_ID,
            'columns' => $columns,
            'isTyped' => true,
            'primaryKey' => [],
            'isDescriptionSystemManaged' => $isDescriptionSystemManaged,
            'bucket' => ['backend' => $backend],
            'definition' => [
                'description' => $storedTableDescription,
                'columns' => $definitionColumns,
            ],
        ]);
    }
}
