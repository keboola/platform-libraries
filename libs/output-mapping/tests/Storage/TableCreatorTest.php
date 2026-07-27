<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Keboola\Datatype\Definition\GenericStorage;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Storage\TableCreator;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinition;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionColumnFactory;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionFromColumns;
use Keboola\StorageApi\ClientException;

class TableCreatorTest extends AbstractTestCase
{
    #[NeedsEmptyOutputBucket]
    public function testCreateTableDefinition(): void
    {
        $tableCreator = new TableCreator($this->clientWrapper);

        $tableDefinition = new TableDefinition(
            new TableDefinitionColumnFactory([], 'snowflake', true),
        );
        $tableDefinition->setTableName('testTable');

        $tableDefinition->addColumn('id', (new GenericStorage('int', ['nullable' => false]))->toMetadata());

        $tableId = $tableCreator->createTableDefinition(
            $this->emptyOutputBucketId,
            $tableDefinition,
        );

        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($tableId);
        self::assertIsArray($table);
        self::assertArrayHasKey('isTyped', $table);
        self::assertTrue($table['isTyped']);
        self::assertArrayHasKey('name', $table);
        self::assertSame('testTable', $table['name']);
        self::assertNotEmpty($table['definition']['columns']);
        self::assertSame(
            [
                [
                    'name' => 'id',
                    'definition' => [
                        'type' => 'NUMBER',
                        'nullable' => true,
                        'length' => '38,0',
                    ],
                    'basetype' => 'INTEGER',
                    'canBeFiltered' => true,
                ],
            ],
            $table['definition']['columns'],
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testCreateTableDefinitionErrorHandling(): void
    {
        $tableCreator = new TableCreator($this->clientWrapper);

        $tableDefinition = new TableDefinition(
            new TableDefinitionColumnFactory([], 'snowflake', true),
        );
        $tableDefinition->setTableName('testTable');

        $tableDefinition->addColumn('id', (new GenericStorage('int', ['nullable' => false]))->toMetadata());
        $tableDefinition->setPrimaryKeysNames(['Name']);

        try {
            $tableCreator->createTableDefinition(
                $this->emptyOutputBucketId,
                $tableDefinition,
            );
            self::fail('CreateTableDefinition should fail with InvalidOutputException');
        } catch (InvalidOutputException $e) {
            self::assertStringContainsString(
                'Cannot create table "testTable" definition in Storage API:',
                $e->getMessage(),
            );
            self::assertStringContainsString(
                'Selected columns are not included in table definition',
                $e->getMessage(),
            );
            self::assertNotNull($e->getPrevious());
            self::assertInstanceOf(ClientException::class, $e->getPrevious());
            self::assertSame(400, $e->getCode());
        }
    }

    #[NeedsEmptyOutputBucket]
    public function testCreateNonTypedTableFromColumns(): void
    {
        $tableCreator = new TableCreator($this->clientWrapper);

        $tableId = $tableCreator->createTableDefinition(
            $this->emptyOutputBucketId,
            new TableDefinitionFromColumns('testTable', ['id', 'name'], ['id']),
        );

        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($tableId);

        // no column carries a type, so Storage creates the table as non-typed
        self::assertFalse($table['isTyped']);
        self::assertSame('testTable', $table['name']);
        self::assertSame(['id', 'name'], $table['columns']);
        self::assertSame(['id'], $table['primaryKey']);
    }

    #[NeedsEmptyOutputBucket]
    public function testCreateNonTypedTableFromColumnsErrorHandling(): void
    {
        $tableCreator = new TableCreator($this->clientWrapper);

        try {
            $tableCreator->createTableDefinition(
                $this->emptyOutputBucketId,
                new TableDefinitionFromColumns('testTable', ['id'], ['name']),
            );
            self::fail('CreateTableDefinition should fail with InvalidOutputException');
        } catch (InvalidOutputException $e) {
            self::assertStringContainsString(
                'Cannot create table "testTable" definition in Storage API:',
                $e->getMessage(),
            );
            self::assertNotNull($e->getPrevious());
            self::assertInstanceOf(ClientException::class, $e->getPrevious());
            self::assertSame($e->getPrevious()->getCode(), $e->getCode());
        }
    }
}
