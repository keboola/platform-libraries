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
    public function testCreateTable(): void
    {
        $tableCreator = new TableCreator($this->clientWrapper);

        $tableId = $tableCreator->createTable(
            $this->emptyOutputBucketId,
            'testTable',
            ['id'],
            [
                'primaryKey' => 'id',
            ],
        );

        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($tableId);

        self::assertIsArray($table);
        self::assertArrayHasKey('isTyped', $table);
        self::assertFalse($table['isTyped']);
        self::assertArrayHasKey('name', $table);
        self::assertSame('testTable', $table['name']);
        self::assertArrayHasKey('columns', $table);
        self::assertSame(['id'], $table['columns']);
        self::assertArrayHasKey('primaryKey', $table);
        self::assertSame(['id'], $table['primaryKey']);
    }

    #[NeedsEmptyOutputBucket]
    public function testCreateTableErrorHandling(): void
    {
        $tableCreator = new TableCreator($this->clientWrapper);

        try {
            $tableCreator->createTable(
                $this->emptyOutputBucketId,
                'testTable',
                ['id'],
                [
                    'primaryKey' => 'name',
                ],
            );
            self::fail('CreateTable should fail with InvalidOutputException');
        } catch (InvalidOutputException $e) {
            self::assertStringContainsString(
                'Cannot create table "testTable" in Storage API:',
                $e->getMessage(),
            );
            self::assertStringContainsString(
                'storage.tables.validation.invalidPrimaryKeyColumns',
                $e->getMessage(),
            );
            self::assertNotNull($e->getPrevious());
            self::assertInstanceOf(ClientException::class, $e->getPrevious());
            self::assertSame($e->getPrevious()->getCode(), $e->getCode());
        }
    }
}
