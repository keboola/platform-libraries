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

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionCode(400);
        $this->expectExceptionMessageMatches(
            '/^Cannot create table \"testTable\" definition in Storage API: {.+}$/u',
        );
        $this->expectExceptionMessage('Selected columns are not included in table definition');

        $tableCreator->createTableDefinition(
            $this->emptyOutputBucketId,
            $tableDefinition,
        );
    }
}
