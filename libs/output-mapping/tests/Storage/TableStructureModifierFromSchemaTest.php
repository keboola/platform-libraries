<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Keboola\Datatype\Definition\GenericStorage;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaPrimaryKey;
use Keboola\OutputMapping\Storage\BucketInfo;
use Keboola\OutputMapping\Storage\TableChangesStore;
use Keboola\OutputMapping\Storage\TableInfo;
use Keboola\OutputMapping\Storage\TableStructureModifierFromSchema;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\OutputMapping\Tests\Needs\NeedsTestTables;

class TableStructureModifierFromSchemaTest extends AbstractTestCase
{
    private TableStructureModifierFromSchema $tableStructureModifier;

    private array $bucket;
    private array $table;

    public function setup(): void
    {
        parent::setup();

        $this->tableStructureModifier = new TableStructureModifierFromSchema($this->clientWrapper, $this->testLogger);
    }

    #[NeedsEmptyOutputBucket]
    public function testEmptyChanges(): void
    {
        $this->prepareStorageData();

        $tableChangesStore = new TableChangesStore();
        self::assertFalse($tableChangesStore->hasMissingColumns());

        $this->tableStructureModifier->updateTableStructure(
            new BucketInfo($this->bucket),
            new TableInfo($this->table),
            $tableChangesStore,
        );

        $updatedTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->table['id']);

        $this->assertEquals(
            $this->dropTimestampParams($this->table),
            $this->dropTimestampParams($updatedTable),
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testAddMissingColumn(): void
    {
        $this->prepareStorageData();

        $tableChangesStore = new TableChangesStore();
        $tableChangesStore->addMissingColumn(new MappingFromConfigurationSchemaColumn([
            'name' => 'newColumn',
            'data_type' => [
                'base' => [
                    'type' => 'STRING',
                    'length' => '255',
                ],
                'snowflake' => [
                    'type' => 'VARCHAR',
                    'length' => '255',
                ],
            ],
        ]));
        self::assertTrue($tableChangesStore->hasMissingColumns());

        $this->tableStructureModifier->updateTableStructure(
            new BucketInfo($this->bucket),
            new TableInfo($this->table),
            $tableChangesStore,
        );

        $updatedTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->table['id']);

        $newColumn = array_diff($updatedTable['columns'], $this->table['columns']);
        self::assertCount(1, $newColumn);

        self::assertEquals(
            [
                'name' => 'newColumn',
                'definition' => [
                    'type' => 'VARCHAR',
                    'nullable' => true,
                    'length' => '255',
                ],
                'basetype' => 'STRING',
                'canBeFiltered' => true,
            ],
            $updatedTable['definition']['columns'][2],
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testErrorColumnAlreadyExists(): void
    {
        $this->prepareStorageData();

        $tableChangesStore = new TableChangesStore();
        $tableChangesStore->addMissingColumn(new MappingFromConfigurationSchemaColumn([
            'name' => 'newColumn',
            'data_type' => [
                'base' => [
                    'type' => 'STRING',
                    'length' => '255',
                ],
                'snowflake' => [
                    'type' => 'VARCHAR',
                    'length' => '255',
                ],
            ],
        ]));
        $tableChangesStore->addMissingColumn(new MappingFromConfigurationSchemaColumn([
            'name' => 'newColumn2',
            'data_type' => [
                'base' => [
                    'type' => 'STRING',
                    'length' => '255',
                ],
                'snowflake' => [
                    'type' => 'VARCHAR',
                    'length' => '255',
                ],
            ],
        ]));
        // following column already exists
        $tableChangesStore->addMissingColumn(new MappingFromConfigurationSchemaColumn([
            'name' => 'Id',
            'data_type' => [
                'base' => [
                    'type' => 'NUMERIC',
                ],
            ],
        ]));
        self::assertTrue($tableChangesStore->hasMissingColumns());

        try {
            $this->tableStructureModifier->updateTableStructure(
                new BucketInfo($this->bucket),
                new TableInfo($this->table),
                $tableChangesStore,
            );
            $this->fail('Exception should be thrown');
        } catch (InvalidOutputException $e) {
            self::assertEquals('Column Id already exists in table tableDefinition', $e->getMessage());
        }

        $updatedTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->table['id']);

        self::assertEquals(
            $this->dropTimestampParams($this->table),
            $this->dropTimestampParams($updatedTable),
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testModifyPrimaryKey(): void
    {
        $this->prepareStorageData(['Id']);

        $primaryKey = new MappingFromConfigurationSchemaPrimaryKey();
        $primaryKey->addPrimaryKeyColumn(new MappingFromConfigurationSchemaColumn([
            'name' => 'Id',
            'primary_key' => true,
            'data_type' => [
                'base' => [
                    'type' => 'INTEGER',
                ],
            ],
        ]));
        $primaryKey->addPrimaryKeyColumn(new MappingFromConfigurationSchemaColumn([
            'name' => 'Name',
            'primary_key' => true,
            'data_type' => [
                'base' => [
                    'type' => 'STRING',
                ],
            ],
        ]));

        $tableChangesStore = new TableChangesStore();
        $tableChangesStore->setPrimaryKey($primaryKey);

        $this->tableStructureModifier->updateTableStructure(
            new BucketInfo($this->bucket),
            new TableInfo($this->table),
            $tableChangesStore,
        );

        $updatedTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->table['id']);

        self::assertEquals(
            ['Id', 'Name'],
            $updatedTable['definition']['primaryKeysNames'],
        );

        self::assertTrue($this->testHandler->hasWarning(
            sprintf('Modifying primary key of table "%s" from "Id" to "Id, Name".', $updatedTable['id']),
        ));
    }

    #[NeedsTestTables(1)]
    public function testModifyPrimaryKeyNonTypedTable(): void
    {
        $this->bucket = $this->clientWrapper->getTableAndFileStorageClient()->getBucket($this->testBucketId);
        $this->table = $this
            ->clientWrapper
            ->getTableAndFileStorageClient()
            ->getTable($this->firstTableId);

        $primaryKey = new MappingFromConfigurationSchemaPrimaryKey();
        $primaryKey->addPrimaryKeyColumn(new MappingFromConfigurationSchemaColumn([
            'name' => 'Id',
            'primary_key' => true,
            'data_type' => [
                'base' => [
                    'type' => 'INTEGER',
                ],
            ],
        ]));
        $primaryKey->addPrimaryKeyColumn(new MappingFromConfigurationSchemaColumn([
            'name' => 'Name',
            'primary_key' => true,
            'data_type' => [
                'base' => [
                    'type' => 'STRING',
                ],
            ],
        ]));

        $tableChangesStore = new TableChangesStore();
        $tableChangesStore->setPrimaryKey($primaryKey);

        $this->tableStructureModifier->updateTableStructure(
            new BucketInfo($this->bucket),
            new TableInfo($this->table),
            $tableChangesStore,
        );

        $updatedTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId);

        self::assertEquals(
            ['Id', 'Name'],
            $updatedTable['primaryKey'],
        );

        self::assertTrue($this->testHandler->hasWarning(
            sprintf('Modifying primary key of table "%s" from "" to "Id, Name".', $updatedTable['id']),
        ));
    }

    #[NeedsTestTables(1)]
    public function testNonTypedTableRestoreOriginalPrimaryKeyOnPrimaryKeyModifyError(): void
    {
        $this->bucket = $this->clientWrapper->getTableAndFileStorageClient()->getBucket($this->testBucketId);
        $this->table = $this
            ->clientWrapper
            ->getTableAndFileStorageClient()
            ->getTable($this->firstTableId);

        $primaryKey = new MappingFromConfigurationSchemaPrimaryKey();
        $primaryKey->addPrimaryKeyColumn(new MappingFromConfigurationSchemaColumn([
            'name' => 'Id',
            'primary_key' => true,
            'data_type' => [
                'base' => [
                    'type' => 'INTEGER',
                ],
            ],
        ]));
        $primaryKey->addPrimaryKeyColumn(new MappingFromConfigurationSchemaColumn([
            'name' => 'NonExistingColumn',
            'primary_key' => true,
            'data_type' => [
                'base' => [
                    'type' => 'STRING',
                ],
            ],
        ]));

        $tableChangesStore = new TableChangesStore();
        $tableChangesStore->setPrimaryKey($primaryKey);

        try {
            $this->tableStructureModifier->updateTableStructure(
                new BucketInfo($this->bucket),
                new TableInfo($this->table),
                $tableChangesStore,
            );
            $this->fail('UpdateTableStructure should fail with InvalidOutputException');
        } catch (InvalidOutputException $e) {
            self::assertStringContainsString('Error changing primary key of table', $e->getMessage());
            self::assertStringContainsString(
                'Primary key columns "NonExistingColumn" not found in "Id, Name, foo, bar"',
                $e->getMessage(),
            );
        }

        $updatedTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->table['id']);

        self::assertEquals(
            $this->dropTimestampParams($this->table),
            $this->dropTimestampParams($updatedTable),
        );

        self::assertTrue($this->testHandler->hasWarning(
            sprintf('Modifying primary key of table "%s" from "" to "Id, NonExistingColumn".', $updatedTable['id']),
        ));
        self::assertTrue($this->testHandler->hasWarningThatContains(
            'Primary key columns "NonExistingColumn" not found in "Id, Name, foo, bar"',
        ));
    }

    #[NeedsEmptyOutputBucket]
    public function testRestoreOriginalPrimaryKeyOnPrimaryKeyModifyError(): void
    {
        $this->prepareStorageData(['Id']);

        $primaryKey = new MappingFromConfigurationSchemaPrimaryKey();
        $primaryKey->addPrimaryKeyColumn(new MappingFromConfigurationSchemaColumn([
            'name' => 'Id',
            'primary_key' => true,
            'data_type' => [
                'base' => [
                    'type' => 'INTEGER',
                ],
            ],
        ]));
        $primaryKey->addPrimaryKeyColumn(new MappingFromConfigurationSchemaColumn([
            'name' => 'NonExistingColumn',
            'primary_key' => true,
            'data_type' => [
                'base' => [
                    'type' => 'STRING',
                ],
            ],
        ]));

        $tableChangesStore = new TableChangesStore();
        $tableChangesStore->setPrimaryKey($primaryKey);

        try {
            $this->tableStructureModifier->updateTableStructure(
                new BucketInfo($this->bucket),
                new TableInfo($this->table),
                $tableChangesStore,
            );
            $this->fail('UpdateTableStructure should fail with InvalidOutputException');
        } catch (InvalidOutputException $e) {
            self::assertStringContainsString('Error changing primary key of table', $e->getMessage());
            self::assertStringContainsString(
                'Primary key columns "NonExistingColumn" not found in "Id, Name"',
                $e->getMessage(),
            );
        }

        $updatedTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->table['id']);

        self::assertEquals(
            $this->dropTimestampParams($this->table),
            $this->dropTimestampParams($updatedTable),
        );

        self::assertTrue($this->testHandler->hasWarning(
            sprintf('Modifying primary key of table "%s" from "Id" to "Id, NonExistingColumn".', $updatedTable['id']),
        ));
        self::assertTrue($this->testHandler->hasWarningThatContains(
            'Primary key columns "NonExistingColumn" not found in "Id, Name"',
        ));
    }

    #[NeedsEmptyOutputBucket]
    public function testModifyColumnAttributes(): void
    {
        $this->prepareStorageData();

        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->table['id']);
        self::assertEquals(
            [
                'name' => 'Name',
                'definition' => [
                    'type' => 'VARCHAR',
                    'nullable' => true,
                    'length' => '17',
                ],
                'basetype' => 'STRING',
                'canBeFiltered' => true,
            ],
            $table['definition']['columns'][1],
        );

        $tableChangesStore = new TableChangesStore();
        $tableChangesStore->addColumnAttributeChanges(new MappingFromConfigurationSchemaColumn([
            'name' => 'Name',
            'data_type' => [
                'base' => [
                    'type' => 'STRING',
                    'length' => '255',
                ],
            ],
            'nullable' => false,
        ]));

        $this->tableStructureModifier->updateTableStructure(
            new BucketInfo($this->bucket),
            new TableInfo($this->table),
            $tableChangesStore,
        );

        $updatedTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->table['id']);
        self::assertEquals(
            [
                'name' => 'Name',
                'definition' => [
                    'type' => 'VARCHAR',
                    'nullable' => false,
                    'length' => '255',
                ],
                'basetype' => 'STRING',
                'canBeFiltered' => true,
            ],
            $updatedTable['definition']['columns'][1],
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testModifyColumnAttributesError(): void
    {
        $this->prepareStorageData();

        $tableChangesStore = new TableChangesStore();

        $tableChangesStore->addColumnAttributeChanges(new MappingFromConfigurationSchemaColumn([
            'name' => 'Id',
            'data_type' => [
                'base' => [
                    'default' => 'new default value',
                ],
            ],
        ]));

        try {
            $this->tableStructureModifier->updateTableStructure(
                new BucketInfo($this->bucket),
                new TableInfo($this->table),
                $tableChangesStore,
            );
            $this->fail('UpdateTableStructure should fail with InvalidOutputException');
        } catch (InvalidOutputException $e) {
            self::assertStringContainsString(
                'Cannot change default value of column "Id" from "" to "new default value"',
                $e->getMessage(),
            );
        }
    }

    private function prepareStorageData(array $primaryKeyNames = []): void
    {
        $idDatatype = new GenericStorage('int', ['nullable' => false]);
        $nameDatatype = new GenericStorage('varchar', ['length' => '17', 'nullable' => false]);

        $this->clientWrapper->getTableAndFileStorageClient()->createTableDefinition($this->emptyOutputBucketId, [
            'name' => 'tableDefinition',
            'primaryKeysNames' => $primaryKeyNames,
            'columns' => [
                [
                    'name' => 'Id',
                    'basetype' => $idDatatype->getBasetype(),
                ],
                [
                    'name' => 'Name',
                    'basetype' => $nameDatatype->getBasetype(),
                    'definition' => [
                        'type' => $nameDatatype->getType(),
                        'length' => $nameDatatype->getLength(),
                    ],
                ],
            ],
        ]);

        $this->bucket = $this->clientWrapper->getTableAndFileStorageClient()->getBucket($this->emptyOutputBucketId);
        $this->table = $this
            ->clientWrapper
            ->getTableAndFileStorageClient()
            ->getTable($this->emptyOutputBucketId . '.tableDefinition');
    }

    private function dropTimestampParams(array $table): array
    {
        unset($table['created']);
        unset($table['lastChangeDate']);
        unset($table['bucket']['lastChangeDate']);
        return $table;
    }
}
