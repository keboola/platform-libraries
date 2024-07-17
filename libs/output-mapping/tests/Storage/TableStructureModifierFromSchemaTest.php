<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Keboola\Datatype\Definition\GenericStorage;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\OutputMapping\Storage\BucketInfo;
use Keboola\OutputMapping\Storage\TableChangesStore;
use Keboola\OutputMapping\Storage\TableInfo;
use Keboola\OutputMapping\Storage\TableStructureModifierFromSchema;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyOutputBucket;

class TableStructureModifierFromSchemaTest extends AbstractTestCase
{
    private array $bucket;
    private array $table;

    #[NeedsEmptyOutputBucket]
    public function testEmptyChanges(): void
    {
        $this->prepareStorageData();
        $tableStructureModifier = new TableStructureModifierFromSchema($this->clientWrapper, $this->testLogger);

        $tableChangesStore = new TableChangesStore();
        self::assertFalse($tableChangesStore->hasMissingColumns());

        $tableStructureModifier->updateTableStructure(
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
        $tableStructureModifier = new TableStructureModifierFromSchema($this->clientWrapper, $this->testLogger);

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

        $tableStructureModifier->updateTableStructure(
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
        $tableStructureModifier = new TableStructureModifierFromSchema($this->clientWrapper, $this->testLogger);

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
            $tableStructureModifier->updateTableStructure(
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

    private function prepareStorageData(): void
    {
        $idDatatype = new GenericStorage('int', ['nullable' => false]);
        $nameDatatype = new GenericStorage('varchar', ['length' => '17', 'nullable' => false]);

        $this->clientWrapper->getTableAndFileStorageClient()->createTableDefinition($this->emptyOutputBucketId, [
            'name' => 'tableDefinition',
            'primaryKeysNames' => [],
            'columns' => [
                [
                    'name' => 'Id',
                    'basetype' => $idDatatype->getBasetype(),
                ],
                [
                    'name' => 'Name',
                    'basetype' => $nameDatatype->getBasetype(),
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
