<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaPrimaryKey;
use Keboola\OutputMapping\Storage\TableChangesStore;
use PHPUnit\Framework\TestCase;

class TableChangesStoreTest extends TestCase
{
    public function testAccessors(): void
    {
        $changesStore = new TableChangesStore();

        self::assertFalse($changesStore->hasMissingColumns());
        self::assertSame([], $changesStore->getMissingColumns());
        self::assertNull($changesStore->getPrimaryKey());

        $changesStore->addMissingColumn(new MappingFromConfigurationSchemaColumn([
            'name' => 'newColumn',
            'data_type' => [
                'base' => [
                    'type' => 'STRING',
                    'length' => '255',
                ],
                'snowflake' => [
                    'type' => 'VARCHAR',
                ],
            ],
        ]));
    }

    public function testAddMissingColumn(): void
    {
        $expectedSchemaColumn = new MappingFromConfigurationSchemaColumn([
            'name' => 'newColumn',
            'data_type' => [
                'base' => [
                    'type' => 'STRING',
                    'length' => '255',
                ],
                'snowflake' => [
                    'type' => 'VARCHAR',
                ],
            ],
        ]);

        $changesStore = new TableChangesStore();
        $changesStore->addMissingColumn($expectedSchemaColumn);

        self::assertTrue($changesStore->hasMissingColumns());
        self::assertSame([$expectedSchemaColumn], $changesStore->getMissingColumns());
    }

    public function testSetPrimaryKey(): void
    {
        $expectedPkColumn = new MappingFromConfigurationSchemaColumn([
            'name' => 'newColumn',
            'primary_key' => true,
            'data_type' => [
                'base' => [
                    'type' => 'STRING',
                    'length' => '255',
                ],
                'snowflake' => [
                    'type' => 'VARCHAR',
                ],
            ],
        ]);

        $primaryKey = new MappingFromConfigurationSchemaPrimaryKey();
        $primaryKey->addPrimaryKeyColumn($expectedPkColumn);

        $changesStore = new TableChangesStore();
        $changesStore->setPrimaryKey($primaryKey);

        self::assertSame([$expectedPkColumn], $changesStore->getPrimaryKey()?->getColumns());
    }

    public function testColumnAttributeChanges(): void
    {
        $expectedPkColumn = new MappingFromConfigurationSchemaColumn([
            'name' => 'col1',
            'primary_key' => true,
            'data_type' => [
                'base' => [
                    'type' => 'STRING',
                    'length' => '255',
                    'default' => 'default',
                ],
                'snowflake' => [
                    'type' => 'VARCHAR',
                ],
            ],
        ]);

        $changesStore = new TableChangesStore();
        $changesStore->addColumnAttributeChanges($expectedPkColumn);

        self::assertSame([$expectedPkColumn], $changesStore->getDifferentColumnAttributes());
    }
}
