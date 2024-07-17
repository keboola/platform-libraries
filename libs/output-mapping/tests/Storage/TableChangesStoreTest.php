<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\OutputMapping\Storage\TableChangesStore;
use PHPUnit\Framework\TestCase;

class TableChangesStoreTest extends TestCase
{
    public function testAccessors(): void
    {
        $changesStore = new TableChangesStore();

        self::assertFalse($changesStore->hasMissingColumns());
        self::assertSame([], $changesStore->getMissingColumns());

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
}
