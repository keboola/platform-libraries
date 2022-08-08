<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\TableDefinition;

use Keboola\Datatype\Definition\GenericStorage;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionColumnFactory;

class TableDefinitionColumnFactoryTest extends \PHPUnit\Framework\TestCase
{
    /** @dataProvider createTableDefinitionColumnProfider */
    public function testFactoryCreateTableDefinitionColumn(
        string $columnName,
        array $columnMetadata,
        array $expectedSerialisation
    ): void {
        $columnFactory = new TableDefinitionColumnFactory();
        $column = $columnFactory->createTableDefinitionColumn($columnName, $columnMetadata);
        $this->assertSame($expectedSerialisation, $column->toArray());
    }

    public function createTableDefinitionColumnProfider(): \Generator
    {
        yield [
            'testColumn',
            (new GenericStorage('varchar'))->toMetadata(),
            [
                'name' => 'testColumn',
                'basetype' => 'STRING',
            ],
        ];
    }
}
