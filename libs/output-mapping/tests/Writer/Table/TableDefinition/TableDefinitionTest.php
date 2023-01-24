<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\TableDefinition;

use Generator;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\Datatype\Definition\Synapse;
use Keboola\OutputMapping\Writer\Table\TableDefinition\BaseTypeTableDefinitionColumn;
use Keboola\OutputMapping\Writer\Table\TableDefinition\NativeTableDefinitionColumn;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinition;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionColumnInterface;
use PHPUnit\Framework\TestCase;

class TableDefinitionTest extends TestCase
{
    /** @dataProvider addTableDefinitionColumnProvider */
    public function testAddTableDefinitionColumn(
        TableDefinition $definition,
        array $tableMetadata,
        string $columnName,
        array $columnMetadata,
        string $backendType,
        TableDefinitionColumnInterface $expectedColumn
    ): void {
        $definition->addColumn($columnName, $columnMetadata, $tableMetadata, $backendType);
        self::assertCount(1, $definition->getColumns());
        self::assertEquals($expectedColumn, $definition->getColumns()[0]);
    }

    public function addTableDefinitionColumnProvider(): Generator
    {
        yield 'basetype test' => [
            'tableDefinition' => new TableDefinition(),
            'tableMetadata' => [],
            'columnName' => 'testColumn',
            'columnMetadata' => (new GenericStorage('varchar', ['length' => '25']))->toMetadata(),
            'backendType' => 'snowflake',
            'expectedColumn' => new BaseTypeTableDefinitionColumn('testColumn', 'STRING'),
        ];

        yield 'native type test' => [
            'tableDefinition' => new TableDefinition(),
            'tableMetadata' => [
                [
                    'key' => 'KBC.datatype.backend',
                    'value' => 'synapse',
                ],
            ],
            'columnName' => 'testColumn',
            'columnMetadata' => (new Synapse('varchar', ['length' => '25']))->toMetadata(),
            'backendType' => 'synapse',
            'expectedColumn' => new NativeTableDefinitionColumn(
                'testColumn',
                new Synapse('varchar', ['length' => '25'])
            ),
        ];
    }
}
