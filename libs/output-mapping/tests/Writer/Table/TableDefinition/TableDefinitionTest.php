<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table;

use Keboola\Datatype\Definition\GenericStorage;
use Keboola\Datatype\Definition\Synapse;
use Keboola\OutputMapping\Writer\Table\TableDefinition\BaseTypeTableDefinitionColumn;
use Keboola\OutputMapping\Writer\Table\TableDefinition\NativeTableDefinitionColumn;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinition;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionColumnFactory;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionColumnInterface;
use PHPUnit\Framework\TestCase;

class TableDefinitionTest extends TestCase
{
    /** @dataProvider addColumnProvider */
    public function testAddTableDefinitionColumn(
        TableDefinition $definition,
        array $tableMetadata,
        string $columnName,
        array $columnMetadata,
        string $backendType,
        TableDefinitionColumnInterface $expectedColumn
    ): void {
        $definition->addColumn($columnName, $columnMetadata, $tableMetadata, $backendType);
        $this->assertCount(1, $definition->getColumns());
        $this->assertEquals($expectedColumn, $definition->getColumns()[0]);
    }

    public function addColumnProvider(): \Generator
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
                    'key' => TableDefinitionColumnFactory::NATIVE_TYPE_METADATA_KEY,
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
