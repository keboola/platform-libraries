<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\TableDefinition;

use Generator;
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
    /** @dataProvider addTableDefinitionColumnProvider */
    public function testAddTableDefinitionColumn(
        array $tableMetadata,
        string $columnName,
        array $columnMetadata,
        string $backendType,
        TableDefinitionColumnInterface $expectedColumn,
    ): void {
        $tableDefinition = new TableDefinition(
            new TableDefinitionColumnFactory($tableMetadata, $backendType, false),
        );

        $tableDefinition->addColumn($columnName, $columnMetadata);
        self::assertCount(1, $tableDefinition->getColumns());
        self::assertEquals($expectedColumn, $tableDefinition->getColumns()[0]);
    }

    public function addTableDefinitionColumnProvider(): Generator
    {
        yield 'basetype test' => [
            'tableMetadata' => [],
            'columnName' => 'testColumn',
            'columnMetadata' => (new GenericStorage('varchar', ['length' => '25']))->toMetadata(),
            'backendType' => 'snowflake',
            'expectedColumn' => new BaseTypeTableDefinitionColumn('testColumn', 'STRING'),
        ];

        yield 'native type test' => [
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
                new Synapse('varchar', ['length' => '25']),
            ),
        ];

        yield 'native type test - different backend' => [
            'tableMetadata' => [
                [
                    'key' => 'KBC.datatype.backend',
                    'value' => 'synapse',
                ],
            ],
            'columnName' => 'testColumn',
            'columnMetadata' => (new Synapse('varchar', ['length' => '25']))->toMetadata(),
            'backendType' => 'snowflake',
            'expectedColumn' => new BaseTypeTableDefinitionColumn('testColumn', 'STRING'),
        ];
    }
}
