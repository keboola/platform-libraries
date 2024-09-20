<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\TableDefinition;

use Generator;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\Datatype\Definition\Common;
use Keboola\Datatype\Definition\Exasol;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\OutputMapping\Writer\Table\TableDefinition\BaseTypeTableDefinitionColumn;
use Keboola\OutputMapping\Writer\Table\TableDefinition\NativeTableDefinitionColumn;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionColumnFactory;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionColumnInterface;
use PHPUnit\Framework\TestCase;

class TableDefinitionColumnFactoryTest extends TestCase
{
    /**
     * @dataProvider createTableDefinitionColumnProvider
     * @param  class-string<TableDefinitionColumnInterface> $expectedTabeDefinitionColumnClass
     */
    public function testFactoryCreateTableDefinitionColumn(
        string $columnName,
        array $columnMetadata,
        array $tableMetadata,
        string $backendType,
        array $expectedSerialisation,
        string $expectedTabeDefinitionColumnClass,
    ): void {
        $columnFactory = new TableDefinitionColumnFactory($tableMetadata, $backendType);
        $column = $columnFactory->createTableDefinitionColumn($columnName, $columnMetadata);
        self::assertInstanceOf($expectedTabeDefinitionColumnClass, $column);
        self::assertSame($expectedSerialisation, $column->toArray());
    }

    public function createTableDefinitionColumnProvider(): Generator
    {
        yield 'simple basetype' =>[
            'columnName' => 'testNoDefinitionUseBaseType',
            'columnMetadata' => (new GenericStorage('varchar'))->toMetadata(),
            'tableMetadata' => [],
            'backendType' => 'snowflake',
            'expectedSerialisation' => [
                'name' => 'testNoDefinitionUseBaseType',
                'basetype' => 'STRING',
            ],
            'expectedTabeDefinitionColumnClass' => BaseTypeTableDefinitionColumn::class,
        ];

        yield 'snowflake native' => [
            'columnName' => 'testTime',
            'columnMetadata' => (new Snowflake('TIME'))->toMetadata(),
            'tableMetadata' => [
                [
                    'key' => 'KBC.datatype.backend',
                    'value' => 'snowflake',
                ],
            ],
            'backendType' => 'snowflake',
            'expectedSerialisation' => [
                'name' => 'testTime',
                'definition' => [
                    'type' => 'TIME',
                    'length' => null,
                    'nullable' => true,
                ],
            ],
            'expectedTabeDefinitionColumnClass' => NativeTableDefinitionColumn::class,
        ];

        yield 'snowflake native missing tableMetadata' => [
            'columnName' => 'testNativeToBaseType',
            'columnMetadata' => (new Snowflake('TEXT', ['nullable' => false, 'length' => '123']))->toMetadata(),
            'tableMetadata' => [],
            'backendType' => 'snowflake',
            'expectedSerialisation' => [
                'name' => 'testNativeToBaseType',
                'basetype' => 'STRING',
            ],
            'expectedTabeDefinitionColumnClass' => BaseTypeTableDefinitionColumn::class,
        ];

        yield 'full native type definition' => [
            'columnName' => 'testDecimalWithLength',
            'columnMetadata' => (new Snowflake('DECIMAL', ['nullable' => false, 'length' => '10,2']))->toMetadata(),
            'tableMetadata' => [
                [
                    'key' => 'KBC.datatype.backend',
                    'value' => 'snowflake',
                ],
            ],
            'backendType' => 'snowflake',
            'expectedSerialisation' => [
                'name' => 'testDecimalWithLength',
                'definition' => [
                    'type' => 'DECIMAL',
                    'length' => '10,2',
                    'nullable' => false,
                ],
            ],
            'expectedTabeDefinitionColumnClass' => NativeTableDefinitionColumn::class,
        ];

        yield 'native type without basetype' => [
            'columnName' => 'testDecimalWithLength',
            'columnMetadata' => [
                [
                    'key' => Common::KBC_METADATA_KEY_NULLABLE,
                    'value' => false,
                ],
            ],
            'tableMetadata' => [],
            'backendType' => 'snowflake',
            'expectedSerialisation' => [
                'name' => 'testDecimalWithLength',
            ],
            'expectedTabeDefinitionColumnClass' => BaseTypeTableDefinitionColumn::class,
        ];

        yield 'different backend' => [
            'columnName' => 'testTime',
            'columnMetadata' => (new Bigquery('TIME'))->toMetadata(),
            'tableMetadata' => [
                [
                    'key' => 'KBC.datatype.backend',
                    'value' => 'bigquery',
                ],
            ],
            'backendType' => 'snowflake',
            'expectedSerialisation' => [
                'name' => 'testTime',
                'basetype' => 'TIMESTAMP',
            ],
            'expectedTabeDefinitionColumnClass' => BaseTypeTableDefinitionColumn::class,
        ];

        yield 'bigquery native' => [
            'columnName' => 'testTime',
            'columnMetadata' => (new Bigquery('TIME'))->toMetadata(),
            'tableMetadata' => [
                [
                    'key' => 'KBC.datatype.backend',
                    'value' => 'bigquery',
                ],
            ],
            'backendType' => 'bigquery',
            'expectedSerialisation' => [
                'name' => 'testTime',
                'definition' => [
                    'type' => 'TIME',
                    'length' => null,
                    'nullable' => true,
                ],
            ],
            'expectedTabeDefinitionColumnClass' => NativeTableDefinitionColumn::class,
        ];

        yield 'exasol native' => [
            'columnName' => 'testTime',
            'columnMetadata' => (new Exasol('TIMESTAMP'))->toMetadata(),
            'tableMetadata' => [
                [
                    'key' => 'KBC.datatype.backend',
                    'value' => 'exasol',
                ],
            ],
            'backendType' => 'exasol',
            'expectedSerialisation' => [
                'name' => 'testTime',
                'definition' => [
                    'type' => 'TIMESTAMP',
                    'length' => null,
                    'nullable' => true,
                ],
            ],
            'expectedTabeDefinitionColumnClass' => NativeTableDefinitionColumn::class,
        ];
    }
}
