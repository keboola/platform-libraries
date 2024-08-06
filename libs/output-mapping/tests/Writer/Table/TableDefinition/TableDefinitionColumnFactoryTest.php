<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\TableDefinition;

use Generator;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionColumnFactory;
use PHPUnit\Framework\TestCase;

class TableDefinitionColumnFactoryTest extends TestCase
{
    /** @dataProvider createTableDefinitionColumnProvider */
    public function testFactoryCreateTableDefinitionColumn(
        string $columnName,
        array $columnMetadata,
        array $tableMetadata,
        string $backendType,
        array $expectedSerialisation,
    ): void {
        $columnFactory = new TableDefinitionColumnFactory($tableMetadata, $backendType);
        $column = $columnFactory->createTableDefinitionColumn($columnName, $columnMetadata);
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
        ];

        yield 'native type without basetype' => [
            'columnName' => 'testDecimalWithLength',
            'columnMetadata' => [
                [
                    'key' => Snowflake::KBC_METADATA_KEY_NULLABLE,
                    'value' => false,
                ],
            ],
            'tableMetadata' => [],
            'backendType' => 'snowflake',
            'expectedSerialisation' => [
                'name' => 'testDecimalWithLength',
            ],
        ];
    }
}
