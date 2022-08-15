<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\TableDefinition;

use Keboola\Datatype\Definition\GenericStorage;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionColumnFactory;

class TableDefinitionColumnFactoryTest extends \PHPUnit\Framework\TestCase
{
    /** @dataProvider createTableDefinitionColumnProfider */
    public function testFactoryCreateTableDefinitionColumn(
        string $columnName,
        array $columnMetadata,
        array $tableMetadata,
        string $backendType,
        array $expectedSerialisation
    ): void {
        $columnFactory = new TableDefinitionColumnFactory($tableMetadata, $backendType);
        $column = $columnFactory->createTableDefinitionColumn($columnName, $columnMetadata);
        $this->assertSame($expectedSerialisation, $column->toArray());
    }

    public function createTableDefinitionColumnProfider(): \Generator
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
            'columnMetadata' => (new Snowflake(Snowflake::TYPE_TIME))->toMetadata(),
            'tableMetadata' => [
                [
                    'key' => TableDefinitionColumnFactory::NATIVE_TYPE_METADATA_KEY,
                    'value' => 'snowflake',
                ],
            ],
            'backendType' => 'snowflake',
            'expectedSerialisation' => [
                'name' => 'testTime',
                'definition' => [
                    'type' => Snowflake::TYPE_TIME,
                    'length' => null,
                    'nullable' => true,
                ],
            ],
        ];

        yield 'snowflake native missing tableMetadata' => [
            'columnName' => 'testNativeToBaseType',
            'columnMetadata' => (new Snowflake(Snowflake::TYPE_TEXT, ['nullable' => false, 'length' => '123']))->toMetadata(),
            'tableMetadata' => [],
            'backendType' => 'snowflake',
            'expectedSerialisation' => [
                'name' => 'testNativeToBaseType',
                'basetype' => 'STRING',
            ],
        ];

        yield 'full native type definition' => [
            'columnName' => 'testDecimalWithLength',
            'columnMetadata' => (new Snowflake(Snowflake::TYPE_DECIMAL, ['nullable' => false, 'length' => '10,2']))->toMetadata(),
            'tableMetadata' => [
                [
                    'key' => TableDefinitionColumnFactory::NATIVE_TYPE_METADATA_KEY,
                    'value' => 'snowflake',
                ],
            ],
            'backendType' => 'snowflake',
            'expectedSerialisation' => [
                'name' => 'testDecimalWithLength',
                'definition' => [
                    'type' => Snowflake::TYPE_DECIMAL,
                    'length' => '10,2',
                    'nullable' => false,
                ],
            ],
        ];
    }
}
