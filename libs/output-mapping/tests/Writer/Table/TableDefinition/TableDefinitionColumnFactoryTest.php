<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\TableDefinition;

use Keboola\Datatype\Definition\DefinitionInterface;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionColumnFactory;

class TableDefinitionColumnFactoryTest extends \PHPUnit\Framework\TestCase
{
    /** @dataProvider createTableDefinitionColumnProfider */
    public function testFactoryCreateTableDefinitionColumn(
        string $columnName,
        ?string $nativeTypeClass,
        array $columnMetadata,
        array $expectedSerialisation
    ): void {
        $columnFactory = new TableDefinitionColumnFactory($nativeTypeClass);
        $column = $columnFactory->createTableDefinitionColumn($columnName, $columnMetadata);
        $this->assertSame($expectedSerialisation, $column->toArray());
    }

    public function createTableDefinitionColumnProfider(): \Generator
    {
        yield [
            'testNoDefinitionUseBaseType',
            null,
            (new GenericStorage('varchar'))->toMetadata(),
            [
                'name' => 'testNoDefinitionUseBaseType',
                'basetype' => 'STRING',
            ],
        ];

        yield [
            'testTime',
            Snowflake::class,
            (new Snowflake(Snowflake::TYPE_TIME))->toMetadata(),
            [
                'name' => 'testTime',
                'definition' => [
                    'type' => Snowflake::TYPE_TIME,
                    'length' => null,
                    'nullable' => true,
                ],
            ],
        ];

        yield [
            'testPreferNativeType',
            Snowflake::class,
            (new Snowflake(Snowflake::TYPE_TEXT, ['nullable' => false, 'length' => '123']))->toMetadata(),
            [
                'name' => 'testPreferNativeType',
                'definition' => [
                    'type' => Snowflake::TYPE_TEXT, // in snowflake, Text is just an alias for Varchar.
                    'length' => '123',
                    'nullable' => false,
                ],
            ],
        ];

        yield [
            'testDecimalWithLength',
            Snowflake::class,
            (new Snowflake(Snowflake::TYPE_DECIMAL, ['nullable' => false, 'length' => '10,2']))->toMetadata(),
            [
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
