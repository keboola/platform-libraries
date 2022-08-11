<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\TableDefinition;

use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\DefinitionInterface;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionColumn;
use PHPUnit\Framework\TestCase;

class TableDefinitionColumnTest extends TestCase
{
    /** @dataProvider createTableDefinitionColumnProvider */
    public function testCreateTableDefinitionColumn(
        string $name,
        ?DefinitionInterface $dataTypeDefinition,
        ?string $baseType,
        array $expectedSerialisation
    ): void {
        self::assertSame(
            (new TableDefinitionColumn($name, $dataTypeDefinition, $baseType))->toArray(),
            $expectedSerialisation
        );
    }

    public function createTableDefinitionColumnProvider(): \Generator
    {
        yield [
            'testNoBaseTypeNoDefinition',
            null,
            null,
            [
                'name' => 'testNoBaseTypeNoDefinition',
            ],
        ];

        yield [
            'testUsingBaseType',
            null,
            BaseType::BOOLEAN,
            [
                'name' => 'testUsingBaseType',
                'basetype' => BaseType::BOOLEAN,
            ]
        ];

        yield [
            'testPreferDefinition',
            (new Snowflake(Snowflake::TYPE_DECIMAL, ['nullable' => false, 'length' => '10,2'])),
            BaseType::BOOLEAN,
            [
                'name' => 'testPreferDefinition',
                'definition' => [
                    'type' => Snowflake::TYPE_DECIMAL,
                    'length' => '10,2',
                    'nullable' => false,
                ],
            ],
        ];
    }
}
