<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\TableDefinition;

use Keboola\Datatype\Definition\GenericStorage;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionFactory;
use PhpUnit\Framework\TestCase;

class TableDefinitionFactoryTest extends TestCase
{
    /** @dataProvider createTableDefinitionProvider */
    public function testFactoryCreateTableDefinition(
        string $tableName,
        array $primaryKeyNames,
        array $columnMetadata,
        array $expectedSerialization
    ): void {
        $tableDefinitionFactory = new TableDefinitionFactory();
        $tableDefinition = $tableDefinitionFactory->createTableDefinition($tableName, $primaryKeyNames, $columnMetadata);
        $this->assertSame($expectedSerialization, $tableDefinition->getRequestData());
    }

    public function createTableDefinitionProvider(): \Generator
    {
        yield [
            'basicTable',
            [
                'one', 'two',
            ],
            [
                'Id' => (new GenericStorage('int', ['nullable' => false]))->toMetadata(),
                'Name' => (new GenericStorage('varchar', ['length' => '17', 'nullable' => false]))->toMetadata(),
                'birthday' => (new GenericStorage('date'))->toMetadata(),
                'created' => (new GenericStorage('timestamp'))->toMetadata(),
            ],
            [
                'name' => 'basicTable',
                'primaryKeysNames' => ['one', 'two'],
                'columns' => [
                    [
                        'name' => 'Id',
                        'basetype' => 'INTEGER'
                    ],
                    [
                        'name' => 'Name',
                        'basetype' => 'STRING'
                    ],
                    [
                        'name' => 'birthday',
                        'basetype' => 'DATE'
                    ],
                    [
                        'name' => 'created',
                        'basetype' => 'TIMESTAMP'
                    ],
                ],
            ],
        ];
    }
}
