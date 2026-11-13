<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\TableDefinition;

use Generator;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\OutputMapping\Mapping\MappingTableLayout;
use Keboola\OutputMapping\Storage\TableDescription;
use Keboola\OutputMapping\Writer\Table\TableDefinition\BaseTypeTableDefinitionColumn;
use Keboola\OutputMapping\Writer\Table\TableDefinition\NativeTableDefinitionColumn;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinition;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionColumnFactory;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionColumnInterface;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

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
                    'value' => 'bigquery',
                ],
            ],
            'columnName' => 'testColumn',
            'columnMetadata' => (new Bigquery('STRING', ['length' => '25']))->toMetadata(),
            'backendType' => 'bigquery',
            'expectedColumn' => new NativeTableDefinitionColumn(
                'testColumn',
                new Bigquery('STRING', ['length' => '25']),
            ),
        ];

        yield 'native type test - different backend' => [
            'tableMetadata' => [
                [
                    'key' => 'KBC.datatype.backend',
                    'value' => 'bigquery',
                ],
            ],
            'columnName' => 'testColumn',
            'columnMetadata' => (new Bigquery('STRING', ['length' => '25']))->toMetadata(),
            'backendType' => 'snowflake',
            'expectedColumn' => new BaseTypeTableDefinitionColumn('testColumn', 'STRING'),
        ];
    }

    public function testSetAndGetTableName(): void
    {
        $tableDefinition = new TableDefinition($this->createMock(TableDefinitionColumnFactory::class));

        $tableDefinition->setTableName('testTableName');
        self::assertSame('testTableName', $tableDefinition->getTableName());
    }

    public function testRequestDataWithLayout(): void
    {
        $tableDefinition = new TableDefinition(new TableDefinitionColumnFactory([], 'bigquery', true));
        $tableDefinition->setTableName('testTable');
        $tableDefinition->setPrimaryKeysNames(['Id']);
        $tableDefinition->addColumn('Id', (new GenericStorage('DATE'))->toMetadata());
        $tableDefinition->addColumn('Name', (new GenericStorage('STRING'))->toMetadata());

        self::assertArrayNotHasKey('timePartitioning', $tableDefinition->getRequestData());
        self::assertArrayNotHasKey('rangePartitioning', $tableDefinition->getRequestData());
        self::assertArrayNotHasKey('clustering', $tableDefinition->getRequestData());

        $tableDefinition->setDescriptions(
            new TableDescription('in.c-main.testTable', 'table description', ['Id' => 'id description']),
            new NullLogger(),
        );
        $tableDefinition->setLayout(new MappingTableLayout(
            ['type' => 'DAY', 'field' => 'Id', 'expiration_ms' => 1000],
            null,
            ['fields' => ['Name']],
        ));

        self::assertSame(
            [
                'name' => 'testTable',
                'primaryKeysNames' => ['Id'],
                'columns' => [
                    ['name' => 'Id', 'basetype' => 'DATE', 'definition' => ['description' => 'id description']],
                    ['name' => 'Name', 'basetype' => 'STRING'],
                ],
                'description' => 'table description',
                'timePartitioning' => ['type' => 'DAY', 'field' => 'Id', 'expirationMs' => 1000],
                'clustering' => ['fields' => ['Name']],
            ],
            $tableDefinition->getRequestData(),
        );
    }
}
