<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\TableDefinition;

use Generator;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\Datatype\Definition\GenericStorage;
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

    public function testRequestDataContainsTableAndColumnDescriptions(): void
    {
        $tableDefinition = new TableDefinition(
            new TableDefinitionColumnFactory([], 'snowflake', false),
        );
        $tableDefinition->setTableName('testTableName');
        $tableDefinition->setPrimaryKeysNames(['id']);
        $tableDefinition->setTableDescription('table description');
        $tableDefinition->setColumnDescriptions(['id' => 'id description']);
        $tableDefinition->addColumn('id', (new GenericStorage('varchar', ['length' => '25']))->toMetadata());
        $tableDefinition->addColumn('name', (new GenericStorage('varchar', ['length' => '25']))->toMetadata());

        $requestData = $tableDefinition->getRequestData();

        self::assertSame('table description', $requestData['description']);
        self::assertSame('id', $requestData['columns'][0]['name']);
        self::assertSame('id description', $requestData['columns'][0]['description']);
        self::assertSame('name', $requestData['columns'][1]['name']);
        self::assertArrayNotHasKey('description', $requestData['columns'][1]);
    }

    public function testRequestDataOmitsDescriptionWhenNotSet(): void
    {
        $tableDefinition = new TableDefinition(
            new TableDefinitionColumnFactory([], 'snowflake', false),
        );
        $tableDefinition->setTableName('testTableName');
        $tableDefinition->setPrimaryKeysNames([]);
        $tableDefinition->addColumn('id', (new GenericStorage('varchar', ['length' => '25']))->toMetadata());

        $requestData = $tableDefinition->getRequestData();

        self::assertArrayNotHasKey('description', $requestData);
        self::assertArrayNotHasKey('description', $requestData['columns'][0]);
    }
}
