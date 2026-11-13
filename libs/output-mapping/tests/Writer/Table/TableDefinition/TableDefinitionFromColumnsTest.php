<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\TableDefinition;

use Keboola\OutputMapping\Mapping\MappingTableLayout;
use Keboola\OutputMapping\Storage\TableDescription;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionFromColumns;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class TableDefinitionFromColumnsTest extends TestCase
{
    public function testRequestDataCarriesNoTypes(): void
    {
        $tableDefinition = new TableDefinitionFromColumns('testTable', ['Id', 'Name'], ['Id']);

        self::assertSame('testTable', $tableDefinition->getTableName());
        self::assertSame(
            [
                'name' => 'testTable',
                'primaryKeysNames' => ['Id'],
                // neither `definition.type` nor `basetype`, otherwise Storage would create a typed table
                'columns' => [
                    ['name' => 'Id'],
                    ['name' => 'Name'],
                ],
            ],
            $tableDefinition->getRequestData(),
        );
    }

    public function testRequestDataWithoutPrimaryKey(): void
    {
        $tableDefinition = new TableDefinitionFromColumns('testTable', ['Id'], []);

        self::assertSame(
            [
                'name' => 'testTable',
                'primaryKeysNames' => [],
                'columns' => [['name' => 'Id']],
            ],
            $tableDefinition->getRequestData(),
        );
    }

    /**
     * The column list comes from the manifest through RestrictedColumnsHelper, which filters with array_filter
     * and therefore leaves gaps in the keys - those must not turn the JSON arrays into objects.
     */
    public function testRequestDataReindexesSparseInput(): void
    {
        $tableDefinition = new TableDefinitionFromColumns(
            'testTable',
            [1 => 'Id', 3 => 'Name'],
            [2 => 'Id'],
        );

        $requestData = $tableDefinition->getRequestData();

        self::assertSame(['Id'], $requestData['primaryKeysNames']);
        self::assertSame([['name' => 'Id'], ['name' => 'Name']], $requestData['columns']);
        self::assertSame('[{"name":"Id"},{"name":"Name"}]', json_encode($requestData['columns']));
    }

    public function testRequestDataWithLayout(): void
    {
        $tableDefinition = new TableDefinitionFromColumns('testTable', ['Id', 'Name'], ['Id']);

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
                    ['name' => 'Id', 'definition' => ['description' => 'id description']],
                    ['name' => 'Name'],
                ],
                'description' => 'table description',
                'timePartitioning' => ['type' => 'DAY', 'field' => 'Id', 'expirationMs' => 1000],
                'clustering' => ['fields' => ['Name']],
            ],
            $tableDefinition->getRequestData(),
        );
    }
}
