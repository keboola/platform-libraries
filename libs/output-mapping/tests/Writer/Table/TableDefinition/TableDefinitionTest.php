<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table;

use Keboola\Datatype\Definition\GenericStorage;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinition;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionColumn;
use PHPUnit\Framework\TestCase;

class TableDefinitionTest extends TestCase
{
    /** @dataProvider addColumnProvider */
    public function testAddTableDefinitionColumn(
        TableDefinition $definition,
        string $columnName,
        array $metadata,
        TableDefinitionColumn $expectedColumn
    ): void {
        $definition->addColumn($columnName, $metadata);
        $this->assertCount(1, $definition->getColumns());
        $this->assertEquals($expectedColumn, $definition->getColumns()[0]);
    }

    public function addColumnProvider(): \Generator
    {
        yield [
            new TableDefinition(Snowflake::class),
            'testColumn',
            (new GenericStorage('varchar', ['length' => '25']))->toMetadata(),
            new TableDefinitionColumn('testColumn', 'STRING'),
        ];
    }
}
