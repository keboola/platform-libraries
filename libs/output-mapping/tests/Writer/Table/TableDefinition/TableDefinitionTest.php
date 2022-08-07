<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table;

use Keboola\Datatype\Definition\GenericStorage;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinition;
use PHPUnit\Framework\TestCase;

class TableDefinitionTest extends TestCase
{
    /** @dataProvider addColumnProvider */
    public function testAddColumn(TableDefinition $definition, string $columnName, array $metadata): void
    {
        $definition->addColumn($columnName, $metadata);
        $this->assertEquals();
    }

    public function addColumnProvider(): \Generator
    {
        yield [
            new TableDefinition(),
            'testColumn',
            (new GenericStorage('varchar', ['length' => '25']))->toMetadata(),
        ];
    }
}
