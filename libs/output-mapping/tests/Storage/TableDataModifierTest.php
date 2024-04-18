<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Storage\TableDataModifier;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsTestTables;
use Keboola\OutputMapping\Writer\Table\MappingDestination;

class TableDataModifierTest extends AbstractTestCase
{
    #[NeedsTestTables(1)]
    public function testDeleteTableRows(): void
    {
        $tableDataModifier = new TableDataModifier($this->clientWrapper);

        $destination = new MappingDestination($this->firstTableId);

        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getDeleteWhereColumn')->willReturn('Id');
        $source->method('getDeleteWhereOperator')->willReturn('eq');
        $source->method('getDeleteWhereValues')->willReturn(['id1', 'id2']);

        $tableDataModifier->updateTableData($source, $destination);

        $newTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId);

        $this->assertEquals(1, $newTable['rowsCount']);
    }

    #[NeedsTestTables(1)]
    public function testWhereColumnNotSet(): void
    {
        $tableDataModifier = new TableDataModifier($this->clientWrapper);

        $destination = new MappingDestination($this->firstTableId);

        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getDeleteWhereColumn')->willReturn(null);
        $source->method('getDeleteWhereOperator')->willReturn('eq');
        $source->method('getDeleteWhereValues')->willReturn(['id1', 'id2']);

        $tableDataModifier->updateTableData($source, $destination);

        $newTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId);

        $this->assertEquals(3, $newTable['rowsCount']);
    }
}
