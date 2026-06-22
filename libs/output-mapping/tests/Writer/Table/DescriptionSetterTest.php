<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table;

use Keboola\OutputMapping\DeferredTasks\LoadTableTaskInterface;
use Keboola\OutputMapping\DeferredTasks\TableDefinition\TableDefinitionDescription;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Writer\Table\DescriptionSetter;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use PHPUnit\Framework\TestCase;

class DescriptionSetterTest extends TestCase
{
    public function testSetsDescriptionForFreshlyCreatedTable(): void
    {
        $processedSource = $this->createMock(MappingFromProcessedConfiguration::class);
        $processedSource->method('getDestination')->willReturn(new MappingDestination('in.c-main.table'));
        $processedSource->method('getTableDescription')->willReturn('table desc');
        $processedSource->method('getColumnDescriptions')->willReturn(['col1' => 'col1 desc']);

        $loadTask = $this->createMock(LoadTableTaskInterface::class);
        $loadTask->method('isUsingFreshlyCreatedTable')->willReturn(true);
        $loadTask->expects(self::once())
            ->method('setDescription')
            ->with(self::isInstanceOf(TableDefinitionDescription::class));

        (new DescriptionSetter())->setDescription($loadTask, $processedSource);
    }

    public function testDoesNotSetDescriptionForExistingTable(): void
    {
        // never overwrite a description on a pre-existing table (it may have been set by a user in the UI)
        $processedSource = $this->createMock(MappingFromProcessedConfiguration::class);
        $processedSource->method('getDestination')->willReturn(new MappingDestination('in.c-main.table'));
        $processedSource->method('getTableDescription')->willReturn('table desc');
        $processedSource->method('getColumnDescriptions')->willReturn(['col1' => 'col1 desc']);

        $loadTask = $this->createMock(LoadTableTaskInterface::class);
        $loadTask->method('isUsingFreshlyCreatedTable')->willReturn(false);
        $loadTask->expects(self::never())->method('setDescription');

        (new DescriptionSetter())->setDescription($loadTask, $processedSource);
    }

    public function testDoesNotSetDescriptionWhenEmpty(): void
    {
        $processedSource = $this->createMock(MappingFromProcessedConfiguration::class);
        $processedSource->method('getDestination')->willReturn(new MappingDestination('in.c-main.table'));
        $processedSource->method('getTableDescription')->willReturn(null);
        $processedSource->method('getColumnDescriptions')->willReturn([]);

        $loadTask = $this->createMock(LoadTableTaskInterface::class);
        $loadTask->method('isUsingFreshlyCreatedTable')->willReturn(true);
        $loadTask->expects(self::never())->method('setDescription');

        (new DescriptionSetter())->setDescription($loadTask, $processedSource);
    }
}
