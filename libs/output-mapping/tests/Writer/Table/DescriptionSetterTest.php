<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table;

use Keboola\OutputMapping\DeferredTasks\LoadTableTaskInterface;
use Keboola\OutputMapping\DeferredTasks\Metadata\ColumnsMetadata;
use Keboola\OutputMapping\DeferredTasks\Metadata\MetadataInterface;
use Keboola\OutputMapping\DeferredTasks\Metadata\TableMetadata;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Writer\Table\DescriptionSetter;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use PHPUnit\Framework\TestCase;

class DescriptionSetterTest extends TestCase
{
    private function systemMetadata(): SystemMetadata
    {
        return new SystemMetadata(['componentId' => 'keboola.my-component']);
    }

    public function testWritesComponentMetadataForExistingTable(): void
    {
        $processedSource = $this->createMock(MappingFromProcessedConfiguration::class);
        $processedSource->method('getDestination')->willReturn(new MappingDestination('in.c-main.table'));
        $processedSource->method('getTableDescription')->willReturn('table desc');
        $processedSource->method('getColumnDescriptions')->willReturn(['col1' => 'col1 desc']);

        $collected = [];
        $loadTask = $this->createMock(LoadTableTaskInterface::class);
        $loadTask->method('isDescriptionInTableDefinition')->willReturn(false);
        $loadTask->expects(self::exactly(2))
            ->method('addMetadata')
            ->willReturnCallback(function (MetadataInterface $metadata) use (&$collected): void {
                $collected[] = $metadata;
            });

        (new DescriptionSetter())->setDescription($loadTask, $processedSource, $this->systemMetadata());

        self::assertInstanceOf(TableMetadata::class, $collected[0]);
        self::assertInstanceOf(ColumnsMetadata::class, $collected[1]);
    }

    public function testSkipsWhenDescriptionIsInTableDefinition(): void
    {
        $processedSource = $this->createMock(MappingFromProcessedConfiguration::class);
        $processedSource->method('getDestination')->willReturn(new MappingDestination('in.c-main.table'));
        $processedSource->method('getTableDescription')->willReturn('table desc');
        $processedSource->method('getColumnDescriptions')->willReturn(['col1' => 'col1 desc']);

        $loadTask = $this->createMock(LoadTableTaskInterface::class);
        $loadTask->method('isDescriptionInTableDefinition')->willReturn(true);
        $loadTask->expects(self::never())->method('addMetadata');

        (new DescriptionSetter())->setDescription($loadTask, $processedSource, $this->systemMetadata());
    }

    public function testDoesNothingWhenNoDescription(): void
    {
        $processedSource = $this->createMock(MappingFromProcessedConfiguration::class);
        $processedSource->method('getDestination')->willReturn(new MappingDestination('in.c-main.table'));
        $processedSource->method('getTableDescription')->willReturn(null);
        $processedSource->method('getColumnDescriptions')->willReturn([]);

        $loadTask = $this->createMock(LoadTableTaskInterface::class);
        $loadTask->method('isDescriptionInTableDefinition')->willReturn(false);
        $loadTask->expects(self::never())->method('addMetadata');

        (new DescriptionSetter())->setDescription($loadTask, $processedSource, $this->systemMetadata());
    }
}
