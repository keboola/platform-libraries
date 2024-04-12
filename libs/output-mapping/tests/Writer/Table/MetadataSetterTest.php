<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table;

use Generator;
use Keboola\OutputMapping\DeferredTasks\TableWriter\LoadTableTask;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Mapping\MappingStorageSources;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\OutputMapping\Writer\Table\MetadataSetter;
use PHPUnit\Framework\TestCase;

class MetadataSetterTest extends TestCase
{
    /**
     * @dataProvider provideSetTableMetadata
     */
    public function testSetTableMetadata(
        MappingFromProcessedConfiguration $mockMappingFromProcessedConfiguration,
        bool $didTableExistBefore,
        int $expectedCountMetadata,
    ): void {
        $mappingDestination = new MappingDestination('in.c-main.table');

        $mockMappingStorageSources = $this->createMock(MappingStorageSources::class);
        $mockMappingStorageSources->method('didTableExistBefore')->willReturn($didTableExistBefore);

        $metadataSetter = new MetadataSetter();
        $loadTask = $metadataSetter->setTableMetadata(
            loadTask: new LoadTableTask($mappingDestination, [], false),
            processedSource: $mockMappingFromProcessedConfiguration,
            storageSources: $mockMappingStorageSources,
            systemMetadata: new SystemMetadata(['componentId' => 'keboola.test']),
        );

        self::assertCount($expectedCountMetadata, $loadTask->getMetadata());
    }

    public function provideSetTableMetadata(): Generator
    {
        $mappingDestination = new MappingDestination('in.c-main.table');

        $mockMappingFromProcessedConfiguration = $this->createMock(MappingFromProcessedConfiguration::class);
        $mockMappingFromProcessedConfiguration->method('getDestination')->willReturn($mappingDestination);
        $mockMappingFromProcessedConfiguration->method('hasMetadata')->willReturn(false);
        $mockMappingFromProcessedConfiguration->method('hasColumnMetadata')->willReturn(false);

        yield 'set-minimal' => [
            $mockMappingFromProcessedConfiguration,
            true,
            1,
        ];

        $mockMappingFromProcessedConfiguration = $this->createMock(MappingFromProcessedConfiguration::class);
        $mockMappingFromProcessedConfiguration->method('getDestination')->willReturn($mappingDestination);
        $mockMappingFromProcessedConfiguration->method('hasMetadata')->willReturn(true);
        $mockMappingFromProcessedConfiguration->method('getMetadata')->willReturn(['metadata']);
        $mockMappingFromProcessedConfiguration->method('hasColumnMetadata')->willReturn(false);

        yield 'set-metadata' => [
            $mockMappingFromProcessedConfiguration,
            true,
            2,
        ];

        $mockMappingFromProcessedConfiguration = $this->createMock(MappingFromProcessedConfiguration::class);
        $mockMappingFromProcessedConfiguration->method('getDestination')->willReturn($mappingDestination);
        $mockMappingFromProcessedConfiguration->method('hasMetadata')->willReturn(false);
        $mockMappingFromProcessedConfiguration->method('hasColumnMetadata')->willReturn(true);
        $mockMappingFromProcessedConfiguration->method('getColumnMetadata')->willReturn(['columnMetadata']);

        yield 'set-column-metadata' => [
            $mockMappingFromProcessedConfiguration,
            true,
            2,
        ];

        $mockMappingFromProcessedConfiguration = $this->createMock(MappingFromProcessedConfiguration::class);
        $mockMappingFromProcessedConfiguration->method('getDestination')->willReturn($mappingDestination);
        $mockMappingFromProcessedConfiguration->method('hasMetadata')->willReturn(true);
        $mockMappingFromProcessedConfiguration->method('getMetadata')->willReturn(['metadata']);
        $mockMappingFromProcessedConfiguration->method('hasColumnMetadata')->willReturn(true);
        $mockMappingFromProcessedConfiguration->method('getColumnMetadata')->willReturn(['columnMetadata']);

        yield 'set-all' => [
            $mockMappingFromProcessedConfiguration,
            false,
            4,
        ];
    }
}
