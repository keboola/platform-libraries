<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Mapping;

use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalData;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Keboola\OutputMapping\Writer\FileItem;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\OutputMapping\Writer\Table\Source\WorkspaceItemSource;
use PHPUnit\Framework\TestCase;

class MappingFromProcessedConfigurationTest extends TestCase
{
    public function testBasic(): void
    {
        $mapping = [
            'destination' => $this->createMock(MappingDestination::class),
            'delimiter' => ',',
            'enclosure' => '"',
        ];

        $sourceMock = $this->createMock(MappingFromRawConfigurationAndPhysicalData::class);
        $sourceMock->method('isSliced')->willReturn(false);
        $sourceMock->method('getPathName')->willReturn('sourcePathName');
        $sourceMock->method('getPath')->willReturn('sourcePath');
        $sourceMock->method('getManifestName')->willReturn('sourceManifestName');
        $sourceMock->method('getConfiguration')->willReturn(null);
        $sourceMock->method('getWorkspaceId')->willReturn('workspaceId');
        $sourceMock->method('getDataObject')->willReturn('dataObject');
        $sourceMock->method('getSourceName')->willReturn('sourceName');
        $sourceMock->method('getItemSourceClass')->willReturn(WorkspaceItemSource::class);

        $fileItemMock = $this->createMock(FileItem::class);

        $physicalDataWithManifest = new MappingFromRawConfigurationAndPhysicalDataWithManifest(
            $sourceMock,
            $fileItemMock,
        );
        $mapping = new MappingFromProcessedConfiguration($mapping, $physicalDataWithManifest);

        self::assertEquals('sourceName', $mapping->getSourceName());
        self::assertEquals('workspaceId', $mapping->getWorkspaceId());
        self::assertEquals('dataObject', $mapping->getDataObject());
        self::assertEquals('sourcePathName', $mapping->getPathName());
        self::assertEquals('eq', $mapping->getDeleteWhereOperator());
        self::assertEquals(',', $mapping->getDelimiter());
        self::assertEquals('"', $mapping->getEnclosure());
        self::assertEquals([], $mapping->getDeleteWhereValues());
        self::assertEquals([], $mapping->getColumnMetadata());
        self::assertEquals([], $mapping->getColumns());
        self::assertEquals([], $mapping->getDistributionKey());
        self::assertEquals([], $mapping->getMetadata());
        self::assertEquals([], $mapping->getPrimaryKey());
        self::assertEquals([], $mapping->getTags());
        self::assertNull($mapping->getDeleteWhereColumn());
        self::assertFalse($mapping->isSliced());
        self::assertFalse($mapping->hasColumnMetadata());
        self::assertFalse($mapping->hasColumns());
        self::assertFalse($mapping->hasDistributionKey());
        self::assertFalse($mapping->hasMetadata());
        self::assertFalse($mapping->hasWriteAlways());
        self::assertFalse($mapping->isIncremental());
        self::assertEquals(WorkspaceItemSource::class, $mapping->getItemSourceClass());
        self::assertInstanceOf(MappingDestination::class, $mapping->getDestination());
    }
}
