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
    private MappingFromProcessedConfiguration $mapping;

    protected function setUp(): void
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
        $this->mapping = new MappingFromProcessedConfiguration($mapping, $physicalDataWithManifest);
    }

    public function testBasic(): void
    {
        $this->assertEquals('sourceName', $this->mapping->getSourceName());
        $this->assertEquals('workspaceId', $this->mapping->getWorkspaceId());
        $this->assertEquals('dataObject', $this->mapping->getDataObject());
        $this->assertEquals('sourcePathName', $this->mapping->getPathName());
        $this->assertEquals('eq', $this->mapping->getDeleteWhereOperator());
        $this->assertEquals(',', $this->mapping->getDelimiter());
        $this->assertEquals('"', $this->mapping->getEnclosure());
        $this->assertEquals([], $this->mapping->getDeleteWhereValues());
        $this->assertEquals([], $this->mapping->getColumnMetadata());
        $this->assertEquals([], $this->mapping->getColumns());
        $this->assertEquals([], $this->mapping->getDistributionKey());
        $this->assertEquals([], $this->mapping->getMetadata());
        $this->assertEquals([], $this->mapping->getPrimaryKey());
        $this->assertEquals([], $this->mapping->getTags());
        $this->assertNull($this->mapping->getDeleteWhereColumn());
        $this->assertFalse($this->mapping->isSliced());
        $this->assertFalse($this->mapping->hasColumnMetadata());
        $this->assertFalse($this->mapping->hasColumns());
        $this->assertFalse($this->mapping->hasDistributionKey());
        $this->assertFalse($this->mapping->hasMetadata());
        $this->assertFalse($this->mapping->hasWriteAlways());
        $this->assertFalse($this->mapping->isIncremental());
        $this->assertEquals(WorkspaceItemSource::class, $this->mapping->getItemSourceClass());
        $this->assertInstanceOf(MappingDestination::class, $this->mapping->getDestination());
    }
}
