<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Mapping;

use Keboola\OutputMapping\Mapping\MappingFromRawConfiguration;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalData;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Keboola\OutputMapping\Writer\FileItem;
use PHPUnit\Framework\TestCase;

class MappingFromRawConfigurationAndPhysicalDataWithManifestTest extends TestCase
{
    public function testBasicConfiguration(): void
    {
        $dataItem = new FileItem('pathName', 'path', 'name', false);
        $mappingItem = new MappingFromRawConfiguration([
            'source' => 'source',
            'columns' => ['column1', 'column2'],
            'delimiter' => ',',
            'enclosure' => '"',
        ]);
        $source = new MappingFromRawConfigurationAndPhysicalData($dataItem, $mappingItem);
        $manifest = new FileItem('manifestPathName', 'manifestPath', 'manifestName', false);

        $this->mapping = new MappingFromRawConfigurationAndPhysicalDataWithManifest($source, $manifest);

        $this->assertEquals('name', $this->mapping->getSourceName());
        $this->assertEquals('pathname.manifest', $this->mapping->getPathNameManifest());
        $this->assertInstanceOf(FileItem::class, $this->mapping->getManifest());
        $this->assertFalse($this->mapping->isSliced());
        $this->assertEquals('pathName', $this->mapping->getPathName());
        $this->assertEquals('path', $this->mapping->getPath());
        $this->assertEquals(FileItem::class, $this->mapping->getItemSourceClass());
    }
}
