<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Mapping;

use Keboola\OutputMapping\Mapping\MappingFromRawConfiguration;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalData;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Keboola\OutputMapping\Writer\FileItem;
use Keboola\OutputMapping\Writer\Table\Source\SourceType;
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

        $mapping = new MappingFromRawConfigurationAndPhysicalDataWithManifest($source, $manifest);

        $this->assertEquals('name', $mapping->getSourceName());
        $this->assertEquals('pathName.manifest', $mapping->getPathNameManifest());
        $this->assertInstanceOf(FileItem::class, $mapping->getManifest());
        $this->assertFalse($mapping->isSliced());
        $this->assertEquals('pathName', $mapping->getPathName());
        $this->assertEquals('path', $mapping->getPath());
        $this->assertEquals(SourceType::FILE, $mapping->getSourceType());
    }
}
