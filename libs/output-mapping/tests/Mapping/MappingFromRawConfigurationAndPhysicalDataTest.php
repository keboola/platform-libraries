<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Mapping;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromRawConfiguration;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalData;
use Keboola\OutputMapping\Writer\FileItem;
use Keboola\OutputMapping\Writer\Table\Source\WorkspaceItemSource;
use PHPUnit\Framework\TestCase;

class MappingFromRawConfigurationAndPhysicalDataTest extends TestCase
{
    public function testFileItem(): void
    {
        $dataItem = new FileItem(
            '/tmp/path/mockfile.csv',
            '/tmp/path/',
            'mockfile.csv',
            false,
        );
        $mappingItem = new MappingFromRawConfiguration([
            'source' => 'source',
            'columns' => ['column1', 'column2'],
            'delimiter' => ',',
            'enclosure' => '"',
        ]);

        $mapping = new MappingFromRawConfigurationAndPhysicalData($dataItem, $mappingItem);

        $this->assertEquals('mockfile.csv', $mapping->getSourceName());
        $this->assertEquals('mockfile.csv.manifest', $mapping->getManifestName());
        $this->assertFalse($mapping->isSliced());
        $this->assertEquals('/tmp/path/mockfile.csv', $mapping->getPathName());
        $this->assertEquals('/tmp/path/', $mapping->getPath());
        $this->assertEquals('Keboola\OutputMapping\Writer\FileItem', $mapping->getItemSourceClass());

        try {
            $mapping->getWorkspaceId();
            $this->fail('Should fail');
        } catch (InvalidOutputException $e) {
            $this->assertEquals('WorkspaceId is available only for WorkspaceItemSource', $e->getMessage());
        }

        try {
            $mapping->getDataObject();
            $this->fail('Should fail');
        } catch (InvalidOutputException $e) {
            $this->assertEquals('DataObject is available only for WorkspaceItemSource', $e->getMessage());
        }
    }

    public function testWorkspaceItemSource(): void
    {
        $dataItem = new WorkspaceItemSource(
            'mockfile.csv',
            '1234567890',
            'TestDataObject',
            false,
        );
        $mappingItem = new MappingFromRawConfiguration([
            'source' => 'source',
            'columns' => ['column1', 'column2'],
            'delimiter' => ',',
            'enclosure' => '"',
        ]);

        $mapping = new MappingFromRawConfigurationAndPhysicalData($dataItem, $mappingItem);

        $this->assertEquals('mockfile.csv', $mapping->getSourceName());
        $this->assertEquals('mockfile.csv.manifest', $mapping->getManifestName());
        $this->assertFalse($mapping->isSliced());
        $this->assertEquals(
            'Keboola\OutputMapping\Writer\Table\Source\WorkspaceItemSource',
            $mapping->getItemSourceClass(),
        );
        $this->assertEquals('1234567890', $mapping->getWorkspaceId());
        $this->assertEquals('TestDataObject', $mapping->getDataObject());

        try {
            $mapping->getPath();
            $this->fail('Should fail');
        } catch (InvalidOutputException $e) {
            $this->assertEquals('Path is available only for FileItem', $e->getMessage());
        }

        try {
            $mapping->getPathName();
            $this->fail('Should fail');
        } catch (InvalidOutputException $e) {
            $this->assertEquals('PathName is available only for FileItem', $e->getMessage());
        }
    }
}
