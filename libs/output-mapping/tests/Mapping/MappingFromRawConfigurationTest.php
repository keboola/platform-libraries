<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Mapping;

use Keboola\OutputMapping\Mapping\MappingFromRawConfiguration;
use PHPUnit\Framework\TestCase;

class MappingFromRawConfigurationTest extends TestCase
{
    private MappingFromRawConfiguration $mapping;

    protected function setUp(): void
    {
        $mappingItem = [
            'source' => 'source',
            'columns' => ['column1', 'column2'],
            'delimiter' => ',',
            'enclosure' => '"',
        ];

        $this->mapping = new MappingFromRawConfiguration($mappingItem);
    }

    public function testGetSourceName(): void
    {
        $this->assertEquals('source', $this->mapping->getSourceName());
    }

    public function testAsArray(): void
    {
        $expectedArray = [
            'source' => 'source',
            'columns' => ['column1', 'column2'],
            'delimiter' => ',',
            'enclosure' => '"',
        ];
        $this->assertEquals($expectedArray, $this->mapping->asArray());
    }

    public function testGetDelimiter(): void
    {
        $this->assertEquals(',', $this->mapping->getDelimiter());
    }

    public function testGetEnclosure(): void
    {
        $this->assertEquals('"', $this->mapping->getEnclosure());
    }

    public function testGetColumns(): void
    {
        $this->assertEquals(['column1', 'column2'], $this->mapping->getColumns());
    }

    public function testSourceCanBeNull(): void
    {
        $mappingItem = [
            'columns' => ['column1', 'column2'],
            'delimiter' => ',',
            'enclosure' => '"',
        ];

        $mapping = new MappingFromRawConfiguration($mappingItem);
        $this->assertEquals('', $mapping->getSourceName());
    }

    public function testSourceCanBeNullWithUnloadStrategy(): void
    {
        $mappingItem = [
            'unload_strategy' => 'direct-grant',
            'destination' => 'in.c-main.test',
            'columns' => ['column1', 'column2'],
        ];

        $mapping = new MappingFromRawConfiguration($mappingItem);
        $this->assertEquals('', $mapping->getSourceName());
        $this->assertEquals(['column1', 'column2'], $mapping->getColumns());
    }
}
