<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests;

use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\SystemMetadata;
use PHPUnit\Framework\TestCase;

class SystemMetadataTest extends TestCase
{
    private SystemMetadata $systemMetadata;

    public function setUp(): void
    {
        $this->systemMetadata = new SystemMetadata([
            'componentId' => 'keboola.output-mapping',
            'configurationId' => '123',
            'configurationRowId' => '456',
            'branchId' => '789',
            'runId' => '101112',
            'stack' => 'us-east',
            'provider' => 'aws',
        ]);

        parent::setUp();
    }

    public function testMissingComponentIdMetadata(): void
    {
        $this->expectException(OutputOperationException::class);
        $this->expectExceptionMessage('Component Id must be set');
        new SystemMetadata([]);
    }

    public function testAsArray(): void
    {
        self::assertEquals([
            'componentId' => 'keboola.output-mapping',
            'configurationId' => '123',
            'configurationRowId' => '456',
            'branchId' => '789',
            'runId' => '101112',
            'stack' => 'us-east',
            'provider' => 'aws',
        ], $this->systemMetadata->asArray());
    }

    public function testGetSystemTags(): void
    {
        self::assertEquals([
            'componentId' => 'keboola.output-mapping',
            'configurationId' => '123',
            'configurationRowId' => '456',
            'branchId' => '789',
            'runId' => '101112',
        ], $this->systemMetadata->getSystemTags());
    }

    public function testGetSystemMetadata(): void
    {
        self::assertEquals('keboola.output-mapping', $this->systemMetadata->getSystemMetadata('componentId'));
        self::assertNull($this->systemMetadata->getSystemMetadata('unexist'));
    }

    public function testGetCreatedMetadata(): void
    {
        self::assertEquals([
            [
                'key' => 'KBC.createdBy.component.id',
                'value' => 'keboola.output-mapping',
            ],
            [
                'key' => 'KBC.createdBy.configuration.id',
                'value' => '123',
            ],
            [
                'key' => 'KBC.createdBy.configurationRow.id',
                'value' => '456',
            ],
            [
                'key' => 'KBC.createdBy.branch.id',
                'value' => '789',
            ],
        ], $this->systemMetadata->getCreatedMetadata());
    }

    public function testGetUpdatedMetadata(): void
    {
        self::assertEquals([
            [
                'key' => 'KBC.lastUpdatedBy.component.id',
                'value' => 'keboola.output-mapping',
            ],
            [
                'key' => 'KBC.lastUpdatedBy.configuration.id',
                'value' => '123',
            ],
            [
                'key' => 'KBC.lastUpdatedBy.configurationRow.id',
                'value' => '456',
            ],
            [
                'key' => 'KBC.lastUpdatedBy.branch.id',
                'value' => '789',
            ],
        ], $this->systemMetadata->getUpdatedMetadata());
    }
}
