<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Storage\TableDescription;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use PHPUnit\Framework\TestCase;

class TableDescriptionTest extends TestCase
{
    public function testCreateFromMapping(): void
    {
        $descriptions = TableDescription::createFromMapping(
            $this->createSourceMock('table desc', ['col1' => 'col1 desc']),
        );

        self::assertSame('in.c-main.table', $descriptions->getTableId());
        self::assertSame('table desc', $descriptions->getTableDescription());
        self::assertSame(['col1' => 'col1 desc'], $descriptions->getColumnDescriptions());
        self::assertFalse($descriptions->isEmpty());
    }

    public function testIsEmptyWithNothingToStore(): void
    {
        $descriptions = TableDescription::createFromMapping($this->createSourceMock(null, []));

        self::assertTrue($descriptions->isEmpty());
    }

    public function testIsEmptyWithColumnDescriptionsOnly(): void
    {
        $descriptions = TableDescription::createFromMapping(
            $this->createSourceMock(null, ['col1' => 'col1 desc']),
        );

        self::assertFalse($descriptions->isEmpty());
    }

    /**
     * @param array<string, string> $columnDescriptions
     */
    private function createSourceMock(
        ?string $tableDescription,
        array $columnDescriptions,
    ): MappingFromProcessedConfiguration {
        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->expects($this->once())
            ->method('getDestination')
            ->willReturn(new MappingDestination('in.c-main.table'));
        $source->expects($this->once())
            ->method('getTableDescription')
            ->willReturn($tableDescription);
        $source->expects($this->once())
            ->method('getColumnDescriptions')
            ->willReturn($columnDescriptions);

        return $source;
    }
}
