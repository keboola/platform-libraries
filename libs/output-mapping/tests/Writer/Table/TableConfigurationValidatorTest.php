<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Keboola\OutputMapping\Writer\Table\Strategy\LocalTableStrategy;
use Keboola\OutputMapping\Writer\Table\Strategy\SqlWorkspaceTableStrategy;
use Keboola\OutputMapping\Writer\Table\TableConfigurationValidator;
use PHPUnit\Framework\TestCase;

class TableConfigurationValidatorTest extends TestCase
{
    public function testValidConfig(): void
    {
        $strategy = $this->createMock(LocalTableStrategy::class);
        $source = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $validator = new TableConfigurationValidator();

        $validator->validate($strategy, $source, [
            'columns' => [],
            'column_metadata' => [],
            'destination' => 'in.c-main.table',
        ]);

        self::assertTrue(true);
    }

    public function testInvalidDestination(): void
    {
        $strategy = $this->createMock(LocalTableStrategy::class);
        $source = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $validator = new TableConfigurationValidator();

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Failed to resolve valid destination. "table" is not a valid table ID.');
        $validator->validate($strategy, $source, [
            'columns' => [],
            'column_metadata' => [],
            'destination' => 'table',
        ]);
    }

    public function testMissingColumns(): void
    {
        $strategy = $this->createMock(LocalTableStrategy::class);
        $source = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $source->method('isSliced')->willReturn(true);
        $source->method('getSourceName')->willReturn('source');

        $validator = new TableConfigurationValidator();

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Sliced file "source" columns specification missing.');
        $validator->validate($strategy, $source, [
            'columns' => [],
            'column_metadata' => [],
            'destination' => 'in.c-main.table',
        ]);
    }

    public function testErrorSystemColumns(): void
    {
        $strategy = $this->createMock(LocalTableStrategy::class);
        $source = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $validator = new TableConfigurationValidator();

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage(
            'Failed to process mapping for table : System columns "_timestamp" cannot be imported to the table.',
        );
        $validator->validate($strategy, $source, [
            'columns' => ['_timestamp'],
            'column_metadata' => [],
            'destination' => 'in.c-main.table',
        ]);
    }

    public function testSystemColumnsOnSqlWorkspaceStrategy(): void
    {
        $strategy = $this->createMock(SqlWorkspaceTableStrategy::class);
        $source = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $validator = new TableConfigurationValidator();

        $validator->validate($strategy, $source, [
            'columns' => ['_timestamp'],
            'column_metadata' => [],
            'destination' => 'in.c-main.table',
        ]);

        self::assertTrue(true);
    }
}
