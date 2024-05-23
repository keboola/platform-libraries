<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Keboola\OutputMapping\OutputMappingSettings;
use Keboola\OutputMapping\Writer\Table\Strategy\LocalTableStrategy;
use Keboola\OutputMapping\Writer\Table\Strategy\SqlWorkspaceTableStrategy;
use Keboola\OutputMapping\Writer\Table\TableConfigurationValidator;
use Keboola\StorageApiBranch\StorageApiToken;
use PHPUnit\Framework\TestCase;

class TableConfigurationValidatorTest extends TestCase
{
    public function testValidConfig(): void
    {
        $strategy = $this->createMock(LocalTableStrategy::class);
        $source = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $validator = new TableConfigurationValidator(
            $strategy,
            new OutputMappingSettings(
                [],
                '',
                $this->createMock(StorageApiToken::class),
                false,
                'none',
            ),
        );

        $validator->validate($source, [
            'columns' => [],
            'column_metadata' => [],
            'schema' => [],
            'destination' => 'in.c-main.table',
        ]);

        self::assertTrue(true);
    }

    public function testValidSchemaHintsConfig(): void
    {
        $strategy = $this->createMock(LocalTableStrategy::class);
        $source = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $storageApiTokenMock = $this->createMock(StorageApiToken::class);
        $storageApiTokenMock
            ->expects($this->once())
            ->method('hasFeature')
            ->willReturnMap([
                [OutputMappingSettings::NEW_NATIVE_TYPES_FEATURE, true],
            ]);

        $validator = new TableConfigurationValidator(
            $strategy,
            new OutputMappingSettings(
                [],
                '',
                $storageApiTokenMock,
                false,
                'hints',
            ),
        );

        $validator->validate($source, [
            'columns' => [],
            'column_metadata' => [],
            'schema' => [
                [
                    'name' =>'col1',
                ],
            ],
            'destination' => 'in.c-main.table',
        ]);

        self::assertTrue(true);
    }

    public function testValidSchemaAuthoritativeConfig(): void
    {
        $strategy = $this->createMock(LocalTableStrategy::class);
        $source = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $storageApiTokenMock = $this->createMock(StorageApiToken::class);
        $storageApiTokenMock
            ->expects($this->once())
            ->method('hasFeature')
            ->willReturnMap([
                [OutputMappingSettings::NEW_NATIVE_TYPES_FEATURE, true],
            ]);

        $validator = new TableConfigurationValidator(
            $strategy,
            new OutputMappingSettings(
                [],
                '',
                $storageApiTokenMock,
                false,
                'authoritative',
            ),
        );

        $validator->validate($source, [
            'columns' => [],
            'column_metadata' => [],
            'schema' => [
                [
                    'name' =>'col1',
                    'data_type' => [
                        'base' => [
                            'type' => 'STRING',
                        ],
                    ],
                ],
            ],
            'destination' => 'in.c-main.table',
        ]);

        self::assertTrue(true);
    }

    public function testErrorSchemaHintsMissingSchema(): void
    {
        $strategy = $this->createMock(LocalTableStrategy::class);
        $source = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $storageApiTokenMock = $this->createMock(StorageApiToken::class);
        $storageApiTokenMock
            ->expects($this->once())
            ->method('hasFeature')
            ->willReturnMap([
                [OutputMappingSettings::NEW_NATIVE_TYPES_FEATURE, true],
            ]);

        $validator = new TableConfigurationValidator(
            $strategy,
            new OutputMappingSettings(
                [],
                '',
                $storageApiTokenMock,
                false,
                'hints',
            ),
        );

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Configuration schema is missing.');
        $validator->validate($source, [
            'columns' => [],
            'column_metadata' => [],
            'schema' => [],
            'destination' => 'in.c-main.table',
        ]);
    }

    public function testErrorSchemaAuthoritativeMissingDataType(): void
    {
        $strategy = $this->createMock(LocalTableStrategy::class);
        $source = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $storageApiTokenMock = $this->createMock(StorageApiToken::class);
        $storageApiTokenMock
            ->expects($this->once())
            ->method('hasFeature')
            ->willReturnMap([
                [OutputMappingSettings::NEW_NATIVE_TYPES_FEATURE, true],
            ]);

        $validator = new TableConfigurationValidator(
            $strategy,
            new OutputMappingSettings(
                [],
                '',
                $storageApiTokenMock,
                false,
                'authoritative',
            ),
        );

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Missing data type for columns: col1, col2');
        $validator->validate($source, [
            'columns' => [],
            'column_metadata' => [],
            'schema' => [
                [
                    'name' =>'col1',
                ],
                [
                    'name' =>'col2',
                ],
            ],
            'destination' => 'in.c-main.table',
        ]);
    }

    public function testInvalidDestination(): void
    {
        $strategy = $this->createMock(LocalTableStrategy::class);
        $source = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $validator = new TableConfigurationValidator(
            $strategy,
            new OutputMappingSettings(
                [],
                '',
                $this->createMock(StorageApiToken::class),
                false,
                'none',
            ),
        );

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Failed to resolve valid destination. "table" is not a valid table ID.');
        $validator->validate($source, [
            'columns' => [],
            'column_metadata' => [],
            'schema' => [],
            'destination' => 'table',
        ]);
    }

    public function testMissingColumns(): void
    {
        $strategy = $this->createMock(LocalTableStrategy::class);
        $source = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $source->method('isSliced')->willReturn(true);
        $source->method('getSourceName')->willReturn('source');

        $validator = new TableConfigurationValidator(
            $strategy,
            new OutputMappingSettings(
                [],
                '',
                $this->createMock(StorageApiToken::class),
                false,
                'none',
            ),
        );

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Sliced file "source" columns specification missing.');
        $validator->validate($source, [
            'columns' => [],
            'column_metadata' => [],
            'schema' => [],
            'destination' => 'in.c-main.table',
        ]);
    }

    public function testErrorSystemColumns(): void
    {
        $strategy = $this->createMock(LocalTableStrategy::class);
        $source = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $validator = new TableConfigurationValidator(
            $strategy,
            new OutputMappingSettings(
                [],
                '',
                $this->createMock(StorageApiToken::class),
                false,
                'none',
            ),
        );

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage(
            'Failed to process mapping for table : System columns "_timestamp" cannot be imported to the table.',
        );
        $validator->validate($source, [
            'columns' => ['_timestamp'],
            'column_metadata' => [],
            'schema' => [],
            'destination' => 'in.c-main.table',
        ]);
    }

    public function testSystemColumnsOnSqlWorkspaceStrategy(): void
    {
        $strategy = $this->createMock(SqlWorkspaceTableStrategy::class);
        $source = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $validator = new TableConfigurationValidator(
            $strategy,
            new OutputMappingSettings(
                [],
                '',
                $this->createMock(StorageApiToken::class),
                false,
                'none',
            ),
        );

        $validator->validate($source, [
            'columns' => ['_timestamp'],
            'column_metadata' => [],
            'schema' => [],
            'destination' => 'in.c-main.table',
        ]);

        self::assertTrue(true);
    }
}
