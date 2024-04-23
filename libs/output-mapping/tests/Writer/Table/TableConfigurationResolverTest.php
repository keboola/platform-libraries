<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Keboola\OutputMapping\OutputMappingSettings;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Writer\Table\TableConfigurationResolver;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class TableConfigurationResolverTest extends TestCase
{
    private TestHandler $testHandler;

    private LoggerInterface $logger;

    protected function setUp(): void
    {
        parent::setUp();

        $this->testHandler = new TestHandler();
        $this->logger = new Logger('test-configuration-resolver');
        $this->logger->pushHandler($this->testHandler);
    }

    public function testMergeConfiguration(): void
    {
        $configuration = $this->createMock(OutputMappingSettings::class);
        $configuration->method('getDefaultBucket')->willReturn('in.c-main');
        $configuration->method('hasTagStagingFilesFeature')->willReturn(false);

        $source = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $source->method('getSourceName')->willReturn('table1');

        $systemMetadata = new SystemMetadata([
            'componentId' => 'keboola.ex-db-snowflake',
            'configurationId' => '123',
            'configurationRowId' => '456',
        ]);

        $mappingFromManifest = [
            'destination' => 'in.c-main.table1',
            'source' => 'in.c-main.table1',
        ];

        $mappingFromConfiguration = [
            'destination' => 'in.c-main.replaceDestination',
            'source' => 'in.c-main.replaceSource',
            'delimiter' => 'NEWDELIMITER',
        ];

        $resolver = new TableConfigurationResolver($this->logger);
        $config = $resolver->resolveTableConfiguration(
            $configuration,
            $source,
            $mappingFromManifest,
            $mappingFromConfiguration,
            $systemMetadata,
        );

        $expectedConfig = [
            'destination' => 'in.c-main.replaceDestination',
            'source' => 'in.c-main.replaceSource',
            'incremental' => false,
            'primary_key' => [],
            'columns' => [],
            'distribution_key' => [],
            'delete_where_values' => [],
            'delete_where_operator' => 'eq',
            'delimiter' => 'NEWDELIMITER',
            'enclosure' => '"',
            'metadata' => [],
            'column_metadata' => [],
            'write_always' => false,
            'tags' => [],
        ];

        self::assertEquals($expectedConfig, $config);
    }

    public function testConfigurationNormalizePK(): void
    {
        $configuration = $this->createMock(OutputMappingSettings::class);
        $configuration->method('getDefaultBucket')->willReturn('in.c-main');
        $configuration->method('hasTagStagingFilesFeature')->willReturn(false);

        $source = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $source->method('getSourceName')->willReturn('table1');

        $systemMetadata = new SystemMetadata([
            'componentId' => 'keboola.ex-db-snowflake',
            'configurationId' => '123',
            'configurationRowId' => '456',
        ]);

        $mappingFromManifest = [
            'destination' => 'in.c-main.table1',
            'source' => 'in.c-main.table1',
        ];

        $mappingFromConfiguration = [
            'primary_key' => [
                'id',
                ' keywithspace ',
                '',
            ],
        ];

        $resolver = new TableConfigurationResolver($this->logger);
        $config = $resolver->resolveTableConfiguration(
            $configuration,
            $source,
            $mappingFromManifest,
            $mappingFromConfiguration,
            $systemMetadata,
        );

        $expectedConfig = [
            'destination' => 'in.c-main.table1',
            'source' => 'in.c-main.table1',
            'incremental' => false,
            'primary_key' => [
                'id',
                'keywithspace',
            ],
            'columns' => [],
            'distribution_key' => [],
            'delete_where_values' => [],
            'delete_where_operator' => 'eq',
            'delimiter' => ',',
            'enclosure' => '"',
            'metadata' => [],
            'column_metadata' => [],
            'write_always' => false,
            'tags' => [],
        ];

        self::assertEquals($expectedConfig, $config);
        self::assertTrue($this->testHandler->hasWarning('Found empty column name in key array.'));
    }

    public function testConfigurationNormalizeDistributionKey(): void
    {
        $configuration = $this->createMock(OutputMappingSettings::class);
        $configuration->method('getDefaultBucket')->willReturn('in.c-main');
        $configuration->method('hasTagStagingFilesFeature')->willReturn(false);

        $source = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $source->method('getSourceName')->willReturn('table1');

        $systemMetadata = new SystemMetadata([
            'componentId' => 'keboola.ex-db-snowflake',
            'configurationId' => '123',
            'configurationRowId' => '456',
        ]);

        $mappingFromManifest = [
            'destination' => 'in.c-main.table1',
            'source' => 'in.c-main.table1',
        ];

        $mappingFromConfiguration = [
            'distribution_key' => [
                'id',
                ' dkwithspace ',
                '',
            ],
        ];

        $resolver = new TableConfigurationResolver($this->logger);
        $config = $resolver->resolveTableConfiguration(
            $configuration,
            $source,
            $mappingFromManifest,
            $mappingFromConfiguration,
            $systemMetadata,
        );

        $expectedConfig = [
            'destination' => 'in.c-main.table1',
            'source' => 'in.c-main.table1',
            'incremental' => false,
            'primary_key' => [],
            'columns' => [],
            'distribution_key' => [
                'id',
                'dkwithspace',
            ],
            'delete_where_values' => [],
            'delete_where_operator' => 'eq',
            'delimiter' => ',',
            'enclosure' => '"',
            'metadata' => [],
            'column_metadata' => [],
            'write_always' => false,
            'tags' => [],
        ];

        self::assertEquals($expectedConfig, $config);
        self::assertTrue($this->testHandler->hasWarning('Found empty column name in key array.'));
    }

    public function testConfigurationWithTagStagingFilesFeature(): void
    {
        $configuration = $this->createMock(OutputMappingSettings::class);
        $configuration->method('getDefaultBucket')->willReturn('in.c-main');
        $configuration->method('hasTagStagingFilesFeature')->willReturn(true);

        $source = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $source->method('getSourceName')->willReturn('table1');

        $systemMetadata = new SystemMetadata([
            'componentId' => 'keboola.ex-db-snowflake',
            'configurationId' => '123',
            'configurationRowId' => '456',
        ]);

        $mappingFromManifest = [
            'destination' => 'in.c-main.table1',
            'source' => 'in.c-main.table1',
        ];

        $mappingFromConfiguration = [];

        $resolver = new TableConfigurationResolver($this->logger);
        $config = $resolver->resolveTableConfiguration(
            $configuration,
            $source,
            $mappingFromManifest,
            $mappingFromConfiguration,
            $systemMetadata,
        );

        $expectedConfig = [
            'destination' => 'in.c-main.table1',
            'source' => 'in.c-main.table1',
            'incremental' => false,
            'primary_key' => [],
            'columns' => [],
            'distribution_key' => [],
            'delete_where_values' => [],
            'delete_where_operator' => 'eq',
            'delimiter' => ',',
            'enclosure' => '"',
            'metadata' => [],
            'column_metadata' => [],
            'write_always' => false,
            'tags' => [
                'componentId: keboola.ex-db-snowflake',
                'configurationId: 123',
                'configurationRowId: 456',
            ],
        ];

        self::assertEquals($expectedConfig, $config);
    }

    public function testDestinationInManifestWithoutBucket(): void
    {
        $configuration = $this->createMock(OutputMappingSettings::class);
        $configuration->method('getDefaultBucket')->willReturn('in.c-main');
        $configuration->method('hasTagStagingFilesFeature')->willReturn(false);

        $source = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $source->method('getSourceName')->willReturn('table1');

        $systemMetadata = new SystemMetadata([
            'componentId' => 'keboola.ex-db-snowflake',
        ]);

        $mappingFromManifest = [
            'destination' => 'table1',
            'source' => 'in.c-main.table1',
        ];

        $mappingFromConfiguration = [];

        $resolver = new TableConfigurationResolver($this->logger);

        $config = $resolver->resolveTableConfiguration(
            $configuration,
            $source,
            $mappingFromManifest,
            $mappingFromConfiguration,
            $systemMetadata,
        );

        self::assertEquals('in.c-main.table1', $config['destination']);
    }

    public function testWarningDestinationFromFileName(): void
    {
        $configuration = $this->createMock(OutputMappingSettings::class);
        $configuration->method('getDefaultBucket')->willReturn(null);
        $configuration->method('hasTagStagingFilesFeature')->willReturn(false);

        $source = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $source->method('getSourceName')->willReturn('in.c-main.table1.csv');

        $systemMetadata = new SystemMetadata([
            'componentId' => 'keboola.ex-db-snowflake',
        ]);

        $mappingFromManifest = [];

        $mappingFromConfiguration = [];

        $resolver = new TableConfigurationResolver($this->logger);

        $config = $resolver->resolveTableConfiguration(
            $configuration,
            $source,
            $mappingFromManifest,
            $mappingFromConfiguration,
            $systemMetadata,
        );

        self::assertEquals('in.c-main.table1', $config['destination']);

        $warningMessage = 'Source table "in.c-main.table1.csv" has neither manifest file nor mapping set, ';
        $warningMessage .= 'falling back to the source name as a destination.This behaviour was DEPRECATED ';
        $warningMessage .= 'and will be removed in the future.';
        self::assertTrue($this->testHandler->hasWarning($warningMessage));
    }

    public function testErrorValidationConfig(): void
    {
        $configuration = $this->createMock(OutputMappingSettings::class);
        $configuration->method('getDefaultBucket')->willReturn('in.c-main');
        $configuration->method('hasTagStagingFilesFeature')->willReturn(false);

        $source = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $source->method('getSourceName')->willReturn('table1');

        $systemMetadata = new SystemMetadata([
            'componentId' => 'keboola.ex-db-snowflake',
        ]);

        $mappingFromManifest = [
            'destination' => 'in.c-main.table1',
            'wrongParam' => 'wrong',
        ];

        $mappingFromConfiguration = [];

        $resolver = new TableConfigurationResolver($this->logger);

        $this->expectException(InvalidOutputException::class);
        $resolver->resolveTableConfiguration(
            $configuration,
            $source,
            $mappingFromManifest,
            $mappingFromConfiguration,
            $systemMetadata,
        );
    }

    public function testErrorWrongDestinationInMapping(): void
    {
        $configuration = $this->createMock(OutputMappingSettings::class);
        $configuration->method('getDefaultBucket')->willReturn('in.c-main');
        $configuration->method('hasTagStagingFilesFeature')->willReturn(false);

        $source = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $source->method('getSourceName')->willReturn('table1');

        $systemMetadata = new SystemMetadata([
            'componentId' => 'keboola.ex-db-snowflake',
        ]);

        $mappingFromManifest = [
            'destination' => 'in.c-main.table1',
        ];

        $mappingFromConfiguration = [
            'destination' => 'in.c-main.table2.wrong',
        ];

        $resolver = new TableConfigurationResolver($this->logger);

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Failed to resolve destination for output table "table1".');
        $resolver->resolveTableConfiguration(
            $configuration,
            $source,
            $mappingFromManifest,
            $mappingFromConfiguration,
            $systemMetadata,
        );
    }

    public function testErrorDestinationInManifestWithoutBucketAndWithoutDefaultBucket(): void
    {
        $configuration = $this->createMock(OutputMappingSettings::class);
        $configuration->method('getDefaultBucket')->willReturn(null);
        $configuration->method('hasTagStagingFilesFeature')->willReturn(false);

        $source = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $source->method('getSourceName')->willReturn('table1');

        $systemMetadata = new SystemMetadata([
            'componentId' => 'keboola.ex-db-snowflake',
        ]);

        $mappingFromManifest = [
            'destination' => 'table1',
        ];

        $mappingFromConfiguration = [];

        $resolver = new TableConfigurationResolver($this->logger);

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Failed to resolve destination for output table "table1".');
        $resolver->resolveTableConfiguration(
            $configuration,
            $source,
            $mappingFromManifest,
            $mappingFromConfiguration,
            $systemMetadata,
        );
    }
}
