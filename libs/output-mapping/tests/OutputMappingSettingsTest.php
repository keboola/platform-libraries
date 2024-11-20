<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests;

use Generator;
use Keboola\OutputMapping\Mapping\MappingFromRawConfiguration;
use Keboola\OutputMapping\OutputMappingSettings;
use Keboola\StorageApiBranch\StorageApiToken;
use PHPUnit\Framework\TestCase;

class OutputMappingSettingsTest extends TestCase
{
    public function testBasic(): void
    {
        $configuration = [
            'bucket' => 'testBucket',
            'mapping' => [['source' => 'source1', 'destination' => 'destination1']],
        ];
        $sourcePathPrefix = 'path/to/source';
        $storageApiToken = $this->createMock(StorageApiToken::class);
        $storageApiToken
            ->expects(self::exactly(6))
            ->method('hasFeature')
            ->willReturn(false)
        ;

        $outputMappingSettings = new OutputMappingSettings(
            $configuration,
            $sourcePathPrefix,
            $storageApiToken,
            false,
            'authoritative',
        );

        self::assertFalse($outputMappingSettings->hasSlicingFeature());
        self::assertFalse($outputMappingSettings->hasTagStagingFilesFeature());
        self::assertFalse($outputMappingSettings->hasNativeTypesFeature());
        self::assertFalse($outputMappingSettings->hasNewNativeTypesFeature());
        self::assertFalse($outputMappingSettings->hasConnectionWebalizeFeature());
        self::assertFalse($outputMappingSettings->hasBigqueryNativeTypesFeature());

        self::assertFalse($outputMappingSettings->isFailedJob());

        self::assertEquals($sourcePathPrefix, $outputMappingSettings->getSourcePathPrefix());
        self::assertEquals($configuration['bucket'], $outputMappingSettings->getDefaultBucket());
        self::assertEquals('authoritative', $outputMappingSettings->getDataTypeSupport());

        foreach ($outputMappingSettings->getMapping() as $item) {
            self::assertInstanceOf(MappingFromRawConfiguration::class, $item);
        }

        self::assertNull($outputMappingSettings->getTreatValuesAsNull());
    }

    public function hasFeatureDataProvider(): Generator
    {
        yield 'output-mapping-slice' => [
            OutputMappingSettings::OUTPUT_MAPPING_SLICE_FEATURE,
            'hasSlicingFeature',
        ];
        yield 'tag-staging-files' => [
            OutputMappingSettings::TAG_STAGING_FILES_FEATURE,
            'hasTagStagingFilesFeature',
        ];
        yield 'native-types' => [
            OutputMappingSettings::NATIVE_TYPES_FEATURE,
            'hasNativeTypesFeature',
        ];
        yield 'new-native-types' => [
            OutputMappingSettings::NEW_NATIVE_TYPES_FEATURE,
            'hasNewNativeTypesFeature',
        ];
        yield 'output-mapping-connection-webalize' => [
            OutputMappingSettings::OUTPUT_MAPPING_CONNECTION_WEBALIZE,
            'hasConnectionWebalizeFeature',
        ];
        yield 'bigquery-native-types' => [
            OutputMappingSettings::BIG_QUERY_NATIVE_TYPES_FEATURE,
            'hasBigqueryNativeTypesFeature',
        ];
    }

    /**
     * @dataProvider hasFeatureDataProvider
     */
    public function testHasFeature(string $featureName, string $hasSpecificFeatureMethodName): void
    {
        $storageApiToken = $this->createMock(StorageApiToken::class);
        $storageApiToken
            ->expects(self::once())
            ->method('hasFeature')
            ->with($featureName)
            ->willReturn(true)
        ;

        $outputMappingSettings = new OutputMappingSettings(
            [],
            'path/to/source',
            $storageApiToken,
            false,
            'authoritative',
        );

        self::assertTrue($outputMappingSettings->$hasSpecificFeatureMethodName());
    }

    public function treatValuesAsNullDataProvider(): Generator
    {
        yield 'missing treat config' => [
            'configuration' => [],
            'expectedTreatValuesAsNullValue' => null,
        ];
        yield 'multiple values' => [
            'configuration' => [
                'treat_values_as_null' => ['value1', 'value2'],
            ],
            'expectedTreatValuesAsNullValue' => ['value1', 'value2'],
        ];
        yield 'null value' => [
            'configuration' => [
                'treat_values_as_null' => null,
            ],
            'expectedTreatValuesAsNullValue' => null,
        ];
        yield 'empty array' => [
            'configuration' => [
                'treat_values_as_null' => [],
            ],
            'expectedTreatValuesAsNullValue' => [],
        ];
        yield 'empty string' => [
            'configuration' => [
                'treat_values_as_null' => [''],
            ],
            'expectedTreatValuesAsNullValue' => [''],
        ];
    }

    /**
     * @dataProvider treatValuesAsNullDataProvider
     */
    public function testTreatValuesAsNull(array $configuration, ?array $expectedTreatValuesAsNullValue): void
    {
        $outputMappingSettings = new OutputMappingSettings(
            $configuration,
            'path/to/source',
            $this->createMock(StorageApiToken::class),
            false,
            'authoritative',
        );

        self::assertSame($expectedTreatValuesAsNullValue, $outputMappingSettings->getTreatValuesAsNull());
    }
}
