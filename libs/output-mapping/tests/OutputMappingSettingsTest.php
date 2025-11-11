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
            ->expects(self::exactly(7))
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

        $rawConfig = $outputMappingSettings->getRawConfiguration();
        self::assertEquals($configuration, $rawConfig);

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
     * @dataProvider hasBothNativeTypesFeaturesDataProvider
     */
    public function testBothNativeTypesFeatures(
        bool $hasNativeTypesFeature,
        bool $hasNewNativeTypesFeature,
        bool $expectedHasNativeTypesSettingsGetter,
        bool $expectedHasNewNativeTypesSettingsGetter,
    ): void {

        $storageApiToken = $this->createMock(StorageApiToken::class);
        $storageApiToken
            ->method('hasFeature')
            ->willReturnCallback(
                function (string $feature) use ($hasNativeTypesFeature, $hasNewNativeTypesFeature) {
                    if ($feature === OutputMappingSettings::NATIVE_TYPES_FEATURE) {
                        return $hasNativeTypesFeature;
                    }
                    if ($feature === OutputMappingSettings::NEW_NATIVE_TYPES_FEATURE) {
                        return $hasNewNativeTypesFeature;
                    }
                    return false;
                },
            );

        $outputMappingSettings = new OutputMappingSettings(
            [],
            'path/to/source',
            $storageApiToken,
            false,
            'authoritative',
        );

        self::assertSame($expectedHasNativeTypesSettingsGetter, $outputMappingSettings->hasNativeTypesFeature());
        self::assertSame($expectedHasNewNativeTypesSettingsGetter, $outputMappingSettings->hasNewNativeTypesFeature());
    }

    /**
     * @dataProvider hasFeatureDataProvider
     */
    public function testHasFeature(string $featureName, string $hasSpecificFeatureMethodName): void
    {
        $storageApiToken = $this->createMock(StorageApiToken::class);
        $storageApiToken
            ->method('hasFeature')
            ->willReturnCallback(
                function (string $feature) use ($featureName) {
                    return $feature === $featureName;
                },
            );

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

    public function hasBothNativeTypesFeaturesDataProvider(): Generator
    {
        yield 'both-native-types-features' => [
            true, // hasNativeTypesFeature
            true, // hasNewNativeTypesFeature
            false, // expectedHasNativeTypesSettingsGetter
            true, // expectedHasNewNativeTypesSettingsGetter
        ];

        yield 'no-native-types-features' => [
            false, // hasNativeTypesFeature
            true, // hasNewNativeTypesFeature
            false, // expectedHasNativeTypesSettingsGetter
            true, // expectedHasNewNativeTypesSettingsGetter
        ];

        yield 'no-new-native-types-features' => [
            true, // hasNativeTypesFeature
            false, // hasNewNativeTypesFeature
            true, // expectedHasNativeTypesSettingsGetter
            false, // expectedHasNewNativeTypesSettingsGetter
        ];

        yield 'no-both-native-types-features' => [
            false, // hasNativeTypesFeature
            false, // hasNewNativeTypesFeature
            false, // expectedHasNativeTypesSettingsGetter
            false, // expectedHasNewNativeTypesSettingsGetter
        ];
    }
}
