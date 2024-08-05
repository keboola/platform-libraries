<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests;

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
            ->expects($this->exactly(2))
            ->method('hasFeature')
            ->willReturnMap([
                [OutputMappingSettings::OUTPUT_MAPPING_SLICE_FEATURE, true],
                [OutputMappingSettings::TAG_STAGING_FILES_FEATURE, false],
                [OutputMappingSettings::NATIVE_TYPES_FEATURE, true],
                [OutputMappingSettings::NEW_NATIVE_TYPES_FEATURE, true],
                [OutputMappingSettings::OUTPUT_MAPPING_CONNECTION_WEBALIZE, true],
            ]);

        $outputMappingSettings = new OutputMappingSettings(
            $configuration,
            $sourcePathPrefix,
            $storageApiToken,
            false,
            'authoritative',
        );

        $this->assertTrue($outputMappingSettings->hasSlicingFeature());
        $this->assertFalse($outputMappingSettings->hasTagStagingFilesFeature());
        $this->assertTrue($outputMappingSettings->hasNativeTypesFeature());
        $this->assertTrue($outputMappingSettings->hasNewNativeTypesFeature());
        $this->assertTrue($outputMappingSettings->hasConnectionWebalizeFeature());

        $this->assertFalse($outputMappingSettings->isFailedJob());

        $this->assertEquals($sourcePathPrefix, $outputMappingSettings->getSourcePathPrefix());
        $this->assertEquals($configuration['bucket'], $outputMappingSettings->getDefaultBucket());
        $this->assertEquals('authoritative', $outputMappingSettings->getDataTypeSupport());

        foreach ($outputMappingSettings->getMapping() as $item) {
            $this->assertInstanceOf(MappingFromRawConfiguration::class, $item);
        }
    }
}
