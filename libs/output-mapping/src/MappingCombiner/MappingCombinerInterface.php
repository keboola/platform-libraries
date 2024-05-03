<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\MappingCombiner;

use Keboola\OutputMapping\Mapping\MappingFromRawConfiguration;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalData;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Keboola\OutputMapping\Writer\FileItem;
use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;

interface MappingCombinerInterface
{
    /**
     * @param array<SourceInterface> $dataItems
     * @param array<MappingFromRawConfiguration> $configurations
     * @return array<MappingFromRawConfigurationAndPhysicalData>
     */
    public function combineDataItemsWithConfigurations(array $dataItems, array $configurations): array;

    /**
     * @param MappingFromRawConfigurationAndPhysicalData[] $sources
     * @param FileItem[] $manifests
     * @return MappingFromRawConfigurationAndPhysicalDataWithManifest[]
     */
    public function combineSourcesWithManifests(array $sources, array $manifests): array;
}
