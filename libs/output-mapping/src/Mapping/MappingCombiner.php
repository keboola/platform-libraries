<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Mapping;

use Keboola\OutputMapping\Lister\PhysicalItem;
use Keboola\OutputMapping\Writer\FileItem;
use Keboola\OutputMapping\Writer\SourceInterface;

class MappingCombiner
{
    /**
     * @param array<SourceInterface> $dataItems
     * @param array<MappingFromRawConfiguration> $configurations
     * @return array<MappingFromRawConfigurationAndPhysicalData>
     */
    public function combineDataItemsWithConfigurations(array $dataItems, array $configurations): array {
        $combinedSources = [];
        foreach ($dataItems as $dataItem) {
            $configurationMappings = array_filter($configurations, function (MappingFromRawConfiguration $mapping) use ($dataItem) {
                return $mapping->getSourceName() === $dataItem->getName();
            });

            /** @var MappingFromRawConfiguration|null $configurationMapping */
            foreach ($configurationMappings ?: [null] as $configurationMapping) {
                $combinedSources[] = new MappingFromRawConfigurationAndPhysicalData(
                    $dataItem,
                    $configurationMapping,
                );
            }
        }

        return $combinedSources;
    }

    /**
     * @param MappingFromRawConfigurationAndPhysicalData[] $sources
     * @param FileItem[] $manifests
     * @return MappingFromRawConfigurationAndPhysicalDataWithManifest[]
     */
    public function combineSourcesWithManifests(array $sources, array $manifests): array {
        $combinedSources = [];
        foreach ($sources as $source) {
            $sourceKey = array_search($source->getManifestName(), array_map(fn($v) => $v->getName(), $manifests));

            $combinedSources[] = new MappingFromRawConfigurationAndPhysicalDataWithManifest(
                $source,
                $sourceKey ? $manifests[$sourceKey] : null,
            );
        }
        return $combinedSources;
    }
}
