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

            if (count($configurationMappings) === 0) {
                $configurationMappings[] = null;
            }

            /** @var MappingFromRawConfiguration|null $configurationMapping */
            foreach ($configurationMappings as $configurationMapping) {
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
            $manifestsFiltered = array_filter($manifests, function (FileItem $manifest) use ($source) {
                return $manifest->getName() === $source->getSourceName() . '.manifest';
            });

            if (count($manifestsFiltered) === 0) {
                $manifestsFiltered[] = null;
            }
            if (count($manifestsFiltered) > 1) {
                // TODO: proper exception (LOGIC ? ), should not happen
                throw new \Exception('Multiple manifests found for source ' . $source->getSourceName());
            }

            /** @var array<FileItem|null> $ma$manifestsFiltered */
            $combinedSources[] = new MappingFromRawConfigurationAndPhysicalDataWithManifest(
                $source,
                reset($manifestsFiltered),
            );
        }
        return $combinedSources;
    }
}
